/**
*
* Copyright (c) 2017 ytk-mp4j https://github.com/yuantiku
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:

* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.

* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

package com.fenbi.mp4j.rpc;

import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operator.IIntOperator;
import com.fenbi.mp4j.operator.IOperator;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author xialong
 */
public class Server implements IServer {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    public final static String ADDRESS_DELIM = "#@#";
    private final static int DEFAULT_REDUCE_WAIT_TIME = 60 * 60 * 1000;

    private final StringBuilder addresses;
    private final String[] addressesArr;

    private int connectedCnt = 0;
    private int closedCnt = 0;
    private volatile boolean closed = false;
    private final int slaveNum;

    private final CyclicBarrier barrier;
    private final Object closedLock = new Object();
    private Exchanger<Integer> exch = new Exchanger<>();

    private final int port;

    private final List<String> killMeScriptList = new ArrayList<>();

    public enum Status {
        WAITING_FOR_CONNECTING("waiting for connecting"),
        WAITING_FOR_HEARTBEAT("waiting for heartbeat");
        private String status;
        private Status(String status) {
            this.status = status;
        }
    }

    private volatile Status status = Status.WAITING_FOR_CONNECTING;
    private final long rpcStartTime = System.currentTimeMillis();
    private final Map<Integer, Long> heartbeatMap = new ConcurrentHashMap<>();
    public static final int WAIT_FOR_CONNECTING_TIMEOUT = 7200 * 1000;
    public static final int WAIT_FOR_HEARTBEAT_TIMEOUT = 600 * 1000;

    private final ScheduledExecutorService scheduledThreadPool;

    private final Object closeLock = new Object();
    private Object allReducePrimitiveArray = new Object();
    private long allReduceCnt = 0;
    private Object []allReduceLocks;

    private ByteArrayOutputStream allReduceByteArray = new ByteArrayOutputStream(8 * 1024 * 1024);
    private volatile ArrayPrimitiveWritable allReduceReturnArray;
    private boolean closeCode = true;

    public Server(int slaveNum, int port) {
        this.slaveNum = slaveNum;
        this.port = port;
        addresses = new StringBuilder();
        addressesArr = new String[slaveNum];
        barrier = new CyclicBarrier(slaveNum);

        long cur = System.currentTimeMillis();
        for (int i = 0; i < slaveNum; i++) {
            heartbeatMap.put(i, cur);
        }

        this.scheduledThreadPool = Executors.newScheduledThreadPool(1);
        scheduledThreadPool.scheduleAtFixedRate(checkTimeoutTask, 10, 30, TimeUnit.SECONDS);

        allReduceLocks = new Object[slaveNum];
        for (int i = 0; i < slaveNum; i++) {
            allReduceLocks[i] = new Object();
        }
    }
    private final Thread checkTimeoutTask = new Thread() {
        @Override
        public void run() {
            timeoutCheck();
        }
    };

    @Override
    public void heartbeat(int rank) throws Mp4jException {
        long cur = System.currentTimeMillis();
        heartbeatMap.put(rank, cur);
        LOG.debug("rank:" + rank + " heartbeat!");
    }

    @Override
    public void barrier() throws Mp4jException {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }


    private void timeoutCheck() {

        long now = System.currentTimeMillis();
        if (status == Status.WAITING_FOR_CONNECTING) {
            LOG.info("waiting for connecting check...");
            if (now - rpcStartTime > WAIT_FOR_CONNECTING_TIMEOUT) {
                LOG.info("[ERROR] waiting for connecting check timeout > " + WAIT_FOR_CONNECTING_TIMEOUT +
                        ", master will be shutdowned!");
                System.exit(2);
            }
            LOG.info("connecting check ok!");
        } else {
            LOG.debug("waiting for heartbeat check...");
            for (int i = 0; i < slaveNum; i++) {
                long lastTime = heartbeatMap.get(i);
                if (now - lastTime > WAIT_FOR_HEARTBEAT_TIMEOUT) {
                    LOG.info("[ERROR] waiting for heartbeat timeout > " + WAIT_FOR_HEARTBEAT_TIMEOUT +
                            ", master will be shutdowned!");
                    System.exit(3);
                }
            }
            LOG.debug("heart beat check ok!");

        }
    }

    @Override
    public Text getAllSlavesInfo(Text slaveSock) throws Mp4jException, BrokenBarrierException, InterruptedException {
        int rank = -1;
        int order;
        String thisSock = slaveSock.toString();

        synchronized (addressesArr) {
            if (connectedCnt >= slaveNum) {
                LOG.info("more than " + slaveNum + " slaves connecting master, may be some slave restart, task failed!");
                return null;
            }

            order = connectedCnt;
            addressesArr[connectedCnt++] = thisSock;
            LOG.info("connecting:" + slaveSock + ", connected count:" + connectedCnt);
        }

        barrier.await();

        synchronized (addresses) {
            if (order == 0) {
                LOG.info("host names before sort:" + Arrays.toString(addressesArr));
                Arrays.sort(addressesArr);
                LOG.info("host names after sort:" + Arrays.toString(addressesArr));

                for (int i = 0; i < addressesArr.length; i++) {
                    addresses.append(addressesArr[i]).append(ADDRESS_DELIM);
                }
            }
        }

        barrier.await();

        synchronized (addressesArr) {
            for (int i = 0; i < addressesArr.length; i++) {
                if (thisSock.equals(addressesArr[i])) {
                    rank = i;
                    break;
                }
            }
        }

        if (rank == 0) {
            status = Status.WAITING_FOR_HEARTBEAT;
        }


        if (rank == -1)
            return null;

        LOG.info("current slave's rank:" + rank + ", address:" + slaveSock);
        synchronized (addresses) {
            Text retList = new Text(addresses.toString() + rank);
            return retList;
        }

    }

    @Override
    public void info(int rank, Text info) throws Mp4jException {
        LOG.info(info.toString());

    }

    @Override
    public void error(int rank, Text error) throws Mp4jException {
        LOG.error(error.toString());
    }


    @Override
    public void debug(int rank, Text debug) throws Mp4jException {
        LOG.debug(debug.toString());
    }


    @Override
    public void close(int rank, int code) throws Mp4jException {

        LOG.info("recv close message from the slave rank:" + rank + ", code:" + code);

        synchronized (closeLock) {
            closeCode = closeCode && (code == 0);
        }


        if (code != 0) {
            LOG.info("unnormally closed, rank=" + rank + ", code:" + code);
            synchronized (closedLock) {
                closed = true;
                LOG.info("unnormally closed, notify to stop...");
                closedLock.notifyAll();
            }
            LOG.info("unnormally closed! directly return! rank=" + rank + ", code:" + code);
            return;
        }

        LOG.info("normal closed, rank:" + rank);
        synchronized (closedLock) {
            closedCnt++;
            closed = true;
            if (closedCnt >= slaveNum) {
                closedLock.notifyAll();
                LOG.info("all slaves have sent close messages! server will be closed right now!");
            }
        }
    }

    public int stop() {
        synchronized (closedLock) {
            while (!closed) {
                try {
                    closedLock.wait();
                } catch (InterruptedException e) {
                    LOG.error("thread interrupted!", e);
                }
            }
        }

        LOG.info("reduce server closed!");
        synchronized (closeLock) {
            return closeCode ? 0 : 1;
        }
    }



    public void shutdown(int error, Text message) throws Mp4jException {
        synchronized (this) {
            LOG.info(message.toString());
            LOG.info("shutdown(" + error + ") invoked");
            System.exit(error);
        }
    }

    @Override
    public void writeFile(Text content , Text fileName) throws Mp4jException {

        PrintWriter writer = null;
        try {
            writer = new PrintWriter(fileName.toString());
            writer.println(content.toString());
        } catch (Exception e) {
            throw new Mp4jException(e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }

    }

    @Override
    public void killMe(int rank, Text script) throws Mp4jException {
        try {
            synchronized (addressesArr) {
                killMeScriptList.add(script.toString());
            }

            barrier.await();

            if (rank == 0) {

                PrintWriter writer = null;
                try {
                    writer = new PrintWriter("kill_" + port + ".sh");
                    for (String kill : killMeScriptList) {
                        writer.println(kill);
                    }

                    String hostName = InetAddress.getLocalHost().getHostName();
                    // get pid
                    // get name representing the running Java virtual machine.
                    String name = ManagementFactory.getRuntimeMXBean().getName();
                    String pid = name.split("@")[0];
                    LOG.info("Pid is:" + pid);
                    writer.println("kill -9 " + pid);

                } finally {
                    if (writer != null) {
                        writer.close();
                    }
                }
            }

        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    @Override
    public int exchange(int rank) throws Mp4jException {
        Integer thatRank = null;
        try {
            thatRank = exch.exchange(rank, DEFAULT_REDUCE_WAIT_TIME, TimeUnit.MILLISECONDS);
            LOG.debug("exchange: " + rank + " <==> " + thatRank);
        } catch (Exception e) {
            LOG.error("exchange timeout!", e);
            System.exit(201);
        }

        return thatRank;

    }

    @Override
    public ArrayPrimitiveWritable primitiveArrayAllReduce(ArrayPrimitiveWritable arrayPrimitiveWritable, int rank) throws Mp4jException {

        Class<?> componentType = arrayPrimitiveWritable.getComponentType();
        synchronized (Server.class) {
            int localIdx = (int)(allReduceCnt % slaveNum);
            if (componentType == Byte.TYPE) {      // byte
                byte []thatArray = (byte[])arrayPrimitiveWritable.get();
                int length = thatArray.length;

                if (localIdx == 0) {
                    byte []thisArray = new byte[slaveNum * length];
                    allReducePrimitiveArray = thisArray;
                    for (int i = 0; i < length; i++) {
                        thisArray[i] = thatArray[i];
                    }
                } else {
                    byte []thisArray = (byte[]) allReducePrimitiveArray;
                    int startIdx = localIdx * length;
                    for (int i = 0; i < length; i++) {
                        thisArray[startIdx + i] = thatArray[i];
                    }
                }
            } else if (componentType == Short.TYPE) {     // short
                short []thatArray = (short[])arrayPrimitiveWritable.get();
                int length = thatArray.length;

                if (localIdx == 0) {
                    short []thisArray = new short[slaveNum * length];
                    allReducePrimitiveArray = thisArray;
                    for (int i = 0; i < length; i++) {
                        thisArray[i] = thatArray[i];
                    }
                } else {
                    short []thisArray = (short[]) allReducePrimitiveArray;
                    int startIdx = localIdx * length;
                    for (int i = 0; i < length; i++) {
                        thisArray[startIdx + i] = thatArray[i];
                    }
                }
            } else if (componentType == Integer.TYPE) {   // int
                int []thatArray = (int[])arrayPrimitiveWritable.get();
                int length = thatArray.length;

                if (localIdx == 0) {
                    int []thisArray = new int[slaveNum * length];
                    allReducePrimitiveArray = thisArray;
                    for (int i = 0; i < length; i++) {
                        thisArray[i] = thatArray[i];
                    }
                } else {
                    int []thisArray = (int[]) allReducePrimitiveArray;
                    int startIdx = localIdx * length;
                    for (int i = 0; i < length; i++) {
                        thisArray[startIdx + i] = thatArray[i];
                    }
                }
            } else if (componentType == Long.TYPE) {      // long
                long []thatArray = (long[])arrayPrimitiveWritable.get();
                int length = thatArray.length;

                if (localIdx == 0) {
                    long []thisArray = new long[slaveNum * length];
                    allReducePrimitiveArray = thisArray;
                    for (int i = 0; i < length; i++) {
                        thisArray[i] = thatArray[i];
                    }
                } else {
                    long []thisArray = (long[]) allReducePrimitiveArray;
                    int startIdx = localIdx * length;
                    for (int i = 0; i < length; i++) {
                        thisArray[startIdx + i] = thatArray[i];
                    }
                }
            } else if (componentType == Float.TYPE) {     // float
                float []thatArray = (float[])arrayPrimitiveWritable.get();
                int length = thatArray.length;

                if (localIdx == 0) {
                    float []thisArray = new float[slaveNum * length];
                    allReducePrimitiveArray = thisArray;
                    for (int i = 0; i < length; i++) {
                        thisArray[i] = thatArray[i];
                    }
                } else {
                    float []thisArray = (float[]) allReducePrimitiveArray;
                    int startIdx = localIdx * length;
                    for (int i = 0; i < length; i++) {
                        thisArray[startIdx + i] = thatArray[i];
                    }
                }
            } else if (componentType == Double.TYPE) {    // double
                double []thatArray = (double[])arrayPrimitiveWritable.get();
                int length = thatArray.length;

                if (localIdx == 0) {
                    double []thisArray = new double[slaveNum * length];
                    allReducePrimitiveArray = thisArray;
                    for (int i = 0; i < length; i++) {
                        thisArray[i] = thatArray[i];
                    }
                } else {
                    double []thisArray = (double[]) allReducePrimitiveArray;
                    int startIdx = localIdx * length;
                    for (int i = 0; i < length; i++) {
                        thisArray[startIdx + i] = thatArray[i];
                    }
                }
            } else {
                throw new Mp4jException("Component type " + componentType.toString()
                        + " is set as the output type, but no encoding is implemented for this type.");
            }

            allReduceCnt ++;
        }

        try {
            barrier.await();
            synchronized (allReduceLocks[rank]) {
                return new ArrayPrimitiveWritable(allReducePrimitiveArray);
            }

        } catch (Exception e) {
            throw new Mp4jException("primitive array exception!");
        }
    }

    @Override
    public ArrayPrimitiveWritable arrayAllReduce(ArrayPrimitiveWritable byteSerialArray, int rank) throws Mp4jException {
        try {
            byte[] obj = (byte[])byteSerialArray.get();
            allReduceByteArray.write(obj);
            barrier.await();
            if (rank == 0) {
                allReduceReturnArray = new ArrayPrimitiveWritable(allReduceByteArray.toByteArray());
                allReduceByteArray.reset();
            }
            barrier.await();
            return allReduceReturnArray;
        } catch (Exception e) {
            throw new Mp4jException("array allreduce exception!");
        }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return IServer.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature(IServer.versionID, null);
    }
}
