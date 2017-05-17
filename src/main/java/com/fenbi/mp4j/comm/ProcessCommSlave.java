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

package com.fenbi.mp4j.comm;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.meta.ArrayMetaData;
import com.fenbi.mp4j.meta.MapMetaData;
import com.fenbi.mp4j.meta.MetaData;
import com.fenbi.mp4j.operand.*;
import com.fenbi.mp4j.operator.*;
import com.fenbi.mp4j.rpc.IServer;
import com.fenbi.mp4j.rpc.Server;
import com.fenbi.mp4j.utils.CommUtils;
import com.fenbi.mp4j.utils.KryoUtils;
import com.fenbi.mp4j.utils.ScatterAllocate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
/**
 * ProcessCommSlave is used as multi-processes communication.
 * @author xialong
 */
public class ProcessCommSlave {
    public static final Logger LOG = LoggerFactory.getLogger(ProcessCommSlave.class);

    public final static String HOST_PORT_DELIM = "###";
    public final static int SOCK_CONN_RETRY_TIME = 50;
    public final static int SOCK_CONN_SLEEP_TIME = 30;

    private final int slaveNum;
    private final int rank;
    private final String rankMsgPrefix;
    private final String[] slaveHosts;
    private final int[] slavePorts;

    private volatile Socket sendSockRef;
    private volatile Socket recvSockRef;
    private final ServerSocket recvDataSock;

    private volatile boolean closed = false;
    private volatile int curStep = 0;

    private final ScheduledExecutorService scheduledThreadPool;
    private volatile int heartbeatExceptionNum = 0;

    private final IServer server;
    private volatile Operand operand;

    private BlockingDeque<MetaData> sendQueue = new LinkedBlockingDeque<>();
    private BlockingDeque<MetaData> recvQueue = new LinkedBlockingDeque<>();
    private BlockingDeque<MetaData> recvResultQueue = new LinkedBlockingDeque<>();


    private final Thread sendDataTask = new Thread() {
        @Override
        public void run() {
            try {
                while (true) {
                    MetaData metaData = sendQueue.take();
                    Output output = getOutput(metaData.getDestRank());

                    operand.send(output, metaData);
                }

            } catch (Exception e) {
                try {
                    exception(e);
                } catch (Mp4jException e1) {
                    LOG.error("Mp4jException", e);
                }
            }
        }
    };

    private final Thread recvDataTask = new Thread() {
        @Override
        public void run() {
            try {
                while (true) {
                    MetaData metaData = recvQueue.take();
                    MetaData recvMetaData = operand.recv(getInput(), metaData);
                    recvResultQueue.put(recvMetaData);
                }
            } catch (Exception e) {
                try {
                    exception(e);
                } catch (Mp4jException e1) {
                    LOG.error("Mp4jException", e);
                }
            }
        }
    };

    private final Thread sendHeartBeatTask = new Thread() {
        @Override
        public void run() {
            heartbeat();
        }
    };

    /**
     * Process communication constructor, every process have just only one ProcessCommSlave instance.
     * @param loginName if you use ssh to execute command, you must provide login name, e.g. ssh loginName@host "your command"
     * @param masterHost master host name
     * @param masterPort master host port
     * @throws Mp4jException
     */
    public ProcessCommSlave(String loginName,
                            String masterHost,
                            int masterPort) throws Mp4jException {

        try {

            LOG.info("master host:" + masterHost + ", master port:" + masterPort);
            InetSocketAddress address = new InetSocketAddress(masterHost, masterPort);
            server = (IServer) RPC.getProxy(IServer.class, IServer.versionID, address, new Configuration());

            String host = InetAddress.getLocalHost().getHostName();
            recvDataSock = new ServerSocket(0);
            int port = recvDataSock.getLocalPort();

            Text addresses = server.getAllSlavesInfo(new Text(host + HOST_PORT_DELIM + port));

            if (address == null) {
                throw new Mp4jException("slaves connecting master failed, may be this slave restarted or master port is occupied, task failed!");
            }

            String[] slavesAddresses = addresses.toString().split(Server.ADDRESS_DELIM);
            slaveNum = slavesAddresses.length - 1;
            rank = Integer.parseInt(slavesAddresses[slaveNum]);
            rankMsgPrefix = "[rank=" + rank + "] ";

                    LOG.info("this slave recv data port:" + port);
            LOG.info("slave num:" + slaveNum);
            LOG.info("slave rank:" + rank);

            // get pid
            // get name representing the running Java virtual machine.
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String pid = name.split("@")[0];
            LOG.info("Pid is:" + pid);

            if (slaveNum > 1) {
                server.killMe(rank, new Text("ssh " + loginName + "@" + host +
                        " \"kill -9 " + pid + "\""));
            } else {
                server.killMe(rank, new Text("kill -9 " + pid));
            }


            slaveHosts = new String[slaveNum];
            slavePorts = new int[slaveNum];
            for (int i = 0; i < slaveNum; i++) {
                String[] addr = slavesAddresses[i].split(HOST_PORT_DELIM);
                slaveHosts[i] = addr[0];
                slavePorts[i] = Integer.parseInt(addr[1]);
            }

            LOG.info("slaves addresses:");
            for (int i = 0; i < slaveNum; i++) {
                LOG.info(slaveHosts[i] + ":" + slavePorts[i]);
            }

            this.scheduledThreadPool = Executors.newScheduledThreadPool(1);
            scheduledThreadPool.scheduleAtFixedRate(sendHeartBeatTask, 5, 15, TimeUnit.SECONDS);
            sendDataTask.start();
            recvDataTask.start();

        } catch (Exception e) {
            LOG.error("exception!", e);
            throw new Mp4jException(e.getCause());
        }

        info("this slave init finished!");
    }

    private void heartbeat() {
        try {
            if (!closed) {
                server.heartbeat(rank);
            }
        } catch (Exception e) {
            e.printStackTrace();
            heartbeatExceptionNum ++;
            LOG.error("rank:" + rank + " send heartbeat exception, exception time:" + heartbeatExceptionNum, e);
            if (heartbeatExceptionNum > 4) {
                LOG.error("heart beat exception > 4 master may be shutdowned! this slave will be shutdowned...");
                System.exit(4);
            }

        }
    }

    /**
     * close communication.
     * @param code close code.
     * @throws Mp4jException
     */
    public void close(int code) throws Mp4jException {

        LOG.info("close code=" + code);
        if (closed)
            return;
        if (server != null) {
            try {
                server.close(rank, code);
            } catch (Exception e) {
                throw new Mp4jException("Exception in close!", e.getCause());
            }
            LOG.info("reduce closed!");
        }

        try {
            if (recvSockRef != null) {
                recvSockRef.close();
                LOG.info("Recv operand socket closed!");
            }

            if (recvDataSock != null) {
                recvDataSock.close();
                LOG.info("Data server socket closed!");
            }

            if (sendSockRef != null) {
                sendSockRef.close();
                LOG.info("Send operand socket closed!");
            }
        } catch (IOException e) {
            throw new Mp4jException("exception in slave close!", e.getCause());
        }

        this.operand = null;

        closed = true;
        LOG.info("reduce closed!");
    }

    /**
     * create a file in master
     * @param content content in a file
     * @param fileName file name to be write
     * @throws Exception
     */
    public void writeFile(String content , String fileName) throws Exception {
        server.writeFile(new Text(content), new Text(fileName));
    }

    /**
     * send information to master
     * @param info information
     * @param onlyRank0 if just rank 0 can send information successfully.
     * @throws Mp4jException
     */
    public void info(String info, boolean onlyRank0) throws Mp4jException {
        try {
            if (!onlyRank0) {
                server.info(rank, new Text(rankMsgPrefix + info));
            } else {
                if (rank == 0) {
                    server.info(rank, new Text(info));
                }
            }
        } catch (Exception e) {
            throw new Mp4jException("exception in slave report!", e.getCause());
        }
    }

    /**
     * only rank 0 can send information successfully.
     * @param info information
     * @throws Mp4jException
     */
    public void info(String info) throws Mp4jException {
        info(info, true);
    }

    /**
     * send debug information to master
     * @param debug debug information
     * @param onlyRank0 if just rank 0 can send debug information successfully.
     * @throws Mp4jException
     */
    public void debug(String debug, boolean onlyRank0) throws Mp4jException {
        try {
            if (!onlyRank0) {
                server.debug(rank, new Text(rankMsgPrefix + debug));
            } else {
                if (rank == 0) {
                    server.debug(rank, new Text(debug));
                }
            }
        } catch (Exception e) {
            throw new Mp4jException("exception in slave report!", e.getCause());
        }
    }

    /**
     *
     * only rank 0 can send debug information successfully.
     * @param debug debug information
     * @throws Mp4jException
     */
    public void debug(String debug) throws Mp4jException {
        debug(debug, true);
    }

    /**
     * send error information to master.
     * @param error error info
     * @throws Mp4jException
     */
    public void error(String error) throws Mp4jException {
        try {
            server.error(rank, new Text(rankMsgPrefix + error));
        } catch (Exception e) {
            throw new Mp4jException("exception in slave report!", e.getCause());
        }
    }

    /**
     * send exception information to master.
     * @param e exception
     * @throws Mp4jException
     */
    public void exception(Exception e) throws Mp4jException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        try {
            error("slave exception:" + sw.toString());
            Thread.sleep(5000);
            close(1);
        } catch (Exception e1) {
            throw new Mp4jException("exception in slave report!", e1.getCause());
        }

    }

    /**
     * slave number
     * @return
     */
    public int getSlaveNum() {
        return slaveNum;
    }

    /**
     * the rank of this process
     * @return
     */
    public int getRank() {
        return rank;
    }

    private Socket getSendDataSocket(int targetRank)
            throws InterruptedException, IOException {
        Socket sendDataSock = null;
        LOG.debug("send data, target rank:" + targetRank);
        for (int i = 0; i < SOCK_CONN_RETRY_TIME; i++) {
            try {
                String targetAddress = slaveHosts[targetRank];
                int targetRecvDataPort = slavePorts[targetRank];
                sendDataSock = new Socket(targetAddress, targetRecvDataPort);
                sendDataSock.setTcpNoDelay(true);
                sendDataSock.setSoLinger(true, 160);
                break;
            } catch (UnknownHostException e) {
                Thread.sleep(SOCK_CONN_SLEEP_TIME);
                if (i == SOCK_CONN_RETRY_TIME - 1)
                    throw e;
                continue;
            } catch (IOException e) {
                Thread.sleep(SOCK_CONN_SLEEP_TIME);
                if (i == SOCK_CONN_RETRY_TIME - 1)
                    throw e;
                continue;
            }
        }

        sendSockRef = sendDataSock;
        return sendDataSock;
    }

    private Socket getRecvDataSocket() throws IOException {
        Socket recvDataSock = this.recvDataSock.accept();
        LOG.debug("recv socket:" + recvDataSock);
        recvSockRef = recvDataSock;
        return recvDataSock;
    }


    /**
     * synchronizing processes
     * @throws Mp4jException
     */
    public void barrier() throws Mp4jException {
        try {
            server.barrier();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    private <T> MetaData dynamicBinaryTreeGather(int rootRank, MetaData thisMetaData) throws Mp4jException {
        try {
            LOG.info("thismetadata:" + thisMetaData);
            int thatRank = server.exchange(rank);
            if ((rank < thatRank ||
                    rank == rootRank) &&
                    thatRank != rootRank) {
                LOG.info("this rank:" + rank + " recv data! that rank:" + thatRank);
                while (true) {
                    recvQueue.put(thisMetaData);
                    MetaData thatMetaData = recvResultQueue.take();
                    LOG.info("recv thatmetadata:" + thatMetaData);
                    // merge
                    switch (operand.getContainer()) {
                        case ARRAY:
                            thisMetaData.stepIncr(1);
                            thisMetaData.setSum(thisMetaData.getSum() + thatMetaData.getSum());
                            thisMetaData.setSegNum(thisMetaData.getSegNum() + thatMetaData.getSegNum());
                            ArrayMetaData thatArrayMetaData = thatMetaData.convertToArrayMetaData();
                            ArrayMetaData thisArrayMetaData = thisMetaData.convertToArrayMetaData();

                            // append ranks, froms, tos
                            thisArrayMetaData.getRanks().addAll(thatArrayMetaData.getRanks());
                            thisArrayMetaData.getSegFroms().addAll(thatArrayMetaData.getSegFroms());
                            thisArrayMetaData.getSegTos().addAll(thatArrayMetaData.getSegTos());
                            break;
                        case MAP:
                            thisMetaData.stepIncr(1);
                            thisMetaData.setSum(thisMetaData.getSum() + thatMetaData.getSum());
                            thisMetaData.setSegNum(1);
                            MapMetaData thatMapMetaData = thatMetaData.convertToMapMetaData();
                            MapMetaData thisMapMetaData = thisMetaData.convertToMapMetaData();
                            thisMapMetaData.getRanks().addAll(thatMapMetaData.getRanks());

                            // merge map, recv process always has 1 map
                            List<Map<String, T>> thatMapDataList = thatMetaData.getMapDataList();
                            Map<String, T> thisMap = (Map<String, T>)thisMapMetaData.getMapDataList().get(0);
                            for (Map<String, T> thatMap : thatMapDataList) {
                                for (Map.Entry<String, T> entry : thatMap.entrySet()) {
                                    thisMap.put(entry.getKey(), entry.getValue());
                                }
                            }
                            // reset dataNums = 1
                            thisMapMetaData.setDataNums(Arrays.asList(thisMap.size()));

                            break;
                        default:
                            throw new Mp4jException("unsupported container:" + operand.getContainer());
                    }
                    LOG.info("merged thisMetaData:" + thisMetaData);

                    if (thisMetaData.getSum() != slaveNum) {
                        thatRank = server.exchange(rank);

                        if (!((rank < thatRank ||
                                rank == rootRank) &&
                                thatRank != rootRank)) {
                            break;
                        }
                    } else {
                        if (rank != rootRank) {
                            throw new Mp4jException("dynamic binary gather must stop at rootRank:" + rootRank + ", thisRank:" + rank);
                        }
                        barrier();
                        LOG.info("root gather finished");
                        return thisMetaData;
                    }
                }
            }

            LOG.info("this rank:" + rank + " send data! that rank:" + thatRank);
            thisMetaData.setDestRank(thatRank);
            LOG.info("send thatmetadata:" + thisMetaData);
            sendQueue.put(thisMetaData);
            barrier();
            LOG.info("gather finished");
            return thisMetaData;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * Takes elements from all processes and gathers them to root process, the data container is array,
     * process i send data interval [sendfroms[i], sendtos[i]) to root process, placed in the same positions,
     * s.t. sendfroms[i] &le; sendtos[i], sendfroms[i] &ge; sendtos[i-1]
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param sendfroms sending start positions, included.
     * @param sendtos sending end positions, excluded
     * @param rootRank root rank
     * @return if this process is root, returned is gathered elements, otherwise,
     *         is invalid array, contains intermediate results.
     * @throws Mp4jException
     */
    public <T> T gatherArray(T arrData, Operand operand, int[] sendfroms, int[] sendtos, int rootRank) throws Mp4jException {

        if (sendfroms.length != slaveNum) {
            throw new Mp4jException("sendfroms array length must be equal to slaveNum");
        }

        if (sendtos.length != slaveNum) {
            throw new Mp4jException("sendtos array length must be equal to slaveNum");
        }

        if (slaveNum == 1) {
            return arrData;
        }

        CommUtils.isfromsTosLegal(sendfroms, sendtos);

        try {
            this.operand = operand;
            this.operand.setCollective(Collective.GATHER);
            this.operand.setContainer(Container.ARRAY);
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(rank)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.GATHER)
                    .insert(rank, sendfroms[rank], sendtos[rank]);
            arrayMetaData.setArrData(arrData);

            return (T)dynamicBinaryTreeGather(rootRank, arrayMetaData).getArrData();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * Takes elements from all processes and gathers them to root process, the data container is map,
     * the data in map regardless of the order, if different process have same keys, only one key(random) will
     * be saved in root process.
     * @param mapData key is {@code String}, value is any object
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return if this process is root, returned is gathered elements, otherwise,
     *         is invalid map, contained intermediate comm result or null.
     * @throws Mp4jException
     */
    public <T> Map<String, T> gatherMap(Map<String, T> mapData, Operand operand, int rootRank) throws Mp4jException {
        if (slaveNum == 1) {
            return mapData;
        }

        try {
            this.operand = operand;
            this.operand.setCollective(Collective.GATHER);
            this.operand.setContainer(Container.MAP);
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(rank)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.GATHER)
                    .insert(rank, mapData.size());
            List<Map<String, T>> listMapList = new ArrayList<>();
            listMapList.add(mapData);
            mapMetaData.setMapDataList(listMapList);

            return (Map<String, T>)dynamicBinaryTreeGather(rootRank, mapMetaData).getMapDataList().get(0);
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * Takes elements from all processes and gathers them to all processes.
     * the operation can be viewed as a combination of gather and broadcast.
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param froms the start position of process i to gathered is froms[i]
     * @param tos the end position of process i to gathered is tos[i]

     * @return allgathered array
     * @throws Mp4jException
     */
    public <T> T allgatherArray(T arrData, Operand operand, int[] froms, int[] tos) throws Mp4jException {

        if (froms.length != slaveNum) {
            throw new Mp4jException("froms array length must be equal to slaveNum");
        }

        if (tos.length != slaveNum) {
            throw new Mp4jException("tos array length must be equal to slaveNum");
        }

        if (slaveNum == 1) {
            return arrData;
        }

        CommUtils.isfromsTosLegal(froms, tos);

        try {
            this.operand = operand;
            this.operand.setCollective(Collective.ALL_GATHER);
            this.operand.setContainer(Container.ARRAY);
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(rank)
                    .setDestRank((rank + 1) % slaveNum)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.ALL_GATHER)
                    .insert(rank, froms[rank], tos[rank]);
            arrayMetaData.setArrData(arrData);

            return (T)ringAllgather(arrayMetaData, Container.ARRAY).getArrData();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }


    }

    /**
     * Takes elements from all processes and gathers them to all processes.
     * Similar with {@link #allgatherArray(Object, Operand, int[], int[])}, this container is map.
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})

     * @return map list
     * @throws Mp4jException
     */
    public <T> List<Map<String, T>> allgatherMap(Map<String, T> mapData, Operand operand) throws Mp4jException {
        if (slaveNum == 1) {
            return Arrays.asList(mapData);
        }

        try {
            this.operand = operand;
            this.operand.setCollective(Collective.ALL_GATHER);
            this.operand.setContainer(Container.MAP);
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(rank)
                    .setDestRank((rank + 1) % slaveNum)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.ALL_GATHER)
                    .insert(rank, mapData.size());
            List<Map<String, T>> listMap = new ArrayList<>(slaveNum);
            listMap.add(mapData);
            mapMetaData.setMapDataList(listMap);

            MapMetaData<T> retMapMetaData = (MapMetaData<T>)ringAllgather(mapMetaData, Container.MAP).convertToMapMetaData();
            return retMapMetaData.getMapDataList();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    private <T> MetaData<T> ringAllgather(MetaData thisMetaData, Container container) throws Mp4jException {
        try {
            List<Map<String, T>> retMapList = null;
            if (container == Container.MAP){
                retMapList = new ArrayList<>(slaveNum);
                for (int i = 0; i < slaveNum; i++) {
                    retMapList.add(Collections.emptyMap());
                }
                retMapList.set(rank, (Map<String, T>)thisMetaData.getMapDataList().get(0));
            }

            // send first block
            sendQueue.put(thisMetaData);

            // recv slaveNum - 1 times, send slaveNum - 2
            for (int step = 1; step < slaveNum; step++) {
                recvQueue.push(thisMetaData);
                MetaData<T> thatMetaData = recvResultQueue.take();

                if (container == Container.MAP) {
                    int realOriginRank = thatMetaData.convertToMapMetaData().getRanks().get(0);
                    retMapList.set(realOriginRank, thatMetaData.getMapDataList().get(0));
                }

                thisMetaData = thatMetaData;
                thatMetaData.setSrcRank(rank)
                        .setDestRank((rank + 1) % slaveNum)
                        .setCollective(Collective.ALL_GATHER);
                if (step < slaveNum - 1) {
                    sendQueue.put(thatMetaData);
                }
            }

            if (container == Container.MAP) {
                thisMetaData.setMapDataList(retMapList);
            }

            barrier();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

        return thisMetaData;
    }

    /**
     * Broadcast array in root process to all other processe(included itself).
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param from start position to broadcast
     * @param to end position to broadcast
     * @param rootRank root rank

     * @return root array, interval is [from, to)
     * @throws Mp4jException
     */
    public <T> T broadcastArray(T arrData, Operand operand, int from, int to, int rootRank) throws Mp4jException {
        if (slaveNum == 1) {
            return arrData;
        }

        CommUtils.isFromToLegal(from, to);

        try {
            int[] froms = new int[slaveNum];
            int[] tos = new int[slaveNum];
            int avg = (to - from) / slaveNum;
            int fromidx = from;
            for (int r = 0; r < slaveNum; r++) {
                froms[r] = fromidx;
                tos[r] = fromidx + avg;
                fromidx += avg;
            }
            tos[slaveNum - 1] = to;

            T scatterArr = scatterArray(arrData, operand, froms, tos, rootRank);
            return allgatherArray(scatterArr, operand, froms, tos);
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * Broadcast single value in root process to all other processes(included itself).
     * @param value value to be broadcast(only root process is valid)
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return the value of root process.
     * @throws Mp4jException
     */
    public <T> T broadcast(T value, Operand operand, int rootRank) throws Mp4jException {
        if (operand instanceof DoubleOperand) {
            double []doubleArr = new double[1];
            doubleArr[0] = (Double) value;
            doubleArr = broadcastArray(doubleArr, operand, 0, doubleArr.length, rootRank);
            return (T)Double.valueOf(doubleArr[0]);
        } else if (operand instanceof FloatOperand) {
            float []floatArr = new float[1];
            floatArr[0] = (Float) value;
            floatArr = broadcastArray(floatArr, operand, 0, floatArr.length, rootRank);
            return (T)Float.valueOf(floatArr[0]);
        } else if (operand instanceof IntOperand) {
            int []intArr = new int[1];
            intArr[0] = (Integer) value;
            intArr = broadcastArray(intArr, operand, 0, intArr.length, rootRank);
            return (T)Integer.valueOf(intArr[0]);
        } else if (operand instanceof LongOperand) {
            long []longArr = new long[1];
            longArr[0] = (Long) value;
            longArr = broadcastArray(longArr, operand, 0, longArr.length, rootRank);
            return (T)Long.valueOf(longArr[0]);
        } else if (operand instanceof ObjectOperand) {
            T []objectArr = (T[]) Array.newInstance(value.getClass(), 1);
            objectArr[0] = value;
            objectArr = broadcastArray(objectArr, operand, 0, objectArr.length, rootRank);
            return objectArr[0];
        } else if (operand instanceof StringOperand) {
            String []stringArr = new String[1];
            stringArr[0] = (String) value;
            stringArr = broadcastArray(stringArr, operand, 0, stringArr.length, rootRank);
            return (T)stringArr[0];
        } else if (operand instanceof ShortOperand) {
            short []shortArr = new short[1];
            shortArr[0] = (Short) value;
            shortArr = broadcastArray(shortArr, operand, 0, shortArr.length, rootRank);
            return (T)Short.valueOf(shortArr[0]);
        } else if (operand instanceof ByteOperand) {
            byte []byteArr = new byte[1];
            byteArr[0] = (Byte) value;
            byteArr = broadcastArray(byteArr, operand, 0, byteArr.length, rootRank);
            return (T)Byte.valueOf(byteArr[0]);
        } else {
            throw new Mp4jException("unknown operand:" + operand);
        }
    }

    /**
     * Broadcast map in root process to all other processes(included itself),
     * similar with {@link #broadcastArray(Object, Operand, int, int, int)}, but this container is map.
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return the root map.
     * @throws Mp4jException
     */
    public <T> Map<String, T> broadcastMap(Map<String, T> mapData, Operand operand, int rootRank) throws Mp4jException {
        if (slaveNum == 1) {
            return mapData;
        }

        try {
            List<Map<String, T>> listMapData = new ArrayList<>();

            if (rank == rootRank) {
                listMapData = new ArrayList<>(slaveNum);
                for (int i = 0; i < slaveNum; i++) {
                    listMapData.add(new HashMap<>((int)((mapData.size() / slaveNum) * 1.2)));
                }
                for (Map.Entry<String, T> entry : mapData.entrySet()) {
                    String key = entry.getKey();
                    T val = entry.getValue();
                    int idx = key.hashCode() % slaveNum;
                    if (idx < 0) {
                        idx += slaveNum;
                    }
                    listMapData.get(idx).put(key, val);
                }
            }


            Map<String, T> scatteredMap = scatterMap(listMapData, operand, rootRank);
            List<Map<String, T>> broadcasedMapList = allgatherMap(scatteredMap, operand);
            Map<String, T> retMap = broadcasedMapList.get(0);
            for (int i = 1; i < broadcasedMapList.size(); i++) {
                Map<String, T> retMapTemp = broadcasedMapList.get(i);
                for (Map.Entry<String, T> entry : retMapTemp.entrySet()) {
                    retMap.put(entry.getKey(), entry.getValue());
                }
            }

            return retMap;

        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }



    /**
     * Send chunks of an array to different processes, the data container is array.
     * process i receive data interval [recvfroms[i], recvtos[i]) from root process, placed in the same positions,
     * s.t. recvfroms[i] &le; recvtos[i], recvfroms[i] &ge; recvtos[i-1]
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param recvfroms receiving start positions, included
     * @param recvtos receiving end positions, excluded
     * @param rootRank root rank

     * @return rank i receives data from root process, placed in [recvfroms[i], recvtos[i])
     * @throws Mp4jException
     */
    public <T> T scatterArray(T arrData, Operand operand, int[] recvfroms, int[] recvtos, int rootRank) throws Mp4jException {

        try {
            if (recvfroms.length != slaveNum) {
                throw new Mp4jException("recvfroms array length must be equal to slaveNum");
            }

            if (recvtos.length != slaveNum) {
                throw new Mp4jException("recvtos array length must be equal to slaveNum");
            }

            if (slaveNum == 1) {
                return arrData;
            }

            CommUtils.isfromsTosLegal(recvfroms, recvtos);

            this.operand = operand;
            this.operand.setCollective(Collective.SCATTER);
            this.operand.setContainer(Container.ARRAY);
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(rank)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(0)
                    .setCollective(Collective.SCATTER);
            for (int i = 0; i < slaveNum; i++) {
                arrayMetaData.insert(i, recvfroms[i], recvtos[i]);
            }
            arrayMetaData.setArrData(arrData);

            return (T)binaryTreeScatter(rootRank, arrayMetaData, Container.ARRAY).convertToArrayMetaData().getArrData();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }


    /**
     * Send chunks of data to different processes, the data container is map.
     * rank i receive data mapDataList.get(i) from root process.
     * @param mapDataList list of maps to be scattered.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return rank i receives data mapDataList.get(i) from root process.
     * @throws Mp4jException
     */
    public <T> Map<String, T> scatterMap(List<Map<String, T>> mapDataList, Operand operand, int rootRank) throws Mp4jException {

        try {
            if (rank == rootRank && mapDataList.size() != slaveNum) {
                throw new Mp4jException("mapDataList size must be equal to slaveNum");
            }

            if (slaveNum == 1) {
                return mapDataList.get(0);
            }

            LOG.info("entry scater map...");
            this.operand = operand;
            this.operand.setCollective(Collective.SCATTER);
            this.operand.setContainer(Container.MAP);
            MapMetaData<T> MapMetaData = new MapMetaData<>();
            MapMetaData.setSrcRank(rank)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(0)
                    .setCollective(Collective.SCATTER);

            if (rank == rootRank) {
                for (int i = 0; i < slaveNum; i++) {
                    MapMetaData.insert(i, mapDataList.get(i).size());
                }
                MapMetaData.setMapDataList(mapDataList);
            }

            LOG.info("will call binary tree scatter...");
            MapMetaData<T> metaData = (MapMetaData<T>)binaryTreeScatter(rootRank, MapMetaData, Container.MAP).convertToMapMetaData();
            Map<String, T> retMap = null;
            List<Map<String, T>> listMapData = metaData.getMapDataList();
            for (int i = 0; i < metaData.getSegNum(); i++) {
                int curRank = metaData.getRank(i);
                if (curRank == rank) {
                    retMap = listMapData.get(i);
                }
            }
            listMapData.clear();
            if (retMap == null) {
                throw new Mp4jException("scatter error retmap must not be null!");
            }
            return retMap;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    protected <T> List<Map<String, T>> scatterMapSpecial(List<List<Map<String, T>>> mapDataListList, Operand operand, int rootRank) throws Mp4jException {

        try {
            if (rank == rootRank && mapDataListList.size() != slaveNum) {
                throw new Mp4jException("mapDataList size must be equal to slaveNum");
            }

            if (slaveNum == 1) {
                return mapDataListList.get(0);
            }

            LOG.info("entry special scatter map...");
            this.operand = operand;
            this.operand.setCollective(Collective.SCATTER);
            this.operand.setContainer(Container.MAP);
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(rank)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(0)
                    .setCollective(Collective.SCATTER);

            if (rank == rootRank) {
                List<Map<String, T>> mapDataListForScatter = new ArrayList<>();
                for (int i = 0; i < slaveNum; i++) {
                    List<Map<String, T>> mapDataList = mapDataListList.get(i);
                    for (int j = 0; j < mapDataList.size(); j++) {
                        mapDataListForScatter.add(mapDataList.get(j));
                        mapMetaData.insert(i, mapDataList.get(j).size());
                    }
                }
                LOG.info("special root map data list size:" + mapDataListForScatter.size() + ", metadata:" + mapMetaData);
                mapMetaData.setMapDataList(mapDataListForScatter);
            }

            LOG.info("will call binary tree scatter...");
            MapMetaData<T> metaData = (MapMetaData<T>)binaryTreeScatter(rootRank, mapMetaData, Container.MAP).convertToMapMetaData();
            LOG.info("binary tree scatter finished!");
            List<Map<String, T>> retMapList = new ArrayList<>();
            List<Map<String, T>> listMapData = metaData.getMapDataList();
            for (int i = 0; i < metaData.getSegNum(); i++) {
                int curRank = metaData.getRank(i);
                if (curRank == rank) {
                    retMapList.add(listMapData.get(i));
                }
            }
            listMapData.clear();
            return retMapList;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    private <T> MetaData<T> scatterSend(MetaData metaData,
                                 List<Integer> sendInfo,
                                 Container container) throws Mp4jException {

        MetaData newMetaData = MetaData.newMetaData(container);
        int newSrcRank = sendInfo.get(0);
        int newDestRank = sendInfo.get(1);
        int newRankFrom = sendInfo.get(2);
        int newRankTo = sendInfo.get(3);
        newMetaData.setSrcRank(newSrcRank);
        newMetaData.setDestRank(newDestRank);
        newMetaData.setCollective(Collective.SCATTER);

        int segNum = metaData.getSegNum();
        if (container == Container.ARRAY) {
            List<Integer> ranks = metaData.convertToArrayMetaData().getRanks();
            List<Integer> froms = metaData.convertToArrayMetaData().getSegFroms();
            List<Integer> tos = metaData.convertToArrayMetaData().getSegTos();
            for (int i = 0; i < segNum; i++) {
                int rank = ranks.get(i);
                int from = froms.get(i);
                int to = tos.get(i);
                if (rank >= newRankFrom && rank <= newRankTo) {
                    newMetaData.insert(rank, from, to);
                }
            }
            newMetaData.setArrData(metaData.getArrData());

        } else if (container == Container.MAP) {
            List<Integer> dataNums = metaData.convertToMapMetaData().getDataNums();
            List<Integer> ranks = metaData.convertToMapMetaData().getRanks();
            List<Map<String, T>> mapList = metaData.convertToMapMetaData().getMapDataList();
            List<Map<String, T>> newMapList = new ArrayList<>();
            for (int i = 0; i < segNum; i++) {
                int rank = ranks.get(i);
                int dataNum = dataNums.get(i);
                if (rank >= newRankFrom && rank <= newRankTo) {
                    newMetaData.insert(rank, dataNum);
                    newMapList.add(mapList.get(i));
                }
            }

            newMetaData.setMapDataList(newMapList);
        } else {
            throw new Mp4jException("unsupport MetaData type in scatter send");
        }

        return newMetaData;
    }

    private <T> MetaData<T> binaryTreeScatter(int rootRank, MetaData thisMetaData, Container container) throws Mp4jException {
        try {
            LOG.info("entry binary tree scatter ...");
            Map<Integer, List<List<Integer>>> allocateMap = ScatterAllocate.allocate(slaveNum, rootRank);
            Map<Integer, Integer> recvNumMap = ScatterAllocate.recvNum(allocateMap);

            // get this rank's send task list
            List<List<Integer>> thisSendTaskList = allocateMap.getOrDefault(rank, Collections.emptyList());

            // get recv number
            Integer recvNum = recvNumMap.getOrDefault(rank, 0);
            if (recvNum >= 2) {
                throw new Mp4jException("scatter error, recv num must <= 1");
            }

            LOG.info("this send task list:" + thisSendTaskList);
            LOG.info("this recv num:" + recvNum);
            // every process only recv once data except the root process
            int thisSendCursor = 0;
            if (rank == rootRank) {
                if (recvNum > 0) {
                    List<Integer> half0 = thisSendTaskList.get(thisSendCursor++);
                    sendQueue.put(scatterSend(thisMetaData, half0, container));
                    if (thisSendCursor < thisSendTaskList.size()) {
                        List<Integer> half1 = thisSendTaskList.get(thisSendCursor);
                        int toRank = half1.get(2);
                        if (toRank == 0 || toRank == slaveNum / 2) {
                            sendQueue.put(scatterSend(thisMetaData, half1, container));
                            thisSendCursor ++;
                        }
                    }
                } else {
                    for (; thisSendCursor < thisSendTaskList.size(); thisSendCursor++) {
                        sendQueue.put(scatterSend(thisMetaData, thisSendTaskList.get(thisSendCursor), container));
                    }
                }

            }

            // recv at most once & send
            if (recvNum > 0) {
                recvQueue.push(thisMetaData);
                MetaData<T> thatMetaData = recvResultQueue.take();
                thisMetaData = thatMetaData;
                thisMetaData.setCollective(Collective.SCATTER);
                for (int cursor = thisSendCursor; cursor < thisSendTaskList.size(); cursor++) {
                    sendQueue.put(scatterSend(thisMetaData, thisSendTaskList.get(cursor), container));
                }
            }

            barrier();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

        return thisMetaData;
    }

    /**
     * ReduceScatterArray operation can ve viewed as a combination of {@link #reduceArray(Object, Operand, IOperator, int, int, int)}
     * and {@link #scatterArray(Object, Operand, int[], int[], int)}.
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param from start position of arrData to be ReduceScattered
     * @param counts rank i receive number of counts[i] elements reducescatter result.

     * @return rank 0 receive interval is [from, counts[0]),
     *         rank 1 receive interval is [from+count[0], from+count[0]+count[1]), ...
     * @throws Mp4jException
     */
    public <T> T reduceScatterArray(T arrData, Operand operand, IOperator operator, int from, int[] counts) throws Mp4jException {
        if (counts.length != slaveNum) {
            throw new Mp4jException("counts array length must be equal to slaveNum");
        }

        if (slaveNum == 1) {
            return arrData;
        }

        CommUtils.isFromCountsLegal(from, counts);

        try {
            int []froms = CommUtils.getFromsFromCount(from, counts, slaveNum);
            int []tos = CommUtils.getTosFromCount(from, counts, slaveNum);

            this.operand = operand;
            this.operand.setCollective(Collective.REDUCE_SCATTER);
            this.operand.setContainer(Container.ARRAY);
            this.operand.setOperator(operator);
            int sendRank = (rank - 1 + slaveNum) % slaveNum;

            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(rank)
                    .setDestRank((rank + 1) % slaveNum)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER)
                    .insert(rank, froms[sendRank], tos[sendRank]);
            arrayMetaData.setArrData(arrData);

            return (T)ringReduceScatter(arrayMetaData, Container.ARRAY).getArrData();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }


    protected  <T> List<Map<String, T>> reduceScatterMapSpecial(List<List<Map<String, T>>> mapDataListList, Operand operand, IOperator operator) throws Mp4jException {
        if (slaveNum == 1) {
            return mapDataListList.get(0);
        }

        if (mapDataListList.size() != slaveNum) {
            throw new Mp4jException("mapDataListList size=" + mapDataListList.size() + ", must be equal to slaveNum=" + slaveNum);
        }

        int blockNum = mapDataListList.get(0).size();
        try {
            this.operand = operand;
            this.operand.setCollective(Collective.REDUCE_SCATTER);
            this.operand.setContainer(Container.MAP);
            this.operand.setOperator(operator);

            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(rank)
                    .setDestRank((rank + 1) % slaveNum)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER);
            List<Map<String, T>> allmapDataList = new ArrayList<>();
            for (int i = 0; i < mapDataListList.size(); i++) {
                List<Map<String, T>> maps = mapDataListList.get(i);
                for (int j = 0; j < maps.size(); j++) {
                    allmapDataList.add(maps.get(j));
                }
            }

            List<Map<String, T>> sendMapList = new ArrayList<>();
            int sendIdx = (rank - 1 + slaveNum) % slaveNum;
            for (int t = 0; t < blockNum; t++) {
                int idx = sendIdx * blockNum + t;
                mapMetaData.insert(rank, allmapDataList.get(idx).size());
                sendMapList.add(allmapDataList.get(idx));
            }
            mapMetaData.setMapDataList(sendMapList);

            // send first block
            sendQueue.put(mapMetaData);

            // recv slaveNum - 1 times, send slaveNum - 2
            for (int step = 1; step < slaveNum; step++) {
                MapMetaData recvMapMetaData = new MapMetaData();
                List<Map<String, T>> recvMapList = new ArrayList<>();
                int recvIdx = (rank - (step + 1) + slaveNum) % slaveNum;
                for (int t = 0; t < blockNum; t++) {
                    int idx = recvIdx * blockNum + t;
                    recvMapMetaData.insert(rank, allmapDataList.get(idx).size());
                    recvMapList.add(allmapDataList.get(idx));
                }

                recvMapMetaData.setMapDataList(recvMapList);
                recvQueue.push(recvMapMetaData);

                MetaData<T> thatMetaData = recvResultQueue.take();

                mapMetaData = thatMetaData.convertToMapMetaData();
                mapMetaData.setSrcRank(rank)
                        .setDestRank((rank + 1) % slaveNum)
                        .setCollective(Collective.REDUCE_SCATTER);
                if (step < slaveNum - 1) {
                    sendQueue.put(mapMetaData);
                }

            }

            barrier();

            return mapMetaData.getMapDataList();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * reduceScatterMap operation can be viewed as a combination of {@link #reduceMap(Map, Operand, IOperator, int)}
     * and {@link #scatterMap(List, Operand, int)}.
     * @param mapDataList list of map
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return rank i process will receive reduce of mapDataList.get(i) in all process
     * @throws Mp4jException
     */
    public <T> Map<String, T> reduceScatterMap(List<Map<String, T>> mapDataList, Operand operand, IOperator operator) throws Mp4jException {
        if (slaveNum == 1) {
            return mapDataList.get(0);
        }

        if (mapDataList.size() != slaveNum) {
            throw new Mp4jException("mapDataList size=" + mapDataList.size() + ", must be equal to slaveNum=" + slaveNum);
        }

        try {
            this.operand = operand;
            this.operand.setCollective(Collective.REDUCE_SCATTER);
            this.operand.setContainer(Container.MAP);
            this.operand.setOperator(operator);
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(rank)
                    .setDestRank((rank + 1) % slaveNum)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER);
            mapMetaData.setMapDataList(mapDataList);

            MapMetaData<T> retMapMetaData = (MapMetaData<T>)ringReduceScatter(mapMetaData, Container.MAP).convertToMapMetaData();
            return retMapMetaData.getMapDataList().get(0);
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    private <T> MetaData<T> ringReduceScatter(MetaData thisMetaData, Container container) throws Mp4jException {
        try {
            List<Map<String, T>> storeMapList = null;

            if (container == Container.MAP){
                storeMapList = thisMetaData.getMapDataList();
                int sendIdx = (rank - 1 + slaveNum) % slaveNum;
                thisMetaData.insert(rank, storeMapList.get(sendIdx).size());
                thisMetaData.setMapDataList(Arrays.asList(storeMapList.get(sendIdx)));
            }

            // send first block
            sendQueue.put(thisMetaData);

            // recv slaveNum - 1 times, send slaveNum - 2
            for (int step = 1; step < slaveNum; step++) {
                if (container == Container.ARRAY) {
                    recvQueue.push(thisMetaData);
                } else if (container == Container.MAP) {
                    MapMetaData recvMapMetaData = new MapMetaData();
                    int recvIdx = (rank - (step + 1) + slaveNum) % slaveNum;
                    recvMapMetaData.insert(rank, storeMapList.get(recvIdx).size());
                    recvMapMetaData.setMapDataList(Arrays.asList(storeMapList.get(recvIdx)));
                    recvQueue.push(recvMapMetaData);
                }

                MetaData<T> thatMetaData = recvResultQueue.take();

                thisMetaData = thatMetaData;
                thatMetaData.setSrcRank(rank)
                        .setDestRank((rank + 1) % slaveNum)
                        .setCollective(Collective.REDUCE_SCATTER);
                if (step < slaveNum - 1) {
                    sendQueue.put(thatMetaData);
                }

            }

            barrier();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

        return thisMetaData;
    }



    /**
     * reduce all array elements, reduced result locate in root process
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param from start position to reduce
     * @param to end position to reduce
     * @param rootRank root rank

     * @return if this process is root, reduced array is returned, otherwise,
     *         is invalid array, contains intermediate result.
     * @throws Mp4jException
     */
    public <T> T reduceArray(T arrData, Operand operand, IOperator operator, int from, int to, int rootRank) throws Mp4jException {
        if (slaveNum == 1) {
            return arrData;
        }

        CommUtils.isFromToLegal(from, to);

        try {
            int[] counts = new int[slaveNum];
            int avg = (to - from) / slaveNum;
            for (int r = 0; r < slaveNum; r++) {
                counts[r] = avg;
            }
            counts[slaveNum - 1] = (to - from) - ((slaveNum - 1) * avg);

            int[] froms = new int[slaveNum];
            int[] tos = new int[slaveNum];
            int fromidx = from;
            for (int r = 0; r < slaveNum; r++) {
                froms[r] = fromidx;
                tos[r] = fromidx + avg;
                fromidx += avg;
            }
            tos[slaveNum - 1] = to;

            T reduceScatteredArr = reduceScatterArray(arrData, operand, operator, from, counts);
            return gatherArray(reduceScatteredArr, operand, froms, tos, rootRank);
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * single value reduce
     * @param value value to be reduced
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param rootRank root rank

     * @return if this process is root, reduced value is returned, otherwise, is a invalid value.
     * @throws Mp4jException
     */
    public <T> T reduce(T value, Operand operand, IOperator operator, int rootRank) throws Mp4jException {
        if (operand instanceof DoubleOperand) {
            double []doubleArr = new double[1];
            doubleArr[0] = (Double) value;
            doubleArr = reduceArray(doubleArr, operand, operator, 0, doubleArr.length, rootRank);
            return (T)Double.valueOf(doubleArr[0]);
        } else if (operand instanceof FloatOperand) {
            float []floatArr = new float[1];
            floatArr[0] = (Float) value;
            floatArr = reduceArray(floatArr, operand, operator, 0, floatArr.length, rootRank);
            return (T)Float.valueOf(floatArr[0]);
        } else if (operand instanceof IntOperand) {
            int []intArr = new int[1];
            intArr[0] = (Integer) value;
            intArr = reduceArray(intArr, operand, operator, 0, intArr.length, rootRank);
            return (T)Integer.valueOf(intArr[0]);
        } else if (operand instanceof LongOperand) {
            long []longArr = new long[1];
            longArr[0] = (Long) value;
            longArr = reduceArray(longArr, operand, operator, 0, longArr.length, rootRank);
            return (T)Long.valueOf(longArr[0]);
        } else if (operand instanceof ObjectOperand) {
            T []objectArr = (T[]) Array.newInstance(value.getClass(), 1);
            objectArr[0] = value;
            objectArr = reduceArray(objectArr, operand, operator, 0, objectArr.length, rootRank);
            return objectArr[0];
        } else if (operand instanceof StringOperand) {
            String []stringArr = new String[1];
            stringArr[0] = (String) value;
            stringArr = reduceArray(stringArr, operand, operator, 0, stringArr.length, rootRank);
            return (T)stringArr[0];
        } else if (operand instanceof ShortOperand) {
            short []shortArr = new short[1];
            shortArr[0] = (Short) value;
            shortArr = reduceArray(shortArr, operand, operator, 0, shortArr.length, rootRank);
            return (T)Short.valueOf(shortArr[0]);
        } else if (operand instanceof ByteOperand) {
            byte []byteArr = new byte[1];
            byteArr[0] = (Byte) value;
            byteArr = reduceArray(byteArr, operand, operator, 0, byteArr.length, rootRank);
            return (T)Byte.valueOf(byteArr[0]);
        } else {
            throw new Mp4jException("unknown operand:" + operand);
        }
    }

    /**
     * Similar with {@link #reduceArray(Object, Operand, IOperator, int, int, int)},
     * but the container is map.
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param rootRank root rank

     * @return if this process is root, reduced map is returned, otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, T> reduceMap(Map<String, T> mapData, Operand operand, IOperator operator, int rootRank) throws Mp4jException {
        if (slaveNum == 1) {
            return mapData;
        }
        try {
            List<Map<String, T>> listMapData = new ArrayList<>(slaveNum);
            for (int i = 0; i < slaveNum; i++) {
                listMapData.add(new HashMap<>(Math.max((int)((mapData.size() / slaveNum) * 1.2), 1)));
            }
            for (Map.Entry<String, T> entry : mapData.entrySet()) {
                String key = entry.getKey();
                T val = entry.getValue();
                int idx = key.hashCode() % slaveNum;
                if (idx < 0) {
                    LOG.info(key + ", code:" + idx);
                    idx += slaveNum;
                }
                listMapData.get(idx).put(key, val);
            }
            Map<String, T> reduceScatteredMap = reduceScatterMap(listMapData, operand, operator);

            return gatherMap(reduceScatteredMap, operand, rootRank);
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    protected static class Mp4jSetSerializer<T> extends Serializer<Set<T>> {
        Serializer<T> valSerializer;
        Class<T> valType;
        public Mp4jSetSerializer(Serializer<T> valSerializer, Class<T> valType) {
            this.valSerializer = valSerializer;
            this.valType = valType;
        }

        @Override
        public void write(Kryo kryo, Output output, Set<T> object) {
            output.writeInt(object.size());
            for (T val : object) {
                valSerializer.write(kryo, output, val);
            }
        }

        @Override
        public Set<T> read(Kryo kryo, Input input, Class<Set<T>> type) {
            int size = input.readInt();
            Set<T> set = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                set.add(valSerializer.read(kryo, input, this.valType));
            }
            return set;
        }
    }

    protected static class Mp4jListSerializer<T> extends Serializer<List<T>> {
        Serializer<T> valSerializer;
        Class<T> valType;
        public Mp4jListSerializer(Serializer<T> valSerializer, Class<T> valType) {
            this.valSerializer = valSerializer;
            this.valType = valType;
        }

        @Override
        public void write(Kryo kryo, Output output, List<T> object) {
            output.writeInt(object.size());
            for (T val : object) {
                valSerializer.write(kryo, output, val);
            }
        }

        @Override
        public List<T> read(Kryo kryo, Input input, Class<List<T>> type) {
            int size = input.readInt();
            List<T> list = new ArrayList<T>(size);
            for (int i = 0; i < size; i++) {
                list.add(valSerializer.read(kryo, input, this.valType));
            }
            return list;
        }
    }

    /**
     * Set union, the set with the same key will be reduced(union) together in the root process.
     * @param mapData map set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element obejct class

     * @return if this process is root, the set with the same key will be reduced together,
     *         otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> reduceMapSetUnion(Map<String, Set<T>> mapData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {

        Operand operand = Operands.OBJECT_OPERAND(new Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<Set<T>>() {
            @Override
            public Set<T> apply(Set<T> o1, Set<T> o2) {
                for (T val : o2) {
                    o1.add(val);
                }
                return o1;
            }
        };

        return reduceMap(mapData, operand, operator, rootRank);

    }

    /**
     * Set union
     * @param setData set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process is root, set unison is returned,
     *         otherwise invalid set or null is returned.
     * @throws Mp4jException
     */
    public <T> Set<T> reduceSetUnion(Set<T> setData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, Set<T>> mapTemp = new HashMap<>(1);
        mapTemp.put("key", setData);

        Map<String, Set<T>> mapReturn = reduceMapSetUnion(mapTemp, rootRank, elementSerializer, elementType);
        if (mapReturn != null) {
            return mapReturn.get("key");
        } else {
            return null;
        }
    }


    /**
     * Set intersection, the set with the same key will be reduced(intersect) together.
     * @param mapData map set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process is root, the set with the same key will be reduced(intersect) together.
     *         otherwise, invalid map is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> reduceMapSetIntersection(Map<String, Set<T>> mapData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Operand operand = Operands.OBJECT_OPERAND(new Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<Set<T>>() {
            @Override
            public Set<T> apply(Set<T> o1, Set<T> o2) {
                o1.retainAll(o2);
                return o1;
            }
        };

        return reduceMap(mapData, operand, operator, rootRank);
    }

    /**
     * Set intersection.
     * @param setData set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process is root, intersection is returned,
     *         otherwise, invalid set or null is returned.
     * @throws Mp4jException
     */
    public <T> Set<T> reduceSetIntersection(Set<T> setData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, Set<T>> mapTemp = new HashMap<>(1);
        mapTemp.put("key", setData);

        Map<String, Set<T>> mapReturn = reduceMapSetIntersection(mapTemp, rootRank, elementSerializer, elementType);
        if (mapReturn != null) {
            return mapReturn.get("key");
        } else {
            return null;
        }
    }

    /**
     * List concat, the lists with the same key will be reduced(concat) together.
     * @param mapData map list data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process is root, the lists with the same key will be reduced(concat) together,
     *         otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, List<T>> reduceMapListConcat(Map<String, List<T>> mapData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Operand operand = Operands.OBJECT_OPERAND(new Mp4jListSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<List<T>>() {
            @Override
            public List<T> apply(List<T> o1, List<T> o2) {
                for (T val : o2) {
                    o1.add(val);
                }
                return o1;
            }
        };

        return reduceMap(mapData, operand, operator, rootRank);
    }

    /**
     * List concat.
     * @param listData list data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process is root, the concated list is returned,
     *         otherwise, invalid list or null is returned.
     * @throws Mp4jException
     */
    public <T> List<T> reduceListConcat(List<T> listData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, List<T>> mapTemp = new HashMap<>(1);
        mapTemp.put("key", listData);

        Map<String, List<T>> mapReturn = reduceMapListConcat(mapTemp, rootRank, elementSerializer, elementType);
        if (mapReturn != null) {
            return mapReturn.get("key");
        } else {
            return null;
        }
    }

    /**
     * Different with reduce operation which only root process contains final reduced result,
     * while all process receive the same reduced result in allreduce operation.
     *
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param from start position to reduce
     * @param to end position to reduce

     * @return arrData[i](rank) = reduce(arrData[i](rank_0), arrData[i](rank_1), ..., arrData[i](rank_slavenumber-1)),
     * @throws Mp4jException
     */
    public <T> T allreduceArray(T arrData, Operand operand, IOperator operator, int from, int to) throws Mp4jException {
        if (slaveNum == 1) {
            return arrData;
        }

        CommUtils.isFromToLegal(from, to);

        try {
            int[] counts = new int[slaveNum];
            int avg = (to - from) / slaveNum;
            for (int r = 0; r < slaveNum; r++) {
                counts[r] = avg;
            }
            counts[slaveNum - 1] = (to - from) - ((slaveNum - 1) * avg);

            int[] froms = new int[slaveNum];
            int[] tos = new int[slaveNum];
            int fromidx = from;
            for (int r = 0; r < slaveNum; r++) {
                froms[r] = fromidx;
                tos[r] = fromidx + avg;
                fromidx += avg;
            }
            tos[slaveNum - 1] = to;

            T reduceScatteredArr = reduceScatterArray(arrData, operand, operator, from, counts);
            return allgatherArray(reduceScatteredArr, operand, froms, tos);
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * Similar with {@link #allreduceArray(Object, Operand, IOperator, int, int)},
     * but it's realized by rpc communication. It is suited to small data.
     *
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param <T>
     * @return arrData[i](rank) = reduce(arrData[i](rank_0), arrData[i](rank_1), ..., arrData[i](rank_slavenumber-1)),
     * @throws Mp4jException
     */
    public <T> T allreduceArrayRpc(T arrData, Operand operand, IOperator operator) throws Mp4jException {

        if (slaveNum == 1) {
            return arrData;
        }

        if (operand instanceof DoubleOperand) {
            ArrayPrimitiveWritable ret = server.primitiveArrayAllReduce(new ArrayPrimitiveWritable(arrData), rank);
            double []retArray = (double[])ret.get();
            double []thisArray = (double[])arrData;
            IDoubleOperator doubleOperator = (IDoubleOperator) operator;
            int idx = 0;
            for (int i = 0; i < slaveNum; i++) {
                if (i == 0) {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = retArray[idx + j];
                    }
                } else {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = doubleOperator.apply(thisArray[j], retArray[idx + j]);
                    }
                }
                idx += thisArray.length;
            }
        } else if (operand instanceof FloatOperand) {
            ArrayPrimitiveWritable ret = server.primitiveArrayAllReduce(new ArrayPrimitiveWritable(arrData), rank);
            float []retArray = (float[])ret.get();
            float []thisArray = (float[])arrData;
            IFloatOperator floatOperator = (IFloatOperator) operator;
            int idx = 0;
            for (int i = 0; i < slaveNum; i++) {
                if (i == 0) {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = retArray[idx + j];
                    }
                } else {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = floatOperator.apply(thisArray[j], retArray[idx + j]);
                    }
                }
                idx += thisArray.length;
            }
        } else if (operand instanceof IntOperand) {
            ArrayPrimitiveWritable ret = server.primitiveArrayAllReduce(new ArrayPrimitiveWritable(arrData), rank);
            int []retArray = (int[])ret.get();
            int []thisArray = (int[])arrData;
            IIntOperator intOperator = (IIntOperator) operator;
            int idx = 0;
            for (int i = 0; i < slaveNum; i++) {
                if (i == 0) {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = retArray[idx + j];
                    }
                } else {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = intOperator.apply(thisArray[j], retArray[idx + j]);
                    }
                }
                idx += thisArray.length;
            }
        } else if (operand instanceof LongOperand) {
            ArrayPrimitiveWritable ret = server.primitiveArrayAllReduce(new ArrayPrimitiveWritable(arrData), rank);
            long []retArray = (long[])ret.get();
            long []thisArray = (long[])arrData;
            ILongOperator longOperator = (ILongOperator) operator;
            int idx = 0;
            for (int i = 0; i < slaveNum; i++) {
                if (i == 0) {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = retArray[idx + j];
                    }
                } else {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = longOperator.apply(thisArray[j], retArray[idx + j]);
                    }
                }
                idx += thisArray.length;
            }
        } else if (operand instanceof ObjectOperand) {
            ObjectOperand objectOperand = (ObjectOperand)operand;
            objectOperand.setOperator(operator);
            ArrayPrimitiveWritable ret = server.arrayAllReduce(new ArrayPrimitiveWritable(objectOperand.convertToBytes(arrData)), rank);
            Input input = new Input(new ByteArrayInputStream((byte[])ret.get()));
            T retx = (T)objectOperand.readFromBytes(arrData, input, slaveNum);
            input.close();
            return retx;
        } else if (operand instanceof StringOperand) {
            String[] thisArray = (String []) arrData;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
            Output output = new Output(byteArrayOutputStream);
            KryoUtils.getKryo().writeClassAndObject(output, thisArray);
            output.close();
            ArrayPrimitiveWritable ret = server.arrayAllReduce(new ArrayPrimitiveWritable(byteArrayOutputStream.toByteArray()), rank);
            Input input = new Input(new ByteArrayInputStream((byte[])ret.get()));
            String []retArray;
            IStringOperator stringOperator = (IStringOperator) operator;
            for (int i = 0; i < slaveNum; i++) {
                retArray = (String[])KryoUtils.getKryo().readClassAndObject(input);
                if (i == 0) {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = retArray[j];
                    }
                } else {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = stringOperator.apply(thisArray[j], retArray[j]);
                    }
                }
            }
            input.close();
        } else if (operand instanceof ShortOperand) {
            ArrayPrimitiveWritable ret = server.primitiveArrayAllReduce(new ArrayPrimitiveWritable(arrData), rank);
            short []retArray = (short[])ret.get();
            short []thisArray = (short[])arrData;
            IShortOperator shortOperator = (IShortOperator) operator;
            int idx = 0;
            for (int i = 0; i < slaveNum; i++) {
                if (i == 0) {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = retArray[idx + j];
                    }
                } else {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = shortOperator.apply(thisArray[j], retArray[idx + j]);
                    }
                }
                idx += thisArray.length;
            }
        } else if (operand instanceof ByteOperand) {
            ArrayPrimitiveWritable ret = server.primitiveArrayAllReduce(new ArrayPrimitiveWritable(arrData), rank);
            byte []retArray = (byte[])ret.get();
            byte []thisArray = (byte[])arrData;
            IByteOperator byteOperator = (IByteOperator) operator;
            int idx = 0;
            for (int i = 0; i < slaveNum; i++) {
                if (i == 0) {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = retArray[idx + j];
                    }
                } else {
                    for (int j = 0; j < thisArray.length; j++) {
                        thisArray[j] = byteOperator.apply(thisArray[j], retArray[idx + j]);
                    }
                }
                idx += thisArray.length;
            }
        } else {
            throw new Mp4jException("unknown operand:" + operand);
        }

        return arrData;
    }


    /**
     * Similar with {@link #allreduceArray(Object, Operand, IOperator, int, int)},
     * this function only allreduce just only one elements.
     * @param value the value for reduce
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return all processes receive the same reduced value.
     * @throws Mp4jException
     */
    public <T> T allreduce(T value, Operand operand, IOperator operator) throws Mp4jException {
        if (operand instanceof DoubleOperand) {
            double []doubleArr = new double[1];
            doubleArr[0] = (Double) value;
            doubleArr = allreduceArray(doubleArr, operand, operator, 0, doubleArr.length);
            return (T)Double.valueOf(doubleArr[0]);
        } else if (operand instanceof FloatOperand) {
            float []floatArr = new float[1];
            floatArr[0] = (Float) value;
            floatArr = allreduceArray(floatArr, operand, operator, 0, floatArr.length);
            return (T)Float.valueOf(floatArr[0]);
        } else if (operand instanceof IntOperand) {
            int []intArr = new int[1];
            intArr[0] = (Integer) value;
            intArr = allreduceArray(intArr, operand, operator, 0, intArr.length);
            return (T)Integer.valueOf(intArr[0]);
        } else if (operand instanceof LongOperand) {
            long []longArr = new long[1];
            longArr[0] = (Long) value;
            longArr = allreduceArray(longArr, operand, operator, 0, longArr.length);
            return (T)Long.valueOf(longArr[0]);
        } else if (operand instanceof ObjectOperand) {
            T []objectArr = (T[]) Array.newInstance(value.getClass(), 1);
            objectArr[0] = value;
            objectArr = allreduceArray(objectArr, operand, operator, 0, objectArr.length);
            return objectArr[0];
        } else if (operand instanceof StringOperand) {
            String []stringArr = new String[1];
            stringArr[0] = (String) value;
            stringArr = allreduceArray(stringArr, operand, operator, 0, stringArr.length);
            return (T)stringArr[0];
        } else if (operand instanceof ShortOperand) {
            short []shortArr = new short[1];
            shortArr[0] = (Short) value;
            shortArr = allreduceArray(shortArr, operand, operator, 0, shortArr.length);
            return (T)Short.valueOf(shortArr[0]);
        } else if (operand instanceof ByteOperand) {
            byte []byteArr = new byte[1];
            byteArr[0] = (Byte) value;
            byteArr = allreduceArray(byteArr, operand, operator, 0, byteArr.length);
            return (T)Byte.valueOf(byteArr[0]);
        } else {
            throw new Mp4jException("unknown operand:" + operand);
        }
    }

    /**
     * Similar with {@link #allreduce(Object, Operand, IOperator)},
     * but it's realized by rpc communication. It is suited to small data.
     *
     * @param value the value for reduce
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param <T>
     * @return
     * @throws Mp4jException
     */
    public <T> T allreduceRpc(T value, Operand operand, IOperator operator) throws Mp4jException {
        if (operand instanceof DoubleOperand) {
            double []doubleArr = new double[1];
            doubleArr[0] = (Double) value;
            doubleArr = allreduceArrayRpc(doubleArr, operand, operator);
            return (T)Double.valueOf(doubleArr[0]);
        } else if (operand instanceof FloatOperand) {
            float []floatArr = new float[1];
            floatArr[0] = (Float) value;
            floatArr = allreduceArrayRpc(floatArr, operand, operator);
            return (T)Float.valueOf(floatArr[0]);
        } else if (operand instanceof IntOperand) {
            int []intArr = new int[1];
            intArr[0] = (Integer) value;
            intArr = allreduceArrayRpc(intArr, operand, operator);
            return (T)Integer.valueOf(intArr[0]);
        } else if (operand instanceof LongOperand) {
            long []longArr = new long[1];
            longArr[0] = (Long) value;
            longArr = allreduceArrayRpc(longArr, operand, operator);
            return (T)Long.valueOf(longArr[0]);
        } else if (operand instanceof ObjectOperand) {
            T []objectArr = (T[]) Array.newInstance(value.getClass(), 1);
            objectArr[0] = value;
            objectArr = allreduceArrayRpc(objectArr, operand, operator);
            return objectArr[0];
        } else if (operand instanceof StringOperand) {
            String []stringArr = new String[1];
            stringArr[0] = (String) value;
            stringArr = allreduceArrayRpc(stringArr, operand, operator);
            return (T)stringArr[0];
        } else if (operand instanceof ShortOperand) {
            short []shortArr = new short[1];
            shortArr[0] = (Short) value;
            shortArr = allreduceArrayRpc(shortArr, operand, operator);
            return (T)Short.valueOf(shortArr[0]);
        } else if (operand instanceof ByteOperand) {
            byte []byteArr = new byte[1];
            byteArr[0] = (Byte) value;
            byteArr = allreduceArrayRpc(byteArr, operand, operator);
            return (T)Byte.valueOf(byteArr[0]);
        } else {
            throw new Mp4jException("unknown operand:" + operand);
        }
    }

    /**
     * Similar with {@link #allreduceArray(Object, Operand, IOperator, int, int)},
     * the container of this function is map, the values which have same key will be reduced,
     * different processes can contain the same and different keys.
     * @param mapData map to be reduced
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return key value map, the values which have same key will be reduced together.
     * @throws Mp4jException
     */
    public <T> Map<String, T> allreduceMap(Map<String, T> mapData, Operand operand, IOperator operator) throws Mp4jException {
        if (slaveNum == 1) {
            return mapData;
        }

        try {
            List<Map<String, T>> listMapData = new ArrayList<>(slaveNum);
            for (int i = 0; i < slaveNum; i++) {
                listMapData.add(new HashMap<>(Math.max((int)((mapData.size() / slaveNum) * 1.2), 1)));
            }
            for (Map.Entry<String, T> entry : mapData.entrySet()) {
                String key = entry.getKey();
                T val = entry.getValue();
                int idx = key.hashCode() % slaveNum;
                if (idx < 0) {
                    LOG.info(key + ", code:" + idx);
                    idx += slaveNum;
                }
                listMapData.get(idx).put(key, val);
            }
            Map<String, T> reduceScatteredMap = reduceScatterMap(listMapData, operand, operator);
            List<Map<String, T>> allreducedMapList = allgatherMap(reduceScatteredMap, operand);
            Map<String, T> retMap = allreducedMapList.get(0);
            for (int i = 1; i < allreducedMapList.size(); i++) {
                Map<String, T> retMapTemp = allreducedMapList.get(i);
                for (Map.Entry<String, T> entry : retMapTemp.entrySet()) {
                    retMap.put(entry.getKey(), entry.getValue());
                }
            }

            return retMap;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * Set union, the set with the same key will be reduced(union) together.
     * @param mapData map set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return the set with the same key will be reduced together.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> allreduceMapSetUnion(Map<String, Set<T>> mapData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {

        Operand operand = Operands.OBJECT_OPERAND(new Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<Set<T>>() {
            @Override
            public Set<T> apply(Set<T> o1, Set<T> o2) {
                for (T val : o2) {
                    o1.add(val);
                }
                return o1;
            }
        };

        return allreduceMap(mapData, operand, operator);

    }

    /**
     * Set union
     * @param setData set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return set union result
     * @throws Mp4jException
     */
    public <T> Set<T> allreduceSetUnion(Set<T> setData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, Set<T>> mapTemp = new HashMap<>(1);

        mapTemp.put("key", setData);
        return allreduceMapSetUnion(mapTemp, elementSerializer, elementType).get("key");

    }


    /**
     * Set intersection, the set with the same key will be reduced(intersect) together.
     * @param mapData map set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return the set with the same key will be reduced(intersect) together.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> allreduceMapSetIntersection(Map<String, Set<T>> mapData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Operand operand = Operands.OBJECT_OPERAND(new Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<Set<T>>() {
            @Override
            public Set<T> apply(Set<T> o1, Set<T> o2) {
                o1.retainAll(o2);
                return o1;
            }
        };

        return allreduceMap(mapData, operand, operator);
    }

    /**
     * Set intersection
     * @param setData set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return set intersected result
     * @throws Mp4jException
     */
    public <T> Set<T> allreduceSetIntersection(Set<T> setData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, Set<T>> mapTemp = new HashMap<>(1);

        mapTemp.put("key", setData);
        return allreduceMapSetIntersection(mapTemp, elementSerializer, elementType).get("key");
    }


    /**
     * List concat, the lists with the same key will be reduced(concat) together.
     * @param mapData map list data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return the lists with the same key will be reduced(concat) together.
     * @throws Mp4jException
     */
    public <T> Map<String, List<T>> allreduceMapListConcat(Map<String, List<T>> mapData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Operand operand = Operands.OBJECT_OPERAND(new Mp4jListSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<List<T>>() {
            @Override
            public List<T> apply(List<T> o1, List<T> o2) {
                for (T val : o2) {
                    o1.add(val);
                }
                return o1;
            }
        };

        return allreduceMap(mapData, operand, operator);
    }

    /**
     * list concat
     * @param listData list data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return list concated result
     * @throws Mp4jException
     */
    public <T> List<T> allreduceListConcat(List<T> listData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, List<T>> mapTemp = new HashMap<>(1);

        mapTemp.put("key", listData);
        return allreduceMapListConcat(mapTemp, elementSerializer, elementType).get("key");
    }

    private Input getInput() throws Mp4jException {
        Socket recvDataSock = null;
        Input input = null;

        try {
            recvDataSock = getRecvDataSocket();
            input = new Input(recvDataSock.getInputStream());
            LOG.debug("getInput:" + recvDataSock.isClosed());
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
        return input;
    }

    private Output getOutput(int targetRank) throws Mp4jException {
        Socket sendDataSock = null;
        Output output = null;
        try {
            sendDataSock = getSendDataSocket(targetRank);
            output = new Output(sendDataSock.getOutputStream());
            LOG.debug("getOutput:" + sendDataSock.isClosed());
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
        return output;
    }

}
