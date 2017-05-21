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

import com.esotericsoftware.kryo.Serializer;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.meta.ArrayMetaData;
import com.fenbi.mp4j.meta.MapMetaData;
import com.fenbi.mp4j.meta.MetaData;
import com.fenbi.mp4j.operand.*;
import com.fenbi.mp4j.operator.Collective;
import com.fenbi.mp4j.operator.Container;
import com.fenbi.mp4j.operator.IObjectOperator;
import com.fenbi.mp4j.operator.IOperator;
import com.fenbi.mp4j.utils.CommUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * ThreadCommSlave is used as Multi-processes, Multi-threads communication.
 * @author xialong
 */
public class ThreadCommSlave {
    public static final Logger LOG = Logger.getLogger(ThreadCommSlave.class);
    private final Exchanger<MetaData> exch = new Exchanger<MetaData>();

    private final int threadNum;
    private final CyclicBarrier barrier;

    private final ProcessCommSlave processCommSlave;
    private volatile boolean isClosed = false;
    private final ThreadLocal<Integer> threadIdLocal = new ThreadLocal<>();
    private final ThreadLocal<Operand> threadOperandLocal = new ThreadLocal<>();
    private final int rank;
    private final int slaveNum;
    private final BlockingQueue<MetaData> threadBQueues[];

    private volatile MetaData delegatingMetaData;

    /**
     * Process communication constructor, every process have just only one ThreadCommSlave instance.
     * @param loginName if you use ssh to execute command, you must provide login name, e.g. ssh loginName@host "your command"
     * @param threadNum thread number in each process.
     * @param masterHost master host name
     * @param masterPort master host port
     * @throws Mp4jException
     */
    public ThreadCommSlave(String loginName, int threadNum, String masterHost, int masterPort) throws Mp4jException {
        this.threadNum = threadNum;
        this.barrier = new CyclicBarrier(threadNum);
        try {
            processCommSlave = new ProcessCommSlave(loginName, masterHost, masterPort);
            this.rank = processCommSlave.getRank();
            this.slaveNum = processCommSlave.getSlaveNum();
            this.threadBQueues = new LinkedBlockingDeque[threadNum];
            for (int t = 0; t < threadNum; t++) {
                this.threadBQueues[t] = new LinkedBlockingDeque<>();
            }
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * get thread num in each process
     * @return thread num
     */
    public int getThreadNum() {
        return threadNum;
    }

    /**
     * Is this comm closed
     * @return
     */
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * set thread id
     * @param threadId must be a value from 0 to threadNum - 1
     */
    public void setThreadId(int threadId) {
        threadIdLocal.set(threadId);
    }

    /**
     * get thread id
     * @return thread is, if you haven't set thread id in current thread, 0 is returned
     */
    public int getThreadId() {
        Integer tidx = threadIdLocal.get();
        if (tidx == null) {
            tidx = 0;
        }
        return tidx;
    }

    private void setOperand(Operand operand) {
        threadOperandLocal.set(operand);
    }

    private Operand getOperand() {
        return threadOperandLocal.get();
    }

    /**
     * synchronizing threads in this process.
     * @throws Mp4jException
     */
    public void threadBarrier() throws Mp4jException {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * synchronizing all processes and threads
     * @throws Mp4jException
     */
    public void barrier() throws Mp4jException {
        threadBarrier();
        if (getThreadId() == 0) {
            processCommSlave.barrier();
        }
        threadBarrier();
    }

    /**
     * the rank of this process
     * @return
     */
    public int getRank() {
        return rank;
    }

    /**
     * slave number
     * @return
     */
    public int getSlaveNum() {
        return slaveNum;
    }

    /**
     * only rank 0 and thread 0 can send information successfully.
     * @param info information
     * @throws Mp4jException
     */
    public void info(String info) throws Mp4jException {
        if (getThreadId() == 0) {
            processCommSlave.info(info);
        }
    }

    /**
     * send information to master
     * @param info
     * @param onlyRank0Thread0 if just rank 0 and thread 0 can send information successfully.
     * @throws Mp4jException
     */
    public void info(String info, boolean onlyRank0Thread0) throws Mp4jException {
        if (onlyRank0Thread0) {
            if (getThreadId() == 0) {
                processCommSlave.info(info);
            }
        } else {
            processCommSlave.info("[threadId=" + getThreadId() + "] " + info, false);
        }
    }

    /**
     * only rank 0 and thread 0 can send debug information successfully.
     * @param debug debug information.
     * @throws Mp4jException
     */
    public void debug(String debug) throws Mp4jException {
        processCommSlave.debug(debug);
    }

    /**
     * send debug information to master
     * @param debug debug information
     * @param onlyRank0Thread0 if just rank 0 and thread 0 can send information successfully.
     * @throws Mp4jException
     */
    public void debug(String debug, boolean onlyRank0Thread0) throws Mp4jException {
        if (onlyRank0Thread0) {
            if (getThreadId() == 0) {
                processCommSlave.debug(debug);
            }
        } else {
            processCommSlave.debug("[threadId=" + getThreadId() + " ] " + debug, false);
        }
    }

    /**
     * send error information to master
     * @param error error information
     * @throws Mp4jException
     */
    public void error(String error) throws Mp4jException {
        processCommSlave.error(error);
    }

    /**
     * send exception information to master
     * @param e exception
     * @throws Mp4jException
     */
    public void exception(Exception e) throws Mp4jException {
        processCommSlave.exception(e);
    }

    /**
     * close communication
     * @param code close code
     * @throws Mp4jException
     */
    public void close(int code) throws Mp4jException {
        synchronized (processCommSlave) {
            if (!isClosed) {
                processCommSlave.close(code);
                isClosed = true;
            }
        }
    }

    private <T> MetaData<T> threadExchange(MetaData thisMetaData, int rootThreadId, int func) throws Mp4jException {

        try {
            thisMetaData.setSrcRank(getThreadId());
            MetaData<T> thatMetaData = exch.exchange(thisMetaData);
            int thisThreadId = thisMetaData.getSrcRank();
            int thatThreadId = thatMetaData.getSrcRank();

            if ((thisThreadId < thatThreadId ||
                    thisThreadId == rootThreadId) &&
                    thatThreadId != rootThreadId) {
                while (true) {
                    if (func == 0) {
                        getOperand().threadMerge(thatMetaData, thisMetaData);
                    } else if (func == 1) {
                        getOperand().threadReduce(thatMetaData, thisMetaData);
                    }

                    if (thisMetaData.getSum() != threadNum) {
                        thatMetaData = exch.exchange(thisMetaData);
                        thatThreadId = thatMetaData.getSrcRank();

                        if (!((thisThreadId < thatThreadId ||
                                thisThreadId == rootThreadId) &&
                                thatThreadId != rootThreadId)) {
                            break;
                        }
                    } else {
                        if (thisThreadId != rootThreadId) {
                            throw new Mp4jException("thread gather must stop at rootThreadId:" + rootThreadId + ", thisThreadId:" + thisThreadId);
                        }
                        barrier.await();
                        LOG.debug("thread finished");
                        return thisMetaData;
                    }
                }
            }

            barrier.await();
            return thisMetaData;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    private <T> T threadReduce(T arrData, Operand operand, IOperator operator, int from, int to, int rootThreadId) throws Mp4jException {


        if (threadNum == 1) {
            return arrData;
        }

        CommUtils.isFromToLegal(from, to);

        try {
            int avg = (to - from) / threadNum;

            int[] froms = new int[threadNum];
            int[] tos = new int[threadNum];
            int fromidx = from;
            for (int r = 0; r < threadNum; r++) {
                froms[r] = fromidx;
                tos[r] = fromidx + avg;
                fromidx += avg;
            }
            tos[threadNum - 1] = to;

            int threadId = getThreadId();

            // thread reduce scatter
            operand.setCollective(Collective.REDUCE_SCATTER);
            operand.setContainer(Container.ARRAY);
            operand.setOperator(operator);
            setOperand(operand);

            int sendRank = (threadId - 1 + threadNum) % threadNum;

            ArrayMetaData<T> thisMetaData = new ArrayMetaData<>();
            thisMetaData.setSrcRank(threadId)
                    .setSum(1)
                    .insert(threadId, froms[sendRank], tos[sendRank]);
            thisMetaData.setArrData(arrData);

            // send first block
            threadBQueues[(threadId + 1) % threadNum].add(thisMetaData);

            // recv threadNum - 1 times, send threadNum - 2
            for (int step = 1; step < threadNum; step++) {

                MetaData<T> thatMetaData = threadBQueues[threadId].take();
                ArrayMetaData<T> newMetaData = new ArrayMetaData<>();
                newMetaData.setArrData(arrData);
                getOperand().threadReduce(thatMetaData, newMetaData);
                newMetaData.setSegNum(1);
                newMetaData.setSegFroms(thatMetaData.convertToArrayMetaData().getSegFroms());
                newMetaData.setSegTos(thatMetaData.convertToArrayMetaData().getSegTos());

                if (step < threadNum - 1) {
                    threadBQueues[(threadId + 1) % threadNum].add(newMetaData);
                }
                thisMetaData = newMetaData;

            }
            thisMetaData.setSegNum(1);
            thisMetaData.setSum(1);
            thisMetaData.getSegFroms().set(0, froms[threadId]);
            thisMetaData.getSegTos().set(0, tos[threadId]);

            threadBarrier();

            // thread gather
            thisMetaData.setSrcRank(getThreadId());
            MetaData<T> thatMetaData = exch.exchange(thisMetaData);
            int thisThreadId = thisMetaData.getSrcRank();
            int thatThreadId = thatMetaData.getSrcRank();

            if ((thisThreadId < thatThreadId ||
                    thisThreadId == rootThreadId) &&
                    thatThreadId != rootThreadId) {
                while (true) {
                    getOperand().threadMerge(thatMetaData, thisMetaData);
                    if (thisMetaData.getSum() != threadNum) {
                        thatMetaData = exch.exchange(thisMetaData);
                        thatThreadId = thatMetaData.getSrcRank();

                        if (!((thisThreadId < thatThreadId ||
                                thisThreadId == rootThreadId) &&
                                thatThreadId != rootThreadId)) {
                            break;
                        }
                    } else {
                        if (thisThreadId != rootThreadId) {
                            throw new Mp4jException("thread gather must stop at rootThreadId:" + rootThreadId + ", thisThreadId:" + thisThreadId);
                        }
                        threadBarrier();
                        LOG.info("thread finished");
                        return thisMetaData.getArrData();
                    }
                }
            }

            threadBarrier();

            return null;

        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#gatherArray(Object, Operand, int[], int[], int)} actually,
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.gatherArrayProcess
     * </pre></blockquote>
     *  process i send data interval [sendfroms[i], sendtos[i]) to root process, placed in the same positions,
     * s.t. sendfroms[i] &le; sendtos[i], sendfroms[i] &ge; sendtos[i-1]
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param sendfroms sending start positions, included.
     * @param sendtos sending end positions, excluded
     * @param rootRank root rank
      primitive types array or Object array
     * @return if this process is root, returned is gathered elements, otherwise,
     *         is invalid array, contains intermediate result.
     * @throws Mp4jException
     */
    public <T> T gatherArrayProcess(T arrData, Operand operand, int[] sendfroms, int[] sendtos, int rootRank) throws Mp4jException {
        return processCommSlave.gatherArray(arrData, operand, sendfroms, sendtos, rootRank);
    }

    /**
     * Takes elements from all processes and all threads and gathers them to root process and root thread, the data container is array,
     * process i thread j send data interval [sendfroms[i][j], sendtos[i][j]) to root process and root thread, placed in the same positions,
     * s.t. sendfroms[i][j] &le; sendtos[i][j], sendfroms[i][j] &ge; sendtos[i][j-1], sendfroms[i][0] &ge; sendtos[i-1][threadNum - 1]
     * @param arrData data array, each process and thread have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param sendfroms start position of process i, thread j is sendfroms[i][j]
     * @param sendtos end position of process i, thread j is sendtos[i][j]
     * @param rootRank root rank
     * @param rootThreadId root thread id

     * @return if this process and thread is root, returned is gathered elements, otherwise,
     *         is invalid array, contains intermediate result.
     * @throws Mp4jException
     */
    public <T> T gatherArray(T arrData, Operand operand, int[][] sendfroms, int[][] sendtos, int rootRank, int rootThreadId) throws Mp4jException {

        //threadBarrier();
        if (sendfroms.length != slaveNum) {
            throw new Mp4jException("sendfroms array length:" + sendfroms.length + " must be equal to slaveNum:" + slaveNum);
        }

        if (sendtos.length != slaveNum) {
            throw new Mp4jException("sendtos array length:" + sendtos.length + " must be equal to slaveNum:" + slaveNum);
        }

        for (int i = 0; i < slaveNum; i++) {
            CommUtils.isfromsTosLegal(sendfroms, sendtos, threadNum);
        }


        try {
            if (threadNum == 1) {
                int[] processFroms = CommUtils.getProcessFroms(sendfroms);
                int[] processTos = CommUtils.getProcessTos(sendtos);
                return processCommSlave.gatherArray(arrData, operand, processFroms, processTos, rootRank);
            }

            // thread gather
            operand.setCollective(Collective.GATHER);
            operand.setContainer(Container.ARRAY);
            setOperand(operand);

            int threadId = getThreadId();
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.GATHER)
                    .insert(threadId, sendfroms[rank][threadId], sendtos[rank][threadId]);
            arrayMetaData.setArrData(arrData);

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;
            ArrayMetaData<T> threadGatheredMetaData = (ArrayMetaData<T>) threadExchange(arrayMetaData, realRootThreadId, 0);

            // process gather
            if (getThreadId() == realRootThreadId) {
                int[] processFroms = CommUtils.getProcessFroms(sendfroms);
                int[] processTos = CommUtils.getProcessTos(sendtos);
                T processGatheredArr = processCommSlave.gatherArray(threadGatheredMetaData.getArrData(), operand, processFroms, processTos, rootRank);

                if (rank == rootRank) {
                    threadBarrier();
                    return processGatheredArr;
                }
            }
            threadBarrier();
            return arrData;

        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#gatherMap(Map, Operand, int)} actually,
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.gatherMapProcess
     * </pre></blockquote>
     * Takes elements from all processes and gathers them to root process, the data container is map,
     * the data in map regardless of the order, if different process have same keys, only one key(random) will
     * be saved in root process.
     *
     * @param mapData key is {@code String}, value is any object
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return if this process is root, returned is gathered elements, otherwise,
     *         is invalid map, contained intermediate comm result or null.
     * @throws Mp4jException
     */
    public <T> Map<String, T> gatherMapProcess(Map<String, T> mapData, Operand operand, int rootRank) throws Mp4jException {
        return processCommSlave.gatherMap(mapData, operand, rootRank);
    }


    /**
     * Takes elements from all processes and threads and gathers them to root process and thread, the data container is map,
     * the data in map regardless of the order, if different process and thread have same keys, only one key(random) will
     * be saved in root process and thread.
     * @param mapData key is {@code String}, value is any object
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank
     * @param rootThreadId root thread

     * @return if this process and thread is root, returned is gathered elements, otherwise,
     *         is invalid map, contained intermediate comm result or null.
     * @throws Mp4jException
     */
    public <T> Map<String, T> gatherMap(Map<String, T> mapData, Operand operand, int rootRank, int rootThreadId) throws Mp4jException {

        //threadBarrier();
        try {
            if (threadNum == 1) {
                return processCommSlave.gatherMap(mapData, operand, rootRank);
            }

            // thread gather
            operand.setCollective(Collective.GATHER);
            operand.setContainer(Container.MAP);
            setOperand(operand);

            int threadId = getThreadId();
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.GATHER)
                    .insert(threadId, mapData.size());
            List<Map<String, T>> listMapList = new ArrayList<>();
            listMapList.add(mapData);
            mapMetaData.setMapDataList(listMapList);

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;
            MapMetaData<T> threadGatheredMetaData = (MapMetaData<T>) threadExchange(mapMetaData, realRootThreadId, 0);

            // process gather
            if (getThreadId() == realRootThreadId) {
                Map<String, T> processGatheredMap = processCommSlave.gatherMap(threadGatheredMetaData.getMapDataList().get(0), operand, rootRank);
                if (rank == rootRank) {
                    threadBarrier();
                    return processGatheredMap;
                }
            }

            threadBarrier();
            return mapData;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allgatherArray(Object, Operand, int[], int[])} actually,
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allgatherArrayProcess
     * </pre></blockquote>
     * Takes elements from all processes and gathers them to all processes.
     * the operation can be viewed as a combination of gather and broadcast.
     *
     * @param arrData data array, each process have the same length.
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param froms the start position of process i to gathered is froms[i]
     * @param tos the end position of process i to gathered is tos[i]

     * @return allgathered array
     * @throws Mp4jException
     */
    public <T> T allgatherArrayProcess(T arrData, Operand operand, int[] froms, int[] tos) throws Mp4jException {
        return processCommSlave.allgatherArray(arrData, operand, froms, tos);
    }

    /**
     * Takes elements from all processes and threads and gathers them to all processes and threads.
     * the operation can be viewed as a combination of gather and broadcast.
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param sendfroms start position of process i, thread j is sendfroms[i][j]
     * @param sendtos end position of process i, thread j is sendtos[i][j]

     * @return allgathered array
     * @throws Mp4jException
     */
    public <T> T allgatherArray(T arrData, Operand operand, int[][] sendfroms, int[][] sendtos) throws Mp4jException {

        //threadBarrier();
        if (sendfroms.length != slaveNum) {
            throw new Mp4jException("sendfroms array length:" + sendfroms.length + " must be equal to slaveNum:" + slaveNum);
        }

        if (sendtos.length != slaveNum) {
            throw new Mp4jException("sendtos array length:" + sendtos.length + " must be equal to slaveNum:" + slaveNum);
        }

        for (int i = 0; i < slaveNum; i++) {
            CommUtils.isfromsTosLegal(sendfroms, sendtos, threadNum);
        }


        try {
            if (threadNum == 1) {
                int[] processFroms = CommUtils.getProcessFroms(sendfroms);
                int[] processTos = CommUtils.getProcessTos(sendtos);
                return processCommSlave.allgatherArray(arrData, operand, processFroms, processTos);
            }

            // thread gather
            operand.setCollective(Collective.GATHER);
            operand.setContainer(Container.ARRAY);
            setOperand(operand);

            int threadId = getThreadId();
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.GATHER)
                    .insert(threadId, sendfroms[rank][threadId], sendtos[rank][threadId]);
            arrayMetaData.setArrData(arrData);

            ArrayMetaData<T> threadGatheredMetaData = (ArrayMetaData<T>) threadExchange(arrayMetaData, 0, 0);

            // process gather
            if (getThreadId() == 0) {
                int[] processFroms = CommUtils.getProcessFroms(sendfroms);
                int[] processTos = CommUtils.getProcessTos(sendtos);
                T processAllgatheredArr = processCommSlave.allgatherArray(threadGatheredMetaData.getArrData(), operand, processFroms, processTos);

                ArrayMetaData<T> rootArrayMetaData = new ArrayMetaData<>();
                rootArrayMetaData.insert(threadId, processFroms[0], processTos[processTos.length - 1]);
                rootArrayMetaData.setArrData(processAllgatheredArr);
                delegatingMetaData = rootArrayMetaData;
                threadBarrier();
                threadBarrier();
                return processAllgatheredArr;
            } else {
                threadBarrier();
                operand.threadArrayAllCopy(delegatingMetaData, arrayMetaData);
                threadBarrier();

                return arrayMetaData.getArrData();
            }

        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allgatherMap(Map, Operand)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allgatherMapProcess
     * </pre></blockquote>
     * Takes elements from all processes and gathers them to all processes.
     *
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})

     * @return map list
     * @throws Mp4jException
     */
    public <T> List<Map<String, T>> allgatherMapProcess(Map<String, T> mapData, Operand operand) throws Mp4jException {
        return processCommSlave.allgatherMap(mapData, operand);
    }

    /**
     * Takes elements from all processes and threads and gathers them to all processes and threads.
     * Similar with {@link #allgatherArray(Object, Operand, int[][], int[][])}, this container is map.

     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})

     * @return map list
     *         Attention: all threads in the same process shared the same result(reduce memory use and gc),
     *                    if you want to modify in different threads, you must clone a duplicate ahead of modification.
     * @throws Mp4jException
     */
    public <T> List<Map<String, T>> allgatherMap(Map<String, T> mapData, Operand operand) throws Mp4jException {

        //threadBarrier();
        try {
            if (threadNum == 1) {
                return processCommSlave.allgatherMap(mapData, operand);
            }

            // thread gather
            operand.setCollective(Collective.GATHER);
            operand.setContainer(Container.MAP);
            setOperand(operand);

            int threadId = getThreadId();
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.GATHER)
                    .insert(threadId, mapData.size());
            List<Map<String, T>> listMapList = new ArrayList<>();
            listMapList.add(mapData);
            mapMetaData.setMapDataList(listMapList);

            MapMetaData<T> threadGatheredMetaData = (MapMetaData<T>) threadExchange(mapMetaData, 0, 0);

            // process gather
            if (getThreadId() == 0) {
                List<Map<String, T>> processAllgatheredMap = processCommSlave.allgatherMap(threadGatheredMetaData.getMapDataList().get(0), operand);

                MapMetaData<T> rootMapMetaData = new MapMetaData<>();
                rootMapMetaData.setMapDataList(processAllgatheredMap);
                delegatingMetaData = rootMapMetaData;

                threadBarrier();
                threadBarrier();
                return processAllgatheredMap;
            } else {
                threadBarrier();
                operand.threadCopy(delegatingMetaData, mapMetaData);
                threadBarrier();

                return mapMetaData.getMapDataList();
            }

        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#broadcastArray(Object, Operand, int, int, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.broadcastArrayProcess
     * </pre></blockquote>
     * Broadcast array in root process to all other processe(included itself).
     *
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param from start position to broadcast
     * @param to end position to broadcast
     * @param rootRank root rank

     * @return root array , interval is [from, to)
     * @throws Mp4jException
     */
    public <T> T broadcastArrayProcess(T arrData, Operand operand, int from, int to, int rootRank) throws Mp4jException {
        return processCommSlave.broadcastArray(arrData, operand, from, to, rootRank);
    }

    /**
     * Broadcast array in root process and thread to all other processes and threads(included itself).
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param from start position to broadcast
     * @param to end position to broadcast
     * @param rootRank root rank
     * @param rootThreadId root thread id

     * @return root array, interval is [from, to)
     * @throws Mp4jException
     */
    public <T> T broadcastArray(T arrData, Operand operand, int from, int to, int rootRank, int rootThreadId) throws Mp4jException {
        //threadBarrier();
        try {

            if (threadNum == 1) {
                return processCommSlave.broadcastArray(arrData, operand, from, to, rootRank);
            }

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;

            if (getThreadId() == realRootThreadId) {
                T rootBrcstArr = processCommSlave.broadcastArray(arrData, operand, from, to, rootRank);

                ArrayMetaData<T> rootArrayMetaData = new ArrayMetaData<>();
                rootArrayMetaData.setArrData(rootBrcstArr);
                delegatingMetaData = rootArrayMetaData;

                threadBarrier();
                threadBarrier();
                return rootBrcstArr;
            } else {
                threadBarrier();
                ArrayMetaData<T> thisArrayMetaData = new ArrayMetaData<>();
                thisArrayMetaData.setArrData(arrData);
                operand.threadArrayAllCopy(delegatingMetaData, thisArrayMetaData);
                threadBarrier();

                return thisArrayMetaData.getArrData();
            }
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#broadcast(Object, Operand, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.broadcastProcess
     * </pre></blockquote>
     * Broadcast single value in root process to all other processes(included itself).
     *
     * @param value value to be broadcast(only root process is valid)
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return the value of root process.
     * @throws Mp4jException
     */
    public <T> T broadcastProcess(T value, Operand operand, int rootRank) throws Mp4jException {
        return processCommSlave.broadcast(value, operand, rootRank);
    }

    /**
     * Broadcast single value in root process and thread to all other processes * threads(included itself).
     * @param value value to be broadcast(only root process is valid)
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank
     * @param rootThreadId root threadId

     * @return the value of root process and thread.
     * @throws Mp4jException
     */
    public <T> T broadcast(T value, Operand operand, int rootRank, int rootThreadId) throws Mp4jException {
        if (operand instanceof DoubleOperand) {
            double []doubleArr = new double[1];
            doubleArr[0] = (Double) value;
            doubleArr = broadcastArray(doubleArr, operand, 0, doubleArr.length, rootRank, rootThreadId);
            return (T)Double.valueOf(doubleArr[0]);
        } else if (operand instanceof FloatOperand) {
            float []floatArr = new float[1];
            floatArr[0] = (Float) value;
            floatArr = broadcastArray(floatArr, operand, 0, floatArr.length, rootRank, rootThreadId);
            return (T)Float.valueOf(floatArr[0]);
        } else if (operand instanceof IntOperand) {
            int []intArr = new int[1];
            intArr[0] = (Integer) value;
            intArr = broadcastArray(intArr, operand, 0, intArr.length, rootRank, rootThreadId);
            return (T)Integer.valueOf(intArr[0]);
        } else if (operand instanceof LongOperand) {
            long []longArr = new long[1];
            longArr[0] = (Long) value;
            longArr = broadcastArray(longArr, operand, 0, longArr.length, rootRank, rootThreadId);
            return (T)Long.valueOf(longArr[0]);
        } else if (operand instanceof ObjectOperand) {
            T []objectArr = (T[]) Array.newInstance(value.getClass(), 1);
            objectArr[0] = value;
            objectArr = broadcastArray(objectArr, operand, 0, objectArr.length, rootRank, rootThreadId);
            return objectArr[0];
        } else if (operand instanceof StringOperand) {
            String []stringArr = new String[1];
            stringArr[0] = (String) value;
            stringArr = broadcastArray(stringArr, operand, 0, stringArr.length, rootRank, rootThreadId);
            return (T)stringArr[0];
        } else if (operand instanceof ShortOperand) {
            short []shortArr = new short[1];
            shortArr[0] = (Short) value;
            shortArr = broadcastArray(shortArr, operand, 0, shortArr.length, rootRank, rootThreadId);
            return (T)Short.valueOf(shortArr[0]);
        } else if (operand instanceof ByteOperand) {
            byte []byteArr = new byte[1];
            byteArr[0] = (Byte) value;
            byteArr = broadcastArray(byteArr, operand, 0, byteArr.length, rootRank, rootThreadId);
            return (T)Byte.valueOf(byteArr[0]);
        } else {
            throw new Mp4jException("unknown operand:" + operand);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#broadcastMap(Map, Operand, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.broadcastMapProcess
     * </pre></blockquote>
     * Broadcast map in root process to all other processes(included itself)
     *
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return the root map.
     * @throws Mp4jException
     */
    public <T> Map<String, T> broadcastMapProcess(Map<String, T> mapData, Operand operand, int rootRank) throws Mp4jException {
        return processCommSlave.broadcastMap(mapData, operand, rootRank);
    }

    /**
     * Broadcast map in root process and thread to all other processes and threads(included itself),
     * similar with {@link #broadcastArray(Object, Operand, int, int, int, int)}, but this container is map.
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank
     * @param rootThreadId

     * @return the root map.
     *         Attention: all threads in the same process shared the same result(reduce memory use and gc),
     *                    if you want to modify in different threads, you must clone a duplicate ahead of modification.
     * @throws Mp4jException
     */
    public <T> Map<String, T> broadcastMap(Map<String, T> mapData, Operand operand, int rootRank, int rootThreadId) throws Mp4jException {
        //threadBarrier();
        try {
            if (threadNum == 1) {
                return processCommSlave.broadcastMap(mapData, operand, rootRank);
            }

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;

            if (getThreadId() == realRootThreadId) {
                Map<String, T> rootBrcstMap = processCommSlave.broadcastMap(mapData, operand, rootRank);

                MapMetaData<T> rootMapMetaData = new MapMetaData<>();
                rootMapMetaData.setMapDataList(Arrays.asList(rootBrcstMap));
                delegatingMetaData = rootMapMetaData;

                threadBarrier();
                threadBarrier();
                return rootBrcstMap;
            } else {
                threadBarrier();
                MapMetaData<T> thisMapMetaData = new MapMetaData<>();
                operand.threadCopy(delegatingMetaData, thisMapMetaData);
                threadBarrier();

                return thisMapMetaData.getMapDataList().get(0);
            }
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#scatterArray(Object, Operand, int[], int[], int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.scatterArrayProcess
     * </pre></blockquote>
     *  Send chunks of an array to different processes, the data container is array.
     *
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param recvfroms receiving start positions, included
     * @param recvtos receiving end positions, excluded
     * @param rootRank root rank

     * @return rank i receives data from root process, placed in [recvfroms[i], recvtos[i])
     * @throws Mp4jException
     */
    public <T> T scatterArrayProcess(T arrData, Operand operand, int[] recvfroms, int[] recvtos, int rootRank) throws Mp4jException {
        return processCommSlave.scatterArray(arrData, operand, recvfroms, recvtos, rootRank);
    }

    /**
     * Send chunks of an array to different processes and threads, the data container is array.
     * process i thread j receive data interval [recvfroms[i][j], recvtos[i][j]) from root process and thread, placed in the same positions,
     * s.t. recvfroms[i][j] &le; recvtos[i][j], recvfroms[i][j] &ge; recvtos[i][j-1], recvfroms[i][0] &ge; recvtos[i-1][threadnum - 1]
     * @param arrData data array, each process and thread have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param recvfroms receiving start positions, included
     * @param recvtos receiving end positions, excluded
     * @param rootRank root rank
     * @param rootThreadId root threadId

     * @return rank i thread j receives data from root process and thread, placed in [recvfroms[i][j], recvtos[i][j])
     * @throws Mp4jException
     */
    public <T> T scatterArray(T arrData, Operand operand, int[][] recvfroms, int[][] recvtos, int rootRank, int rootThreadId) throws Mp4jException {

        //threadBarrier();
        if (recvfroms.length != slaveNum) {
            throw new Mp4jException("recvfroms's length:" + recvfroms.length + " must be equal slaveNum:" + slaveNum + "!");
        }

        if (recvtos.length != slaveNum) {
            throw new Mp4jException("recvto's length:" + recvtos.length + " must be equal slaveNum:" + slaveNum + "!");
        }

        for (int i = 0; i < slaveNum; i++) {
            CommUtils.isfromsTosLegal(recvfroms, recvtos, threadNum);
        }

        try {

            if (threadNum == 1) {
                int[] processFroms = CommUtils.getProcessFroms(recvfroms);
                int[] processTos = CommUtils.getProcessTos(recvtos);
                return processCommSlave.scatterArray(arrData, operand, processFroms, processTos, rootRank);
            }

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;
            int threadId = getThreadId();
            if (threadId == realRootThreadId) {
                int[] processFroms = CommUtils.getProcessFroms(recvfroms);
                int[] processTos = CommUtils.getProcessTos(recvtos);
                T rootScatteredArr = processCommSlave.scatterArray(arrData, operand, processFroms, processTos, rootRank);

                ArrayMetaData<T> rootArrayMetaData = new ArrayMetaData<>();
                rootArrayMetaData.setArrData(rootScatteredArr);
                delegatingMetaData = rootArrayMetaData;

                threadBarrier();
                threadBarrier();
                return rootScatteredArr;
            } else {
                threadBarrier();

                T rootArr = (T) delegatingMetaData.getArrData();
                ArrayMetaData<T> thisArrayMetaData = new ArrayMetaData<>();
                thisArrayMetaData.setArrData(arrData);
                ArrayMetaData<T> rootArrayMetaData = new ArrayMetaData<>();
                rootArrayMetaData.setArrData(rootArr);
                rootArrayMetaData.insert(0, recvfroms[rank][threadId], recvtos[rank][threadId]);
                operand.threadCopy(rootArrayMetaData, thisArrayMetaData);

                threadBarrier();
                return thisArrayMetaData.getArrData();
            }

        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#scatterMap(List, Operand, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.scatterMapProcess
     * </pre></blockquote>
     * Send chunks of data to different processes, the data container is map.
     *
     * @param mapDataList list of maps to be scattered.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank

     * @return rank i receives data mapDataList.get(i) from root process.
     * @throws Mp4jException
     */
    public <T> Map<String, T> scatterMapProcess(List<Map<String, T>> mapDataList, Operand operand, int rootRank) throws Mp4jException {
        return processCommSlave.scatterMap(mapDataList, operand, rootRank);
    }

    /**
     * Send chunks of data to different processes and threads, the data container is map.
     * process i thread j receive data mapDataListList.get(i).get(j) from root process and thread.
     * @param mapDataListList list of list of maps to be scattered.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param rootRank root rank
     * @param rootThreadId root thread id

     * @return process i thread j receive data mapDataListList.get(i).get(j) from root process and thread.
     * @throws Mp4jException
     */
    public <T> Map<String, T> scatterMap(List<List<Map<String, T>>> mapDataListList, Operand operand, int rootRank, int rootThreadId) throws Mp4jException {
        //threadBarrier();
        if (rank == rootRank && getThreadId() == rootThreadId && mapDataListList.size() != slaveNum) {
            throw new Mp4jException("mapDataListList's size:" + mapDataListList.size() + " must be equal slaveNum:" + slaveNum + "!");
        }

        try {
            if (threadNum == 1) {
                List<Map<String, T>> tempList = new ArrayList<>();
                if (rank == rootRank) {
                    for (List<Map<String, T>> maplist : mapDataListList) {
                        tempList.add(maplist.get(0));
                    }
                }
                return processCommSlave.scatterMap(tempList, operand, rootRank);
            }

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;
            int threadId = getThreadId();
            if (threadId == realRootThreadId) {
                List<Map<String, T>> rootScatteredMapList = processCommSlave.scatterMapSpecial(mapDataListList, operand, rootRank);

                // input threadnum - 1 MetaDatas to queue
                MapMetaData<T> rootMapMetaData = new MapMetaData<>();
                rootMapMetaData.setMapDataList(rootScatteredMapList);
                delegatingMetaData = rootMapMetaData;

                threadBarrier();
                threadBarrier();
                return rootScatteredMapList.get(realRootThreadId);
            } else {
                threadBarrier();
                MapMetaData<T> rootMapMetaData = delegatingMetaData.convertToMapMetaData();
                threadBarrier();
                return rootMapMetaData.getMapDataList().get(getThreadId());
            }
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceScatterArray(Object, Operand, IOperator, int, int[])} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceScatterArrayProcess
     * </pre></blockquote>
     * ReduceScatterArray operation can ve viewed as a combination of reduce and scatter.
     *
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param from start position of arrData to be ReduceScattered
     * @param counts rank i receive number of counts[i] elements reducescatter result.

     * @return rank 0 receive interval is [from, counts[0]),
     *         rank 1 receive interval is [from+counts[0], from+counts[0]+counts[1]), ...
     * @throws Mp4jException
     */
    public <T> T reduceScatterArrayProcess(T arrData, Operand operand, IOperator operator, int from, int[] counts) throws Mp4jException {
        return processCommSlave.reduceScatterArray(arrData, operand, operator, from, counts);
    }

    /**
     * ReduceScatterArray operation can ve viewed as a combination of {@link #reduceScatterArray(Object, Operand, IOperator, int, int[][])}
     * and {@link #scatterArray(Object, Operand, int[][], int[][], int, int)}.
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param from start position of arrData to be ReduceScattered
     * @param counts process i thread j receive number of counts[i][j] elements reducescatter result.

     * @return rank 0 thread 0 receive interval is [from, counts[0][0]),
     *         rank 0 thread j receive interval is [from+counts[0][0]+...+counts[0][j-1], counts[0][j]),
     *         rank i thread j receive interval is [from+counts[0][0]+...+counts[0][threadnum-1]+counts[1][0] + ...+counts[i][j-1] counts counts[i][j]),
     * @throws Mp4jException
     */
    public <T> T reduceScatterArray(T arrData, Operand operand, IOperator operator, int from, int[][] counts) throws Mp4jException {

        //threadBarrier();
        if (counts.length != slaveNum) {
            throw new Mp4jException("counts.length must be equal to slaveNum!");
        }
        try {
            CommUtils.isFromCountsLegal(from, counts);
            if (threadNum == 1) {
                int[] processCount = new int[slaveNum];
                for (int i = 0; i < slaveNum; i++) {
                    processCount[i] = counts[i][0];
                }
                return processCommSlave.reduceScatterArray(arrData, operand, operator, from, processCount);
            }

            // thread reduce
            operand.setCollective(Collective.REDUCE_SCATTER);
            operand.setContainer(Container.ARRAY);
            operand.setOperator(operator);
            setOperand(operand);

            int allto = from;
            int rankFrom = from;
            for (int r = 0; r < slaveNum; r++) {
                for (int i = 0; i < threadNum; i++) {
                    allto += counts[r][i];
                    if (r < rank) {
                        rankFrom += counts[r][i];
                    }
                }
            }
            int[] tfroms = CommUtils.getFromsFromCount(rankFrom, counts[rank], threadNum);
            int[] ttos = CommUtils.getTosFromCount(rankFrom, counts[rank], threadNum);

            int threadId = getThreadId();
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER)
                    .insert(threadId, 0, allto);
            arrayMetaData.setArrData(arrData);

            long start = System.currentTimeMillis();
            ArrayMetaData<T> threadReducedMetaData = (ArrayMetaData<T>) threadExchange(arrayMetaData, 0, 1);
            if (threadId == 0) {
                debug("local thread reduce takes:" + (System.currentTimeMillis() - start));
            }

            // process reduce
            if (threadId == 0) {
                int[] processCounts = new int[slaveNum];
                for (int r = 0; r < slaveNum; r++) {
                    processCounts[r] = 0;
                    for (int t = 0; t < threadNum; t++) {
                        processCounts[r] += counts[r][t];
                    }
                }
                T processReduceScatteredArr = processCommSlave.reduceScatterArray(threadReducedMetaData.getArrData(),
                        operand, operator, from, processCounts);
                ArrayMetaData processMetaData = new ArrayMetaData();
                processMetaData.setArrData(processReduceScatteredArr);
                delegatingMetaData = processMetaData;
            }

            barrier.await();
            T fromArr = (T) delegatingMetaData.getArrData();
            ArrayMetaData<T> fromMetaData = new ArrayMetaData<>();
            fromMetaData.setArrData(fromArr)
                    .insert(0, tfroms[threadId], ttos[threadId]);
            getOperand().threadCopy(fromMetaData, arrayMetaData);
            threadBarrier();

            return arrayMetaData.getArrData();
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceScatterMap(List, Operand, IOperator)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceScatterMapProcess
     * </pre></blockquote>
     * Reducescatter map, can be viewed as combination reduce and scatter.
     *
     * @param mapDataList list of map
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return rank i process will receive reduce of mapDataList.get(i) in all process
     * @throws Mp4jException
     */
    public <T> Map<String, T> reduceScatterMapProcess(List<Map<String, T>> mapDataList, Operand operand, IOperator operator) throws Mp4jException {
        return processCommSlave.reduceScatterMap(mapDataList, operand, operator);
    }

    /**
     * reduceScatterMap operation can be viewed as a combination of {@link #reduceMap(Map, Operand, IOperator, int, int)}
     * and {@link #scatterMap(List, Operand, int, int)}.
     * @param mapDataListList list of list of maps
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return rank i thread j receive result of reduce of mapDataListList.get(i).get(j) in all processes and threads.
     * @throws Mp4jException
     */
    public <T> Map<String, T> reduceScatterMap(List<List<Map<String, T>>> mapDataListList, Operand operand, IOperator operator) throws Mp4jException {

        //threadBarrier();
        if (mapDataListList.size() != slaveNum) {
            throw new Mp4jException("mapDataListList dimension must be equal to slaveNum * threadNum!");
        }

        for (int r = 0; r < slaveNum; r++) {
            if (mapDataListList.get(r).size() != threadNum) {
                throw new Mp4jException("mapDataListList dimension must be equal to slaveNum * threadNum!");
            }
        }
        try {
            if (threadNum == 1) {
                List<Map<String, T>> mapDataList = new ArrayList<>(slaveNum);
                for (int r = 0; r < slaveNum; r++) {
                    mapDataList.add(mapDataListList.get(r).get(0));
                }

                return processCommSlave.reduceScatterMap(mapDataList, operand, operator);
            }

            // thread reduce
            operand.setCollective(Collective.REDUCE_SCATTER);
            operand.setContainer(Container.MAP);
            operand.setOperator(operator);
            setOperand(operand);

            int threadId = getThreadId();
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER);
            List<Map<String, T>> mapDataListLocal = new ArrayList<>(slaveNum * threadNum);
            for (int r = 0; r < slaveNum; r++) {
                List<Map<String, T>> mapList = mapDataListList.get(r);
                for (int t = 0; t < threadNum; t++) {
                    mapDataListLocal.add(mapList.get(t));
                    mapMetaData.insert(threadId, mapList.get(t).size());
                }
            }
            mapMetaData.setMapDataList(mapDataListLocal);

            MapMetaData<T> localReduceMetaData = (MapMetaData<T>) threadExchange(mapMetaData, 0, 1);

            if (threadId == 0) {
                List<Map<String, T>> localMapReduceList = localReduceMetaData.getMapDataList();
                List<List<Map<String, T>>> mapsForProcess = new ArrayList<>(slaveNum * threadNum);
                for (int r = 0; r < slaveNum; r++) {
                    List<Map<String, T>> mapsList = new ArrayList<>(threadNum);
                    mapsForProcess.add(mapsList);
                    for (int t = 0; t < threadNum; t++) {
                        mapsList.add(localMapReduceList.get(r * threadNum + t));
                    }
                }

                List<Map<String, T>> processResultList = processCommSlave.reduceScatterMapSpecial(mapsForProcess, operand, operator);
                mapMetaData.setMapDataList(processResultList);
                delegatingMetaData = mapMetaData;
            }

            barrier.await();
            Map<String, T> retMap = (Map<String, T>) delegatingMetaData.getMapDataList().get(threadId);
            threadBarrier();

            return retMap;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceArray(Object, Operand, IOperator, int, int, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceArrayProcess
     * </pre></blockquote>
     * reduce all array elements, reduced result locate in root process
     *
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
    public <T> T reduceArrayProcess(T arrData, Operand operand, IOperator operator, int from, int to, int rootRank) throws Mp4jException {
        return processCommSlave.reduceArray(arrData, operand, operator, from, to, rootRank);
    }

    /**
     * reduce all array elements, reduced result locate in root process and thread
     * @param arrData data array, each process and thread have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param from start position to reduce
     * @param to end position to reduce
     * @param rootRank root rank
     * @param rootThreadId root thread id

     * @return if this process and thread is root, reduced array is returned, otherwise,
     *         is invalid array, contains intermediate result.
     * @throws Mp4jException
     */
    public <T> T reduceArray(T arrData, Operand operand, IOperator operator, int from, int to, int rootRank, int rootThreadId) throws Mp4jException {

        //threadBarrier();
        CommUtils.isFromToLegal(from, to);

        try {
            if (threadNum == 1) {
                return processCommSlave.reduceArray(arrData, operand, operator, from, to, rootRank);
            }

            // thread reduce
            operand.setCollective(Collective.REDUCE_SCATTER);
            operand.setContainer(Container.ARRAY);
            operand.setOperator(operator);
            setOperand(operand);

            int threadId = getThreadId();
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER)
                    .insert(threadId, from, to);
            arrayMetaData.setArrData(arrData);

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;
            T threadReducedArr = (T) threadExchange(arrayMetaData, realRootThreadId, 1).getArrData();


            if (threadId == realRootThreadId) {
                T reducedArr = processCommSlave.reduceArray(threadReducedArr, operand, operator, from, to, rootRank);
                threadBarrier();
                return reducedArr;
            }

            threadBarrier();
            return arrData;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }

    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduce(Object, Operand, IOperator, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceProcess
     * </pre></blockquote>
     * single value reduce
     *
     * @param value value to be reduced
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param rootRank root rank

     * @return if this process and thread is root, reduced value is returned, otherwise, is a invalid value.
     * @throws Mp4jException
     */
    public <T> T reduceProcess(T value, Operand operand, IOperator operator, int rootRank) throws Mp4jException {
        return processCommSlave.reduce(value, operand, operator, rootRank);
    }

    /**
     * single value reduce
     * @param value value to be reduced
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param rootRank root rank
     * @param rootThreadId root thread id

     * @return if this process and thread is root, reduced value is returned, otherwise, is a invalid value.
     * @throws Mp4jException
     */
    public <T> T reduce(T value, Operand operand, IOperator operator, int rootRank, int rootThreadId) throws Mp4jException {
        if (operand instanceof DoubleOperand) {
            double []doubleArr = new double[1];
            doubleArr[0] = (Double) value;
            doubleArr = reduceArray(doubleArr, operand, operator, 0, doubleArr.length, rootRank, rootThreadId);
            return (T)Double.valueOf(doubleArr[0]);
        } else if (operand instanceof FloatOperand) {
            float []floatArr = new float[1];
            floatArr[0] = (Float) value;
            floatArr = reduceArray(floatArr, operand, operator, 0, floatArr.length, rootRank, rootThreadId);
            return (T)Float.valueOf(floatArr[0]);
        } else if (operand instanceof IntOperand) {
            int []intArr = new int[1];
            intArr[0] = (Integer) value;
            intArr = reduceArray(intArr, operand, operator, 0, intArr.length, rootRank, rootThreadId);
            return (T)Integer.valueOf(intArr[0]);
        } else if (operand instanceof LongOperand) {
            long []longArr = new long[1];
            longArr[0] = (Long) value;
            longArr = reduceArray(longArr, operand, operator, 0, longArr.length, rootRank, rootThreadId);
            return (T)Long.valueOf(longArr[0]);
        } else if (operand instanceof ObjectOperand) {
            T []objectArr = (T[]) Array.newInstance(value.getClass(), 1);
            objectArr[0] = value;
            objectArr = reduceArray(objectArr, operand, operator, 0, objectArr.length, rootRank, rootThreadId);
            return objectArr[0];
        } else if (operand instanceof StringOperand) {
            String []stringArr = new String[1];
            stringArr[0] = (String) value;
            stringArr = reduceArray(stringArr, operand, operator, 0, stringArr.length, rootRank, rootThreadId);
            return (T)stringArr[0];
        } else if (operand instanceof ShortOperand) {
            short []shortArr = new short[1];
            shortArr[0] = (Short) value;
            shortArr = reduceArray(shortArr, operand, operator, 0, shortArr.length, rootRank, rootThreadId);
            return (T)Short.valueOf(shortArr[0]);
        } else if (operand instanceof ByteOperand) {
            byte []byteArr = new byte[1];
            byteArr[0] = (Byte) value;
            byteArr = reduceArray(byteArr, operand, operator, 0, byteArr.length, rootRank, rootThreadId);
            return (T)Byte.valueOf(byteArr[0]);
        } else {
            throw new Mp4jException("unknown operand:" + operand);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceMap(Map, Operand, IOperator, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceMapProcess
     * </pre></blockquote>
     * map reduce.
     *
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param rootRank root rank

     * @return if this process and thread is root, reduced map is returned, otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, T> reduceMapProcess(Map<String, T> mapData, Operand operand, IOperator operator, int rootRank) throws Mp4jException {
        return processCommSlave.reduceMap(mapData, operand, operator, rootRank);
    }

    /**
     * Similar with {@link #reduceArray(Object, Operand, IOperator, int, int, int, int)},
     * but the container is map.
     * @param mapData map data
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param rootRank root rank
     * @param rootThreadId root thread id

     * @return if this process and thread is root, reduced map is returned, otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, T> reduceMap(Map<String, T> mapData, Operand operand, IOperator operator, int rootRank, int rootThreadId) throws Mp4jException {

        //threadBarrier();
        try {
            if (threadNum == 1) {
                return processCommSlave.reduceMap(mapData, operand, operator, rootRank);
            }

            operand.setCollective(Collective.REDUCE_SCATTER);
            operand.setContainer(Container.MAP);
            operand.setOperator(operator);
            setOperand(operand);

            int threadId = getThreadId();
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER);

            mapMetaData.setMapDataList(Arrays.asList(mapData));

            int realRootThreadId = rank == rootRank ? rootThreadId : 0;
            MapMetaData<T> localReduceMetaData = (MapMetaData<T>) threadExchange(mapMetaData, realRootThreadId, 1);
            if (threadId == realRootThreadId) {
                Map<String, T> processReturnMap = processCommSlave.reduceMap(localReduceMetaData.getMapDataList().get(0), operand, operator, rootRank);
                if (rank == rootRank) {
                    threadBarrier();
                    return processReturnMap;
                }
            }

            threadBarrier();
            return null;

        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceMapSetUnion(Map, int, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceMapSetUnionProcess
     * </pre></blockquote>
     * Set union, the set with the same key will be reduced(union) together in the root process.
     *
     * @param mapData map set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, the set with the same key will be reduced together,
     *         otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> reduceMapSetUnionProcess(Map<String, Set<T>> mapData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.reduceMapSetUnion(mapData, rootRank, elementSerializer, elementType);
    }

    /**
     * Set union, the set with the same key will be reduced(union) together in the root process and thread.
     * @param mapData map set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, the set with the same key will be reduced together,
     *         otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> reduceMapSetUnion(Map<String, Set<T>> mapData, int rootRank, int rootThreadId, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {

        Operand operand = Operands.OBJECT_OPERAND(new ProcessCommSlave.Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<Set<T>>() {
            @Override
            public Set<T> apply(Set<T> o1, Set<T> o2) {
                for (T val : o2) {
                    o1.add(val);
                }
                return o1;
            }
        };

        return reduceMap(mapData, operand, operator, rootRank, rootThreadId);

    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceSetUnion(Set, int, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceSetUnionProcess
     * </pre></blockquote>
     *
     * Set union
     *
     * @param setData set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, set unison is returned,
     *         otherwise invalid set or null is returned.
     * @throws Mp4jException
     */
    public <T> Set<T> reduceSetUnionProcess(Set<T> setData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.reduceSetUnion(setData, rootRank, elementSerializer, elementType);
    }

    /**
     * Set union
     * @param setData set data
     * @param rootRank root rank
     * @param rootThreadId root thread id

     * @return if this process and thread is root, set unison is returned,
     *         otherwise invalid set or null is returned.
     * @throws Mp4jException
     */
    public <T> Set<T> reduceSetUnion(Set<T> setData, int rootRank, int rootThreadId, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, Set<T>> mapTemp = new HashMap<>(1);
        mapTemp.put("key", setData);

        Map<String, Set<T>> mapReturn = reduceMapSetUnion(mapTemp, rootRank, rootThreadId, elementSerializer, elementType);
        if (mapReturn != null) {
            return mapReturn.get("key");
        } else {
            return null;
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceMapSetIntersection(Map, int, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceMapSetIntersectionProcess
     * </pre></blockquote>
     * Set intersection, the set with the same key will be reduced(intersect) together.
     *
     * @param mapData map set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, the set with the same key will be reduced(intersect) together.
     *         otherwise, invalid map is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> reduceMapSetIntersectionProcess(Map<String, Set<T>> mapData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.reduceMapSetIntersection(mapData, rootRank, elementSerializer, elementType);
    }

    /**
     * Set intersection, the set with the same key will be reduced(intersect) together.
     * @param mapData map set data
     * @param rootRank root rank
     * @param rootThreadId root thread id
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, the set with the same key will be reduced(intersect) together.
     *         otherwise, invalid map is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> reduceMapSetIntersection(Map<String, Set<T>> mapData, int rootRank, int rootThreadId, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Operand operand = Operands.OBJECT_OPERAND(new ProcessCommSlave.Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<Set<T>>() {
            @Override
            public Set<T> apply(Set<T> o1, Set<T> o2) {
                o1.retainAll(o2);
                return o1;
            }
        };

        return reduceMap(mapData, operand, operator, rootRank, rootThreadId);
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceSetIntersection(Set, int, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceSetIntersectionProcess
     * </pre></blockquote>
     * Set intersection.
     *
     * @param setData set data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process is root, intersection is returned,
     *         otherwise, invalid set or null is returned.
     * @throws Mp4jException
     */
    public <T> Set<T> reduceSetIntersectionProcess(Set<T> setData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.reduceSetIntersection(setData, rootRank, elementSerializer, elementType);
    }

    /**
     * Set intersection.
     * @param setData set data
     * @param rootRank root rank
     * @param rootThreadId root thread id
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, intersection is returned,
     *         otherwise, invalid set or null is returned.
     * @throws Mp4jException
     */
    public <T> Set<T> reduceSetIntersection(Set<T> setData, int rootRank, int rootThreadId, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, Set<T>> mapTemp = new HashMap<>(1);
        mapTemp.put("key", setData);

        Map<String, Set<T>> mapReturn = reduceMapSetIntersection(mapTemp, rootRank, rootThreadId, elementSerializer, elementType);
        if (mapReturn != null) {
            return mapReturn.get("key");
        } else {
            return null;
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceMapListConcat(Map, int, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceMapListConcatProcess
     * </pre></blockquote>
     * List concat, the lists with the same key will be reduced(concat) together.
     *
     * @param mapData map list data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process is root, the lists with the same key will be reduced(concat) together,
     *         otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, List<T>> reduceMapListConcatProcess(Map<String, List<T>> mapData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.reduceMapListConcat(mapData, rootRank, elementSerializer, elementType);
    }

    /**
     *  List concat, the lists with the same key will be reduced(concat) together.
     * @param mapData map list data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, the lists with the same key will be reduced(concat) together,
     *         otherwise, invalid map or null is returned.
     * @throws Mp4jException
     */
    public <T> Map<String, List<T>> reduceMapListConcat(Map<String, List<T>> mapData, int rootRank, int rootThreadId, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Operand operand = Operands.OBJECT_OPERAND(new ProcessCommSlave.Mp4jListSerializer<>(elementSerializer, elementType), elementType);
        IOperator operator = new IObjectOperator<List<T>>() {
            @Override
            public List<T> apply(List<T> o1, List<T> o2) {
                for (T val : o2) {
                    o1.add(val);
                }
                return o1;
            }
        };

        return reduceMap(mapData, operand, operator, rootRank, rootThreadId);
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#reduceListConcat(List, int, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.reduceListConcatProcess
     * </pre></blockquote>
     * List concat.
     *
     * @param listData list data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, the concated list is returned,
     *         otherwise, invalid list or null is returned.
     * @throws Mp4jException
     */
    public <T> List<T> reduceListConcatProcess(List<T> listData, int rootRank, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.reduceListConcat(listData, rootRank, elementSerializer, elementType);
    }

    /**
     * List concat.
     * @param listData list data
     * @param rootRank root rank
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return if this process and thread is root, the concated list is returned,
     *         otherwise, invalid list or null is returned.
     * @throws Mp4jException
     */
    public <T> List<T> reduceListConcat(List<T> listData, int rootRank, int rootThreadId, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        Map<String, List<T>> mapTemp = new HashMap<>(1);
        mapTemp.put("key", listData);

        Map<String, List<T>> mapReturn = reduceMapListConcat(mapTemp, rootRank, rootThreadId, elementSerializer, elementType);
        if (mapReturn != null) {
            return mapReturn.get("key");
        } else {
            return null;
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceArray(Object, Operand, IOperator, int, int)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceArrayProcess
     * </pre></blockquote>
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
    public <T> T allreduceArrayProcess(T arrData, Operand operand, IOperator operator, int from, int to) throws Mp4jException {
        return processCommSlave.allreduceArray(arrData, operand, operator, from, to);
    }

    /**
     * Similar with {@link #allreduceProcess(Object, Operand, IOperator)},
     * but it's realized by rpc communication. It is suited to small data.
     *
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param <T>
     * @return arrData[i](rank) = reduce(arrData[i](rank_0), arrData[i](rank_1), ..., arrData[i](rank_slavenumber-1)),
     * @throws Mp4jException
     */
    public <T> T allreduceArrayRpcProcess(T arrData, Operand operand, IOperator operator) throws Mp4jException {
        return processCommSlave.allreduceArrayRpc(arrData, operand, operator);
    }

    /**
     * Different with reduce operation which only root process and thread contains final reduced result,
     * while all processes and threads receive the same reduced result in allreduce operation.
     *
     * @param arrData data array, each process have the same length.
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})
     * @param from start position to reduce
     * @param to end position to reduce

     * @return arrData[i](rank,threadid) = reduce(arrData[i](rank_0, thread_0), arrData[i](rank_0, thread_threadnum), ..., arrData[i](rank_slavenumber-1, thread_threadnum)),
     * @throws Mp4jException
     */
    public <T> T allreduceArray(T arrData, Operand operand, IOperator operator, int from, int to) throws Mp4jException {

        //threadBarrier();
        CommUtils.isFromToLegal(from, to);

        try {
            if (threadNum == 1) {
                return processCommSlave.allreduceArray(arrData, operand, operator, from, to);
            }

            // thread reduce
            operand.setCollective(Collective.ALL_REDUCE);
            operand.setContainer(Container.ARRAY);
            operand.setOperator(operator);
            setOperand(operand);

            int threadId = getThreadId();
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.ALL_REDUCE)
                    .insert(threadId, from, to);
            arrayMetaData.setArrData(arrData);

            ArrayMetaData<T> threadReducedMetaData = (ArrayMetaData<T>) threadExchange(arrayMetaData, 0, 1);

            if (threadId == 0) {
                T reducedArr = processCommSlave.allreduceArray(threadReducedMetaData.getArrData(), operand, operator, from, to);
                arrayMetaData.setArrData(reducedArr);
                delegatingMetaData = arrayMetaData;
                threadBarrier();
                threadBarrier();
                return reducedArr;
            }
            threadBarrier();
            // copy
            T fromArr = (T) delegatingMetaData.getArrData();
            ArrayMetaData<T> fromMetaData = new ArrayMetaData<>();
            fromMetaData.setArrData(fromArr)
                    .insert(0, from, to);
            getOperand().threadCopy(fromMetaData, arrayMetaData);
            threadBarrier();
            return arrData;
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
     * @return arrData[i](rank,threadid) = reduce(arrData[i](rank_0, thread_0), arrData[i](rank_0, thread_threadnum), ..., arrData[i](rank_slavenumber-1, thread_threadnum)),
     * @throws Mp4jException
     */
    public <T> T allreduceArrayRpc(T arrData, Operand operand, IOperator operator) throws Mp4jException {
        try {
            if (threadNum == 1) {
                return processCommSlave.allreduceArrayRpc(arrData, operand, operator);
            }

            // thread reduce
            operand.setCollective(Collective.ALL_REDUCE);
            operand.setContainer(Container.ARRAY);
            operand.setOperator(operator);
            setOperand(operand);

            int threadId = getThreadId();
            ArrayMetaData<T> arrayMetaData = new ArrayMetaData<>();
            arrayMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.ALL_REDUCE)
                    .insert(threadId, -1, -1);
            arrayMetaData.setArrData(arrData);

            ArrayMetaData<T> threadReducedMetaData = (ArrayMetaData<T>) threadExchange(arrayMetaData, 0, 1);

            if (threadId == 0) {
                T reducedArr = processCommSlave.allreduceArrayRpc(threadReducedMetaData.getArrData(), operand, operator);
                arrayMetaData.setArrData(reducedArr);
                delegatingMetaData = arrayMetaData;
                threadBarrier();
                threadBarrier();
                return reducedArr;
            }
            threadBarrier();
            // copy
            T fromArr = (T) delegatingMetaData.getArrData();
            ArrayMetaData<T> fromMetaData = new ArrayMetaData<>();
            fromMetaData.setArrData(fromArr)
                    .insert(0, -1, -1);
            getOperand().threadCopy(fromMetaData, arrayMetaData);
            threadBarrier();
            return arrData;
        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduce(Object, Operand, IOperator)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceProcess
     * </pre></blockquote>
     * single value allreduce.
     *
     * @param value the value for reduce
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return all processes and threads receive the same reduced value.
     * @throws Mp4jException
     */
    public <T> T allreduceProcess(T value, Operand operand, IOperator operator) throws Mp4jException {
        return processCommSlave.allreduce(value, operand, operator);
    }

    /**
     * Similar with {@link #allreduceArray(Object, Operand, IOperator, int, int)},
     * this function only allreduce just only one elements.
     * @param value the value for reduce
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return all processes and threads receive the same reduced value.
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

    public <T> T allreduceRpcProcess(T value, Operand operand, IOperator operator) throws Mp4jException {
        return processCommSlave.allreduceRpc(value, operand, operator);
    }

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
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceMap(Map, Operand, IOperator)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceMapProcess
     * </pre></blockquote>
     * map allreduce
     *
     * @param mapData map to be reduced
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return key value map, the values which have same key will be reduced together.
     * @throws Mp4jException
     */
    public <T> Map<String, T> allreduceMapProcess(Map<String, T> mapData, Operand operand, IOperator operator) throws Mp4jException {
        return processCommSlave.allreduceMap(mapData, operand, operator);
    }

    /**
     * Similar with {@link #allreduceArray(Object, Operand, IOperator, int, int)},
     * the container of this function is map, the values which have same key will be reduced,
     * different processes and threads can contain the same and different keys.
     * @param mapData map to be reduced
     * @param operand operand(there are 8 operands in {@link com.fenbi.mp4j.operand.Operands})
     * @param operator operator(different operands provide different operator in {@link com.fenbi.mp4j.operator.Operators})

     * @return key value map, the values which have same key will be reduced together.
     *         Attention: all threads in the same process shared the same result(reduce memory use and gc),
     *                    if you want to modify in different threads, you must clone a duplicate ahead of modification.
     * @throws Mp4jException
     */
    public <T> Map<String, T> allreduceMap(Map<String, T> mapData, Operand operand, IOperator operator) throws Mp4jException {

        //threadBarrier();
        try {
            if (threadNum == 1) {
                return processCommSlave.allreduceMap(mapData, operand, operator);
            }

            operand.setCollective(Collective.REDUCE_SCATTER);
            operand.setContainer(Container.MAP);
            operand.setOperator(operator);
            setOperand(operand);

            int threadId = getThreadId();
            MapMetaData<T> mapMetaData = new MapMetaData<>();
            mapMetaData.setSrcRank(threadId)
                    .setDestRank(-1)
                    .setStep(0)
                    .setSum(1)
                    .setCollective(Collective.REDUCE_SCATTER);

            mapMetaData.setMapDataList(Arrays.asList(mapData));

            MapMetaData<T> localReduceMetaData = (MapMetaData<T>) threadExchange(mapMetaData, 0, 1);
            if (threadId == 0) {
                Map<String, T> processReturnMap = processCommSlave.allreduceMap(localReduceMetaData.getMapDataList().get(0), operand, operator);
                mapMetaData.setMapDataList(Arrays.asList(processReturnMap));
                delegatingMetaData = mapMetaData;
                threadBarrier();
                threadBarrier();
                return processReturnMap;
            }

            threadBarrier();
            // copy
            List<Map<String, T>> fromMapList = delegatingMetaData.getMapDataList();
            MapMetaData<T> fromMetaData = new MapMetaData<>();
            fromMetaData.setMapDataList(fromMapList);
            getOperand().threadCopy(fromMetaData, mapMetaData);
            threadBarrier();
            return mapMetaData.getMapDataList().get(0);

        } catch (Exception e) {
            throw new Mp4jException(e);
        }
    }

    /**
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceMapSetUnion(Map, Serializer, Class)} actully.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceMapSetUnionProcess
     * </pre></blockquote>
     * Set union, the set with the same key will be reduced(union) together.
     *
     * @param mapData map set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return the set with the same key will be reduced together.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> allreduceMapSetUnionProcess(Map<String, Set<T>> mapData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.allreduceMapSetUnion(mapData, elementSerializer, elementType);
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

        Operand operand = Operands.OBJECT_OPERAND(new ProcessCommSlave.Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
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
     * This function all {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceSetUnion(Set, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceSetUnionProcess
     * </pre></blockquote>
     * Set union
     *
     * @param setData set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return set union result
     * @throws Mp4jException
     */
    public <T> Set<T> allreduceSetUnionProcess(Set<T> setData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.allreduceSetUnion(setData, elementSerializer, elementType);
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
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceMapSetIntersection(Map, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceMapSetIntersectionProcess
     * </pre></blockquote>
     * Set intersection, the set with the same key will be reduced(intersect) together.
     *
     * @param mapData map set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return the set with the same key will be reduced(intersect) together.
     * @throws Mp4jException
     */
    public <T> Map<String, Set<T>> allreduceMapSetIntersectionProcess(Map<String, Set<T>> mapData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.allreduceMapSetIntersection(mapData, elementSerializer, elementType);
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
        Operand operand = Operands.OBJECT_OPERAND(new ProcessCommSlave.Mp4jSetSerializer<>(elementSerializer, elementType), elementType);
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
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceSetIntersection(Set, Serializer, Class)} acutally.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceSetIntersectionProcess
     * </pre></blockquote>
     * Set intersection
     *
     * @param setData set data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return set intersected result
     * @throws Mp4jException
     */
    public <T> Set<T> allreduceSetIntersectionProcess(Set<T> setData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.allreduceSetIntersection(setData, elementSerializer, elementType);
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
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceMapListConcat(Map, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceMapListConcatProcess
     * </pre></blockquote>
     * List concat, the lists with the same key will be reduced(concat) together.
     *
     * @param mapData map list data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return the lists with the same key will be reduced(concat) together.
     * @throws Mp4jException
     */
    public <T> Map<String, List<T>> allreduceMapListConcatProcess(Map<String, List<T>> mapData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.allreduceMapListConcat(mapData, elementSerializer, elementType);
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
        Operand operand = Operands.OBJECT_OPERAND(new ProcessCommSlave.Mp4jListSerializer<>(elementSerializer, elementType), elementType);
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
     * This function call {@link com.fenbi.mp4j.comm.ProcessCommSlave#allreduceListConcat(List, Serializer, Class)} actually.
     * each process allow only one thread to call this function, just like process communication.
     * <blockquote><pre>
     *     if (getThreadId() == i) threadCommSlave.allreduceListConcatProcess
     * </pre></blockquote>
     * list concat
     *
     * @param listData list data
     * @param elementSerializer element object Kryo serializer
     * @param elementType element object class

     * @return list concated result
     * @throws Mp4jException
     */
    public <T> List<T> allreduceListConcatProcess(List<T> listData, Serializer<T> elementSerializer, Class<T> elementType) throws Mp4jException {
        return processCommSlave.allreduceListConcat(listData, elementSerializer, elementType);
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
}
