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

package com.fenbi.mp4j.check.checkbyte;

import com.fenbi.mp4j.check.ThreadCheck;
import com.fenbi.mp4j.comm.ThreadCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.utils.CommUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xialong
 */
public class ThreadGatherCheck extends ThreadCheck {


    public ThreadGatherCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
                             int arrSize, int objSize, int runTime, int threadNum, boolean compress) {
        super(threadCommSlave, serverHostName, serverHostPort,
                arrSize, objSize, runTime, threadNum, compress);
    }

    @Override
    public void check() throws Mp4jException {
        final byte[][] arr = new byte[threadNum][arrSize];
        int slaveNum = threadCommSlave.getSlaveNum();
        int rank = threadCommSlave.getRank();
        int rootRank = 0;
        int rootThreadId = 0;

        Thread[] threads = new Thread[threadNum];
        for (int t = 0; t < threadNum; t++) {
            final int tidx = t;
            threads[t] = new Thread() {
                @Override
                public void run() {
                    try {
                        // set thread id
                        threadCommSlave.setThreadId(tidx);
                        boolean success = true;
                        long start;

                        for (int rt = 1; rt <= runTime; rt++) {
                            info("run time:" + rt + "...");

                            // gather array
                            info("begin to thread gather byte arr...");
                            int [][]froms = CommUtils.createThreadArrayFroms(arrSize, slaveNum, threadNum);
                            int [][]tos = CommUtils.createThreadArrayTos(arrSize, slaveNum, threadNum);

                            for (int i = froms[rank][tidx]; i < tos[rank][tidx]; i++) {
                                arr[tidx][i] = (byte)1;
                            }

                            start = System.currentTimeMillis();
                            threadCommSlave.gatherArray(arr[tidx], Operands.BYTE_OPERAND(compress), froms, tos, rootRank, rootThreadId);
                            info("thread gather byte arr takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int r = 0; r < slaveNum; r++) {
                                    for (int t = 0; t < threadNum; t++) {
                                        int from = froms[r][t];
                                        int to = tos[r][t];
                                        for (int j = from; j < to; j++) {
                                            if (arr[tidx][j] != 1) {
                                                success = false;
                                            }
                                        }
                                    }
                                }

                                if (success && arrSize < 500) {
                                    info("thread gather byte arr success:" + Arrays.toString(arr[tidx]));
                                }
                            }

                            if (!success) {
                                if (arrSize < 500) {
                                    info("thread gather byte arr error:" + Arrays.toString(arr[tidx]), false);
                                }
                                threadCommSlave.close(1);
                            }

                            info("thread gather byte arr success!");

                            // gather map
                            info("begin to thread gather byte map...");
                            Map<String, Byte> map = new HashMap<>(objSize);
                            int idx = rank * threadNum + tidx;
                            for (int i = idx * objSize; i < (idx + 1) * objSize; i++) {
                                map.put(i + "", new Byte((byte)1));
                            }
                            start = System.currentTimeMillis();
                            Map<String, Byte> retMap = threadCommSlave.gatherMap(map, Operands.BYTE_OPERAND(compress), rootRank, rootThreadId);
                            info("thread gather byte map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            if (rank == rootRank && tidx == rootThreadId) {
                                if (retMap.size() != slaveNum * threadNum * objSize) {
                                    info("thread gather byte map retMap size:" + retMap.size() + ", expected size:" + slaveNum * threadNum * objSize);
                                    success = false;
                                }

                                for (int i = 0; i < slaveNum * threadNum * objSize; i++) {
                                    Byte val = retMap.get(i + "");
                                    if (val.intValue() != 1) {
                                        info("thread gather byte map key:" + i + "'s value=" + val + ", expected val:" + i);
                                        success = false;
                                    }
                                }

                                if (!success) {
                                    info("thread gather byte map error:" + retMap, false);
                                    threadCommSlave.close(1);
                                }
                            }
                            if (objSize < 500) {
                                info("thread gather byte map result:" + retMap);
                            }
                            info("thread gather byte map success!");
                        }

                        if (tidx == 0) {
                            for (int rt = 1; rt <= runTime; rt++) {
                                info("run time:" + rt + "...");

                                // byte array
                                info("begin to thread-process gather byte arr...");
                                byte []arr = new byte[arrSize];
                                int avgnum = arrSize / slaveNum;

                                int rootRank = 0;
                                int []froms = new int[slaveNum];
                                int []tos = new int[slaveNum];

                                for (int r = 0; r < slaveNum; r++) {
                                    froms[r] = r * avgnum;
                                    tos[r] = (r + 1) * avgnum;

                                    if (r == slaveNum - 1) {
                                        tos[r] = arrSize;
                                    }
                                }

                                for (int i = froms[rank]; i < tos[rank]; i++) {
                                    arr[i] = (byte)rank;
                                }
                                start = System.currentTimeMillis();
                                threadCommSlave.gatherArrayProcess(arr, Operands.BYTE_OPERAND(compress), froms, tos, rootRank);
                                info("thread-process gather byte arr takes:" + (System.currentTimeMillis() - start));

                                if (rank == rootRank) {
                                    for (int i = 0; i < arr.length; i++) {
                                        int r = avgnum == 0 ? slaveNum - 1 : Math.min(i / avgnum, slaveNum - 1);
                                        if (arr[i] != r) {
                                            info("thread-process gather byte array error:" + Arrays.toString(arr), false);
                                            threadCommSlave.close(1);
                                        }
                                    }
                                }
                                info("thread-process gather byte arr success!");
                                if (rank == rootRank && arrSize < 500) {
                                    threadCommSlave.info("thread-process gather result:" + Arrays.toString(arr), false);
                                    LOG.info("thread-process gather result:" + Arrays.toString(arr));
                                }

                                // map
                                info("begin to thread-process gather byte map...");
                                Map<String, Byte> map = new HashMap<>(objSize);
                                for (int i = rank * objSize; i < (rank + 1) * objSize; i++) {
                                    map.put(i + "", new Byte((byte)1));
                                }
                                start = System.currentTimeMillis();
                                Map<String, Byte> retMap = threadCommSlave.gatherMapProcess(map, Operands.BYTE_OPERAND(compress), rootRank);
                                info("thread-process gather byte map takes:" + (System.currentTimeMillis() - start));

                                if (rank == rootRank) {
                                    success = true;
                                    if (retMap.size() != slaveNum * objSize) {
                                        info("thread-process gather byte map retMap size:" + retMap.size() + ", expected size:" + slaveNum * objSize);
                                        success = false;
                                    }

                                    for (int i = 0; i < slaveNum * objSize; i++) {
                                        Byte val = retMap.get(i + "");
                                        if (val.intValue() != 1) {
                                            info("thread-process gather byte map key:" + i + "'s value=" + val + ", expected val:" + i);
                                            success = false;
                                        }
                                    }

                                    if (!success) {
                                        info("thread-process gather byte map error:" + retMap);
                                        threadCommSlave.close(1);
                                    }
                                }
                                if (objSize < 500) {
                                    info("thread-process gather byte map result:" + retMap);
                                }
                                info("thread-process gather byte map success!");

                            }
                        }

                        threadCommSlave.threadBarrier();


                    } catch (Exception e) {
                        try {
                            threadCommSlave.exception(e);
                        } catch (Mp4jException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            };
            threads[t].start();
        }

        for (int t = 0; t < threadNum; t++) {
            try {
                threads[t].join();
            } catch (InterruptedException e) {
                throw new Mp4jException(e);
            }
        }
    }
}
