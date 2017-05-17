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

package com.fenbi.mp4j.check.checkint;

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
        final int[][] arr = new int[threadNum][arrSize];
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
                            info("begin to thread gather int arr...");
                            int [][]froms = CommUtils.createThreadArrayFroms(arrSize, slaveNum, threadNum);
                            int [][]tos = CommUtils.createThreadArrayTos(arrSize, slaveNum, threadNum);

                            for (int i = froms[rank][tidx]; i < tos[rank][tidx]; i++) {
                                arr[tidx][i] = rank * threadNum + tidx;
                            }

                            start = System.currentTimeMillis();
                            threadCommSlave.gatherArray(arr[tidx], Operands.INT_OPERAND(compress), froms, tos, rootRank, rootThreadId);
                            info("thread gather int arr takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int r = 0; r < slaveNum; r++) {
                                    for (int t = 0; t < threadNum; t++) {
                                        int from = froms[r][t];
                                        int to = tos[r][t];
                                        for (int j = from; j < to; j++) {
                                            if (arr[tidx][j] != (r * threadNum + t)) {
                                                success = false;
                                            }
                                        }
                                    }
                                }

                                if (success && arrSize < 500) {
                                    info("thread gather int arr success:" + Arrays.toString(arr[tidx]));
                                }
                            }

                            if (!success) {
                                if (arrSize < 500) {
                                    info("thread gather int arr error:" + Arrays.toString(arr[tidx]), false);
                                }
                                threadCommSlave.close(1);
                            }

                            info("thread gather int arr success!");

                            // gather map
                            info("begin to thread gather int map...");
                            Map<String, Integer> map = new HashMap<>(objSize);
                            int idx = rank * threadNum + tidx;
                            for (int i = idx * objSize; i < (idx + 1) * objSize; i++) {
                                map.put(i + "", new Integer(i));
                            }
                            start = System.currentTimeMillis();
                            Map<String, Integer> retMap = threadCommSlave.gatherMap(map, Operands.INT_OPERAND(compress), rootRank, rootThreadId);
                            info("thread gather int map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            if (rank == rootRank && tidx == rootThreadId) {
                                if (retMap.size() != slaveNum * threadNum * objSize) {
                                    info("thread gather int map retMap size:" + retMap.size() + ", expected size:" + slaveNum * threadNum * objSize);
                                    success = false;
                                }

                                for (int i = 0; i < slaveNum * threadNum * objSize; i++) {
                                    Integer val = retMap.get(i + "");
                                    if (val.intValue() != i) {
                                        info("thread gather int map key:" + i + "'s value=" + val + ", expected val:" + i);
                                        success = false;
                                    }
                                }

                                if (!success) {
                                    info("thread gather int map error:" + retMap, false);
                                    threadCommSlave.close(1);
                                }
                            }
                            if (objSize < 500) {
                                info("thread gather int map result:" + retMap);
                            }
                            info("thread gather int map success!");
                        }


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
