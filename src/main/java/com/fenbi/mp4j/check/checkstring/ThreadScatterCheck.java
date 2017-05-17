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

package com.fenbi.mp4j.check.checkstring;

import com.fenbi.mp4j.check.ThreadCheck;
import com.fenbi.mp4j.comm.ThreadCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.utils.CommUtils;

import java.util.*;

/**
 * @author xialong
 */
public class ThreadScatterCheck extends ThreadCheck {


    public ThreadScatterCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
                              int arrSize, int objSize, int runTime, int threadNum, boolean compress) {
        super(threadCommSlave, serverHostName, serverHostPort,
                arrSize, objSize, runTime, threadNum, compress);
    }

    @Override
    public void check() throws Mp4jException {
        final String[][] arr = new String[threadNum][arrSize];
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

                            // scatter array
                            info("begin to thread scatter String arr...");
                            int [][]froms = CommUtils.createThreadArrayFroms(arrSize, slaveNum, threadNum);
                            int [][]tos = CommUtils.createThreadArrayTos(arrSize, slaveNum, threadNum);

                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int r = 0; r < slaveNum; r++) {
                                    for (int t = 0; t < threadNum; t++) {
                                        for (int i = froms[r][t]; i < tos[r][t]; i++) {
                                            arr[rootRank][i] = (r * threadNum + t) + "";
                                        }
                                    }
                                }
                            }



                            start = System.currentTimeMillis();
                            threadCommSlave.scatterArray(arr[tidx], Operands.STRING_OPERAND(compress), froms, tos, rootRank, rootThreadId);
                            info("thread scatter String arr takes:" + (System.currentTimeMillis() - start));

                            for (int i = froms[rank][tidx]; i < tos[rank][tidx]; i++) {
                                if (Integer.parseInt(arr[tidx][i]) != rank * threadNum + tidx) {
                                    success = false;
                                }
                            }

                            if (success && arrSize < 500) {
                                info("thread scatter String arr success:" + Arrays.toString(arr[tidx]));
                            }

                            if (!success) {
                                if (arrSize < 500) {
                                    info("thread scatter String arr error:" + Arrays.toString(arr[tidx]), false);
                                }
                                threadCommSlave.close(1);
                            }

                            info("thread scatter String arr success!");

                            // scatter map
                            info("begin to thread scatter String map...");
                            List<List<Map<String, String>>> mapListList = new ArrayList<>();
                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int r = 0; r < slaveNum; r++) {
                                    List<Map<String, String>> mapList = new ArrayList<>();
                                    for (int t = 0; t < threadNum; t++) {
                                        Map<String, String> map = new HashMap<>();
                                        mapList.add(map);
                                        int idx = r * threadNum + t;
                                        for (int i = idx * objSize; i < (idx + 1) * objSize; i++) {
                                            map.put(i + "", new String(i + ""));
                                        }
                                    }
                                    mapListList.add(mapList);
                                }
                            }
                            start = System.currentTimeMillis();
                            Map<String, String> retMap = threadCommSlave.scatterMap(mapListList, Operands.STRING_OPERAND(compress), rootRank, rootThreadId);
                            info("thread scatter String map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            if (retMap.size() != objSize) {
                                success = false;
                            }
                            int idx = rank * threadNum + tidx;
                            for (int i = idx * objSize; i < (idx + 1) * objSize; i++) {
                                String val = retMap.get(i + "");
                                if (val == null || Integer.parseInt(val) != i) {
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread scatter String map error!", false);
                                if (objSize < 500) {
                                    info("thread scatter String map error:" + retMap, false);
                                }
                                threadCommSlave.close(1);
                            }

                            if (success && objSize < 500) {
                                info("thread scatter String map success result:" + retMap);
                            }
                            info("thread scatter String map success!");
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
