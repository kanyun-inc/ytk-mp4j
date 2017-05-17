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

package com.fenbi.mp4j.check.checkdouble;

import com.fenbi.mp4j.check.ThreadCheck;
import com.fenbi.mp4j.comm.ThreadCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.operator.Operators;

import java.util.*;

/**
 * @author xialong
 */
public class ThreadReduceCheck extends ThreadCheck {


    public ThreadReduceCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
                             int arrSize, int objSize, int runTime, int threadNum, boolean compress) {
        super(threadCommSlave, serverHostName, serverHostPort,
                arrSize, objSize, runTime, threadNum, compress);
    }

    @Override
    public void check() throws Mp4jException {
        final double[][] arr = new double[threadNum][arrSize];
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

                            // double array
                            info("begin to thread reduce double arr...");
                            double []arr = new double[arrSize];

                            for (int i = 0; i < arrSize; i++) {
                                arr[i] = 1;
                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.reduceArray(arr, Operands.DOUBLE_OPERAND(compress), Operators.Double.SUM, 0, arrSize, rootRank, rootThreadId);
                            info("thread reduce double arr takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int i = 0; i < arrSize; i++) {
                                    if (arr[i] != slaveNum * threadNum) {
                                        success = false;
                                    }
                                }
                            }

                            if (!success) {
                                info("thread reduce double arr error, result:" + Arrays.toString(arr), false);
                                threadCommSlave.close(1);
                            }

                            info("thread reduce double arr success!");
                            if (arrSize < 500 && rank == rootRank && tidx == rootThreadId) {
                                info("thread reduce result:" + Arrays.toString(arr), false);
                            }

                            // map
                            info("begin to thread reduce double map...");
                            Map<String, Double> map = new HashMap<>(objSize);
                            for (int i = 0; i < objSize; i++) {
                                map.put(i + "", new Double(1));
                            }
                            int idx = rank * threadNum + tidx;
                            map.put(-(idx + 1) + "", new Double(1));

                            start = System.currentTimeMillis();
                            Map<String, Double> retMap = threadCommSlave.reduceMap(map, Operands.DOUBLE_OPERAND(compress), Operators.Double.SUM, rootRank, rootThreadId);
                            info("thread reduce double map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int i = 0; i < objSize; i++) {
                                    Double val = retMap.get(i + "");
                                    if (val == null || val.intValue() != slaveNum * threadNum) {
                                        success = false;
                                    }
                                }

                                for (int r = 0; r < slaveNum * threadNum; r++) {
                                    String key = -(r + 1) + "";
                                    Double val = retMap.get(key);
                                    if (val == null || val.intValue() != 1) {
                                        success = false;
                                    }
                                }

                                if (retMap.size() != objSize + slaveNum * threadNum) {
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread reduce double map error:" + retMap);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500 && rank == rootRank && tidx == rootThreadId) {
                                info("thread reduce double map:" + retMap, false);
                            }
                            info("thread reduce double map success!");

                            // single double
                            double singleDouble = threadCommSlave.reduce(1.0, Operands.DOUBLE_OPERAND(compress), Operators.Double.SUM, rootRank, rootThreadId);
                            if (rank == rootRank && tidx == rootThreadId && ((int)singleDouble) != slaveNum * threadNum) {
                                info("thread reduce single double error:" + singleDouble, false);
                                threadCommSlave.close(1);
                            }
                            info("thread reduce single double success!");
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
