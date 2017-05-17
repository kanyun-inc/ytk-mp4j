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

package com.fenbi.mp4j.check.checklong;

import com.fenbi.mp4j.check.ThreadCheck;
import com.fenbi.mp4j.comm.ThreadCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.operator.Operators;

import java.util.*;

/**
 * @author xialong
 */
public class ThreadReduceScatterCheck extends ThreadCheck {


    public ThreadReduceScatterCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
                                    int arrSize, int objSize, int runTime, int threadNum, boolean compress) {
        super(threadCommSlave, serverHostName, serverHostPort,
                arrSize, objSize, runTime, threadNum, compress);
    }

    @Override
    public void check() throws Mp4jException {
        final long[][] arr = new long[threadNum][arrSize];
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

                            // long array
                            info("begin to thread reducescatter long arr...");
                            long []arr = new long[arrSize];
                            int avgnum = arrSize / (slaveNum * threadNum);

                            int from = 0;
                            int [][]counts = new int[slaveNum][threadNum];

                            for (int r = 0; r < slaveNum; r++) {
                                for (int t = 0; t < threadNum; t++) {
                                    counts[r][t] = avgnum;
                                }

                            }
                            counts[slaveNum - 1][threadNum - 1] = arrSize - (slaveNum * threadNum - 1) * avgnum;

                            for (int i = 0 ; i < arrSize; i++) {
                                int r = avgnum == 0 ? slaveNum - 1 : i / avgnum;
                                arr[i] = r;

                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.reduceScatterArray(arr, Operands.LONG_OPERAND(compress), Operators.Long.SUM, from, counts);
                            info("thread reducescatter long arr takes:" + (System.currentTimeMillis() - start));

                            int startidx = (rank * threadNum + tidx) * avgnum;
                            int endidx = startidx + avgnum;
                            if (rank == slaveNum - 1 && tidx == threadNum - 1) {
                                endidx = arrSize;
                            }
                            for (int i = startidx; i < endidx; i++) {
                                int r = avgnum == 0 ? slaveNum - 1 : i / avgnum;
                                if (arr[i] != r * slaveNum * threadNum) {
                                    info("thread reducescatter long array error:" + Arrays.toString(arr), false);
                                    threadCommSlave.close(1);
                                }
                            }
                            info("thread reducescatter long arr success!");
                            if (arrSize < 500) {
                                info("thread reducescatter result:" + Arrays.toString(arr));
                            }

                            // map
                            info("begin to thread reducescatter long map...");
                            List<List<Map<String, Long>>> mapListList = new ArrayList<>(slaveNum);
                            for (int r = 0; r < slaveNum; r++) {
                                List<Map<String, Long>> mapList = new ArrayList<>(threadNum);
                                mapListList.add(mapList);
                                for (int t = 0; t < threadNum; t++) {
                                    int idx = r * threadNum + t;
                                    Map<String, Long> map = new HashMap<>(objSize);
                                    mapList.add(map);
                                    for (int i = idx * objSize; i < (idx + 1) * objSize; i++) {
                                        map.put(i + "", new Long(1));
                                    }
                                }

                            }

                            start = System.currentTimeMillis();
                            Map<String, Long> retMap = threadCommSlave.reduceScatterMap(mapListList, Operands.LONG_OPERAND(compress), Operators.Long.SUM);
                            info("thread reducescatter long map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            if (retMap.size() != objSize) {
                                info("thread reducescatter long map retMap size:" + retMap.size() + ", expected size:" + objSize);
                                success = false;
                            }

                            int idx = rank * threadNum + tidx;
                            for (int i = idx * objSize; i < (idx + 1) * objSize; i++) {
                                Long val = retMap.get(i + "");
                                if (val == null || val.intValue() != slaveNum * threadNum) {
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread reducescatter long map error:" + retMap);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500) {
                                info("thread reducescatter long map:" + retMap);
                            }
                            info("thread reducescatter long map success!");
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
