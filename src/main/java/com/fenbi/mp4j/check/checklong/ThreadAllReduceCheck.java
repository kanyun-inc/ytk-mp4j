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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xialong
 */
public class ThreadAllReduceCheck extends ThreadCheck {


    public ThreadAllReduceCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
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
                            info("begin to thread allreduce long arr...");
                            long []arr = new long[arrSize];

                            for (int i = 0; i < arrSize; i++) {
                                arr[i] = 1;
                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.allreduceArray(arr, Operands.LONG_OPERAND(compress), Operators.Long.SUM, 0, arrSize);
                            info("thread allreduce long arr takes:" + (System.currentTimeMillis() - start));

                            for (int i = 0; i < arrSize; i++) {
                                if (arr[i] != slaveNum * threadNum) {
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread allreduce long arr error", false);
                                if (arrSize < 500) {
                                    info("thread allreduce result:" + Arrays.toString(arr), false);
                                }
                                threadCommSlave.close(1);
                            }

                            info("thread allreduce long arr success!");
                            if (arrSize < 500) {
                                info("thread allreduce result:" + Arrays.toString(arr), false);
                            }

                            // map
                            info("begin to allreduce long map...");
                            Map<String, Long> map = new HashMap<>(objSize);
                            for (int i = 0; i < objSize; i++) {
                                map.put(i + "", new Long(1));
                            }
                            int index = rank * threadNum + tidx;
                            map.put(-(index + 1) + "", new Long(1));



                            start = System.currentTimeMillis();
                            Map<String, Long> retMap = threadCommSlave.allreduceMap(map, Operands.LONG_OPERAND(compress), Operators.Long.SUM);
                            info("thread allreduce long map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            for (int i = 0; i < objSize; i++) {
                                Long val = retMap.get(i + "");
                                if (val == null || val.intValue() != slaveNum * threadNum) {
                                    success = false;
                                }
                            }

                            for (int r = 0; r < slaveNum * threadNum; r++) {
                                String key = -(r + 1) + "";
                                Long val = retMap.get(key);
                                if (val == null || val.intValue() != 1) {
                                    success = false;
                                }
                            }

                            if (retMap.size() != objSize + slaveNum * threadNum) {
                                success = false;
                            }

                            if (!success) {
                                info("thread allreduce long map error:" + retMap);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500) {
                                info("thread allreduce long map:" + retMap);
                            }
                            info("thread allreduce long map success!");

                            // single long
                            long singleLong = threadCommSlave.allreduce(1L, Operands.LONG_OPERAND(compress), Operators.Long.SUM);
                            if (((int)singleLong) != slaveNum * threadNum) {
                                info("thread allreduce single long error:" + singleLong, false);
                                threadCommSlave.close(1);
                            }
                            info("thread allreduce single long success!");
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
