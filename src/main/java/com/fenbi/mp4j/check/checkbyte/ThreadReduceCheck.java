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
import com.fenbi.mp4j.operator.Operators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

                            // byte array
                            info("begin to thread reduce byte arr...");
                            byte []arr = new byte[arrSize];

                            for (int i = 0; i < arrSize; i++) {
                                arr[i] = 1;
                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.reduceArray(arr, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, 0, arrSize, rootRank, rootThreadId);
                            info("thread reduce byte arr takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int i = 0; i < arrSize; i++) {
                                    if (arr[i] != slaveNum * threadNum) {
                                        success = false;
                                    }
                                }
                            }

                            if (!success) {
                                info("thread reduce byte arr error", false);
                                threadCommSlave.close(1);
                            }

                            info("thread reduce byte arr success!");
                            if (arrSize < 500 && rank == rootRank && tidx == rootThreadId) {
                                info("thread reduce result:" + Arrays.toString(arr), false);
                            }

                            // map
                            info("begin to thread reduce byte map...");
                            Map<String, Byte> map = new HashMap<>(objSize);
                            for (int i = 0; i < objSize; i++) {
                                map.put(i + "", new Byte((byte)1));
                            }
                            int idx = rank * threadNum + tidx;
                            map.put(-(idx + 1) + "", new Byte((byte)1));

                            start = System.currentTimeMillis();
                            Map<String, Byte> retMap = threadCommSlave.reduceMap(map, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, rootRank, rootThreadId);
                            info("thread reduce byte map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int i = 0; i < objSize; i++) {
                                    Byte val = retMap.get(i + "");
                                    if (val == null || val.intValue() != slaveNum * threadNum) {
                                        success = false;
                                    }
                                }

                                for (int r = 0; r < slaveNum * threadNum; r++) {
                                    String key = -(r + 1) + "";
                                    Byte val = retMap.get(key);
                                    if (val == null || val.intValue() != 1) {
                                        success = false;
                                    }
                                }

                                if (retMap.size() != objSize + slaveNum * threadNum) {
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread reduce byte map error:" + retMap);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500 && rank == rootRank && tidx == rootThreadId) {
                                info("thread reduce byte map:" + retMap, false);
                            }
                            info("thread reduce byte map success!");

                            // single byte
                            byte singleByte = threadCommSlave.reduce((byte)1, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, rootRank, rootThreadId);
                            if (rank == rootRank && tidx == rootThreadId && singleByte != slaveNum * threadNum) {
                                info("thread reduce single byte error:" + singleByte, false);
                                threadCommSlave.close(1);
                            }
                            info("thread reduce single byte success!");
                        }

                        if (tidx == 0) {
                            for (int rt = 1; rt <= runTime; rt++) {
                                info("run time:" + rt + "...");

                                // byte array
                                info("begin to thread-process reduce byte arr...");
                                byte []arr = new byte[arrSize];

                                for (int i = 0; i < arrSize; i++) {
                                    arr[i] = 1;
                                }
                                start = System.currentTimeMillis();
                                threadCommSlave.reduceArrayProcess(arr, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, 0, arrSize, rootRank);
                                info("thread-process reduce byte arr takes:" + (System.currentTimeMillis() - start));

                                if (rank == rootRank) {
                                    for (int i = 0; i < arrSize; i++) {
                                        if (arr[i] != slaveNum) {
                                            success = false;
                                        }
                                    }
                                }

                                if (!success) {
                                    info("thread-process reduce byte arr error", false);
                                    threadCommSlave.close(1);
                                }

                                info("thread-process reduce byte arr success!");
                                if (arrSize < 500 && rank == rootRank) {
                                    info("thread-process reduce result:" + Arrays.toString(arr), false);
                                }

                                // map
                                info("begin to thread-process reduce byte map...");
                                Map<String, Byte> map = new HashMap<>(objSize);
                                for (int i = 0; i < objSize; i++) {
                                    map.put(i + "", new Byte((byte)1));
                                }
                                map.put(-(rank + 1) + "", new Byte((byte)1));

                                start = System.currentTimeMillis();
                                Map<String, Byte> retMap = threadCommSlave.reduceMapProcess(map, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, rootRank);
                                info("thread-process reduce byte map takes:" + (System.currentTimeMillis() - start));

                                success = true;
                                if (rank == rootRank) {
                                    for (int i = 0; i < objSize; i++) {
                                        Byte val = retMap.get(i + "");
                                        if (val == null || val.intValue() != slaveNum) {
                                            success = false;
                                        }
                                    }

                                    for (int r = 0; r < slaveNum; r++) {
                                        String key = -(r + 1) + "";
                                        Byte val = retMap.get(key);
                                        if (val == null || val.intValue() != 1) {
                                            success = false;
                                        }
                                    }

                                    if (retMap.size() != objSize + slaveNum) {
                                        success = false;
                                    }
                                }

                                if (!success) {
                                    info("thread-process reduce byte map error:" + retMap);
                                    threadCommSlave.close(1);
                                }

                                if (objSize < 500 && rank == rootRank) {
                                    info("thread-process reduce byte map:" + retMap, false);
                                }
                                info("thread-process reduce byte map success!");

                                byte singleByte = threadCommSlave.reduceProcess((byte)1, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, rootRank);
                                if (rank == rootRank && singleByte != slaveNum) {
                                    info("thread-process reduce single byte error:" + singleByte, false);
                                    threadCommSlave.close(1);
                                }
                                info("thread-process reduce single byte success!");
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
