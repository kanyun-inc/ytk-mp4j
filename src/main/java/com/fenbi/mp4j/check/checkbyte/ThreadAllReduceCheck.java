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
                            info("begin to thread allreduce byte arr...");
                            byte []arr = new byte[arrSize];

                            for (int i = 0; i < arrSize; i++) {
                                arr[i] = 1;
                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.allreduceArray(arr, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, 0, arrSize);
                            info("thread allreduce byte arr takes:" + (System.currentTimeMillis() - start));

                            for (int i = 0; i < arrSize; i++) {
                                if (arr[i] != slaveNum * threadNum) {
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread allreduce byte arr error", false);
                                if (arrSize < 500) {
                                    info("thread allreduce result:" + Arrays.toString(arr), false);
                                }
                                threadCommSlave.close(1);
                            }

                            info("thread allreduce byte arr success!");
                            if (arrSize < 500) {
                                info("thread allreduce result:" + Arrays.toString(arr), false);
                            }

                            // map
                            info("begin to allreduce byte map...");
                            Map<String, Byte> map = new HashMap<>(objSize);
                            for (int i = 0; i < objSize; i++) {
                                map.put(i + "", new Byte((byte)1));
                            }
                            int index = rank * threadNum + tidx;
                            map.put(-(index + 1) + "", new Byte((byte)1));



                            start = System.currentTimeMillis();
                            Map<String, Byte> retMap = threadCommSlave.allreduceMap(map, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM);
                            info("thread allreduce byte map takes:" + (System.currentTimeMillis() - start));

                            success = true;
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

                            if (!success) {
                                info("thread allreduce byte map error:" + retMap);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500) {
                                info("thread allreduce byte map:" + retMap);
                            }
                            info("thread allreduce byte map success!");

                            // single byte
                            byte singleByte = threadCommSlave.allreduce((byte)1, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM);
                            if (singleByte != slaveNum * threadNum) {
                                info("thread allreduce single byte error:" + singleByte, false);
                                threadCommSlave.close(1);
                            }
                            info("thread allreduce single byte success!");
                        }

                        if (tidx == 0) {
                            for (int rt = 1; rt <= runTime; rt++) {
                                info("run time:" + rt + "...");

                                // byte array
                                info("begin to thread-process allreduce byte arr...");
                                byte []arr = new byte[arrSize];

                                for (int i = 0; i < arrSize; i++) {
                                    arr[i] = 1;
                                }
                                start = System.currentTimeMillis();
                                threadCommSlave.allreduceArrayProcess(arr, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM, 0, arrSize);
                                info("thread-process allreduce byte arr takes:" + (System.currentTimeMillis() - start));

                                for (int i = 0; i < arrSize; i++) {
                                    if (arr[i] != slaveNum) {
                                        success = false;
                                    }
                                }

                                if (!success) {
                                    info("thread-process allreduce byte arr error", false);
                                    threadCommSlave.close(1);
                                }

                                info("thread-process allreduce byte arr success!");
                                if (arrSize < 500) {
                                    info("thread-process allreduce result:" + Arrays.toString(arr));
                                }

                                // map
                                info("begin to thread-process allreduce byte map...");
                                Map<String, Byte> map = new HashMap<>(objSize);
                                for (int i = 0; i < objSize; i++) {
                                    map.put(i + "", new Byte((byte)1));
                                }
                                map.put(-(rank + 1) + "", new Byte((byte)1));



                                start = System.currentTimeMillis();
                                Map<String, Byte> retMap = threadCommSlave.allreduceMapProcess(map, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM);
                                info("thread-process allreduce byte map takes:" + (System.currentTimeMillis() - start));

                                success = true;
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

                                if (!success) {
                                    info("thread-process allreduce byte map error:" + retMap);
                                    threadCommSlave.close(1);
                                }

                                if (objSize < 500) {
                                    info("thread-process allreduce byte map:" + retMap);
                                }
                                info("thread-process allreduce byte map success!");

                                // single byte
                                byte singleByte = threadCommSlave.allreduceProcess((byte)1, Operands.BYTE_OPERAND(compress), Operators.Byte.SUM);
                                if (singleByte != slaveNum) {
                                    info("thread-process allreduce single byte error:" + singleByte, false);
                                    threadCommSlave.close(1);
                                }
                                info("thread-process allreduce single byte success!");
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
