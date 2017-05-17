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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xialong
 */
public class ThreadBroadcastCheck extends ThreadCheck {


    public ThreadBroadcastCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
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
                            info("begin to thread broadcast byte arr...");
                            byte []arr = new byte[arrSize];

                            for (int i = 0; i < arrSize; i++) {
                                if (rank == rootRank && tidx == rootThreadId) {
                                    arr[i] = 1;
                                } else {
                                    arr[i] = -1;
                                }
                            }


                            start = System.currentTimeMillis();
                            arr = threadCommSlave.broadcastArray(arr, Operands.BYTE_OPERAND(compress), 0, arrSize, rootRank, rootThreadId);
                            info("thread broadcast byte arr takes:" + (System.currentTimeMillis() - start));

                            for (int i = 0; i < arr.length; i++) {
                                if (arr[i] != 1) {
                                    info("thread broadcast byte array error:" + Arrays.toString(arr), false);
                                    threadCommSlave.close(1);
                                }
                            }
                            info("thread broadcast byte arr success!");
                            if (arrSize < 500) {
                                info("thread broadcast result:" + Arrays.toString(arr));
                            }

                            // map
                            info("begin to thread broadcast byte map...");
                            Map<String, Byte> map = new HashMap<>();
                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int i = 0; i < objSize; i++) {
                                    map.put(i + "", new Byte((byte)1));
                                }
                            }

                            start = System.currentTimeMillis();
                            Map<String, Byte> retMap = threadCommSlave.broadcastMap(map, Operands.BYTE_OPERAND(compress), rootRank, rootThreadId);
                            info("thread broadcast byte map takes:" + (System.currentTimeMillis() - start));

                            success = true;

                            for (int i = 0; i < objSize; i++) {
                                String key = i + "";
                                Byte val = retMap.get(key);
                                if (val == null || val.intValue() != 1) {
                                    success = false;
                                }

                            }

                            if (!success) {
                                info("thread broadcast byte map error:" + retMap);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500) {
                                info("thread broadcast byte map list:" + retMap.toString());
                            }
                            info("thread broadcast byte map success!");

                            // single byte
                            byte singleByte = threadCommSlave.broadcast((byte)(rank * threadNum + tidx), Operands.BYTE_OPERAND(compress), rootRank, rootThreadId);
                            if (singleByte != (rootRank * threadNum + rootThreadId)) {
                                info("thread broadcast single byte error:" + singleByte, false);
                                threadCommSlave.close(1);
                            }
                            info("thread broadcast single byte success!");

                        }

                        if (tidx == 0) {
                            for (int rt = 1; rt <= runTime; rt++) {
                                info("run time:" + rt + "...");
                                // byte array
                                info("begin to thread-process broadcast byte arr...");
                                byte []arr = new byte[arrSize];

                                for (int i = 0; i < arrSize; i++) {
                                    if (rank == rootRank) {
                                        arr[i] = 1;
                                    } else {
                                        arr[i] = -1;
                                    }
                                }


                                start = System.currentTimeMillis();
                                arr = threadCommSlave.broadcastArrayProcess(arr, Operands.BYTE_OPERAND(compress), 0, arrSize, rootRank);
                                info("thread-process broadcast byte arr takes:" + (System.currentTimeMillis() - start));

                                for (int i = 0; i < arr.length; i++) {
                                    if (arr[i] != 1) {
                                        info("thread-process broadcast byte array error:" + Arrays.toString(arr), false);
                                        threadCommSlave.close(1);
                                    }
                                }
                                info("thread-process broadcast byte arr success!");
                                if (arrSize < 500) {
                                    info("thread-process broadcast result:" + Arrays.toString(arr));
                                }

                                // map
                                info("begin to thread-process broadcast byte map...");
                                Map<String, Byte> map = new HashMap<>();
                                if (rank == rootRank) {
                                    for (int i = 0; i < objSize; i++) {
                                        map.put(i + "", new Byte((byte)1));
                                    }
                                }

                                start = System.currentTimeMillis();
                                Map<String, Byte> retMap = threadCommSlave.broadcastMapProcess(map, Operands.BYTE_OPERAND(compress), rootRank);
                                info("thread-process broadcast byte map takes:" + (System.currentTimeMillis() - start));

                                success = true;

                                for (int i = 0; i < objSize; i++) {
                                    String key = i + "";
                                    Byte val = retMap.get(key);
                                    if (val == null || val.intValue() != 1) {
                                        success = false;
                                    }

                                }

                                if (!success) {
                                    info("thread-process broadcast byte map error:" + retMap);
                                    threadCommSlave.close(1);
                                }

                                if (objSize < 500) {
                                    info("thread-process broadcast byte map list:" + retMap.toString());
                                }
                                info("thread-process broadcast byte map success!");

                                // single byte
                                byte singleByte = threadCommSlave.broadcastProcess((byte)rank, Operands.BYTE_OPERAND(compress), rootRank);
                                if (singleByte != rootRank) {
                                    info("thread-process broadcast single byte error:" + singleByte, false);
                                    threadCommSlave.close(1);
                                }
                                info("thread-process broadcast single byte success!");
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
