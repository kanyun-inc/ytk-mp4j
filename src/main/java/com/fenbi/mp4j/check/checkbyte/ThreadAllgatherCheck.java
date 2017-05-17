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
import java.util.List;
import java.util.Map;

/**
 * @author xialong
 */
public class ThreadAllgatherCheck extends ThreadCheck {


    public ThreadAllgatherCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
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

                            // allgather array
                            info("begin to thread allgather byte arr...");
                            int [][]froms = CommUtils.createThreadArrayFroms(arrSize, slaveNum, threadNum);
                            int [][]tos = CommUtils.createThreadArrayTos(arrSize, slaveNum, threadNum);

                            for (int i = froms[rank][tidx]; i < tos[rank][tidx]; i++) {
                                arr[tidx][i] = (byte)1;
                            }

                            start = System.currentTimeMillis();
                            threadCommSlave.allgatherArray(arr[tidx], Operands.BYTE_OPERAND(compress), froms, tos);
                            info("thread allgather byte arr takes:" + (System.currentTimeMillis() - start));

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
                                info("thread allgather byte arr success:" + Arrays.toString(arr[tidx]));
                            }

                            if (!success) {
                                if (arrSize < 500) {
                                    info("thread allgather byte arr error:" + Arrays.toString(arr[tidx]), false);
                                }
                                threadCommSlave.close(1);
                            }


                            info("thread allgather byte arr success!");


                            // allgather map
                            info("begin to thread allgather byte map...");
                            Map<String, Byte> map = new HashMap<>(objSize);
                            int idx = rank * threadNum + tidx;
                            for (int i = idx * objSize; i < (idx + 1) * objSize; i++) {
                                map.put(i + "", new Byte((byte)1));
                            }
                            start = System.currentTimeMillis();
                            List<Map<String, Byte>> retMapList = threadCommSlave.allgatherMap(map, Operands.BYTE_OPERAND(compress));
                            info("thread allgather byte map takes:" + (System.currentTimeMillis() - start));

                            Map<String, Byte> retMap = new HashMap<>();
                            for (Map<String, Byte> mapx : retMapList) {
                                for (Map.Entry<String, Byte> entry : mapx.entrySet()) {
                                    retMap.put(entry.getKey(), entry.getValue());
                                }
                            }

                            success = true;
                            if (retMap.size() != slaveNum * threadNum * objSize) {
                                info("thread allgather byte map retMap size:" + retMap.size() + ", expected size:" + slaveNum * threadNum * objSize);
                                success = false;
                            }

                            for (int i = 0; i < slaveNum * threadNum * objSize; i++) {
                                Byte val = retMap.get(i + "");
                                if (val.intValue() != 1) {
                                    info("thread allgather byte map key:" + i + "'s value=" + val + ", expected val:" + i);
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread allgather byte map error:" + retMap, false);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500) {
                                info("thread allgather byte map result:" + retMap);
                            }
                            info("thread allgather byte map success!");
                        }

                        // process
                        if (tidx == 0) {
                            for (int rt = 1; rt <= runTime; rt++) {
                                info("run time:" + rt + "...");
                                // byte array
                                info("begin to thread-process allgather byte arr...");
                                byte []arr = new byte[arrSize];
                                int avgnum = arrSize / slaveNum;

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
                                threadCommSlave.allgatherArrayProcess(arr, Operands.BYTE_OPERAND(compress), froms, tos);
                                info("thread-process allgather byte arr takes:" + (System.currentTimeMillis() - start));

                                for (int i = 0; i < arr.length; i++) {
                                    int r = avgnum == 0 ? slaveNum - 1 : Math.min(i / avgnum, slaveNum - 1);
                                    if (arr[i] != r) {
                                        info("thread-process allgather byte array error:" + Arrays.toString(arr), false);
                                        threadCommSlave.close(1);
                                    }
                                }
                                info("thread-process allgather byte arr success!");
                                if (arrSize < 500) {
                                    info("thread-process allgather result:" + Arrays.toString(arr));
                                }

                                // map
                                info("begin to thread-process allgather byte map...");
                                Map<String, Byte> map = new HashMap<>(objSize);
                                for (int i = rank * objSize; i < (rank + 1) * objSize; i++) {
                                    map.put(i + "", new Byte((byte)1));
                                }
                                start = System.currentTimeMillis();
                                List<Map<String, Byte>> retMapList = threadCommSlave.allgatherMapProcess(map, Operands.BYTE_OPERAND(compress));
                                info("thread-process allgather byte map takes:" + (System.currentTimeMillis() - start));

                                success = true;
                                if (retMapList.size() != slaveNum) {
                                    info("thread-process allgather byte map retMapList size:" + retMapList.size() + ", expected size:" + slaveNum);
                                    success = false;
                                }

                                for (int i = 0; i < slaveNum; i++) {
                                    int idxstart = i * objSize;
                                    Map<String, Byte> retMap = retMapList.get(i);
                                    for (int j = 0; j < objSize; j++) {
                                        Byte val = retMap.get((idxstart + j) + "");
                                        if (val.intValue() != 1) {
                                            success = false;
                                        }
                                    }
                                }

                                if (!success) {
                                    info("thread-process allgather byte map error:" + retMapList);
                                    threadCommSlave.close(1);
                                }

                                if (objSize < 500) {
                                    info("thread-process allgather byte map list:" + retMapList.toString());
                                }
                                info("thread-process allgather byte map success!");
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
