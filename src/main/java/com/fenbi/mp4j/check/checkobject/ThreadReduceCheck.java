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

package com.fenbi.mp4j.check.checkobject;

import com.fenbi.mp4j.check.ThreadCheck;
import com.fenbi.mp4j.comm.ThreadCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.operator.IObjectOperator;
import com.fenbi.mp4j.utils.KryoUtils;

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
        final ObjectNode[][] arr = new ObjectNode[threadNum][arrSize];
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

                            // ObjectNode array
                            info("begin to thread reduce ObjectNode arr...");
                            ObjectNode []arr = new ObjectNode[arrSize];

                            for (int i = 0; i < arrSize; i++) {
                                arr[i] = new ObjectNode(1);
                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.reduceArray(arr, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), new IObjectOperator<ObjectNode>() {
                                @Override
                                public ObjectNode apply(ObjectNode d1, ObjectNode d2) {
                                    d1.val += d2.val;
                                    return d1;
                                }
                            }, 0, arrSize, rootRank, rootThreadId);
                            info("thread reduce ObjectNode arr takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int i = 0; i < arrSize; i++) {
                                    if (arr[i].val != slaveNum * threadNum) {
                                        success = false;
                                    }
                                }
                            }

                            if (!success) {
                                info("thread reduce ObjectNode arr error", false);
                                threadCommSlave.close(1);
                            }

                            info("thread reduce ObjectNode arr success!");
                            if (arrSize < 500 && rank == rootRank && tidx == rootThreadId) {
                                info("thread reduce result:" + Arrays.toString(arr), false);
                            }

                            // map
                            info("begin to thread reduce ObjectNode map...");
                            Map<String, ObjectNode> map = new HashMap<>(objSize);
                            for (int i = 0; i < objSize; i++) {
                                map.put(i + "", new ObjectNode(1));
                            }
                            int idx = rank * threadNum + tidx;
                            map.put(-(idx + 1) + "", new ObjectNode(1));

                            start = System.currentTimeMillis();
                            Map<String, ObjectNode> retMap = threadCommSlave.reduceMap(map, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), new IObjectOperator<ObjectNode>() {
                                @Override
                                public ObjectNode apply(ObjectNode d1, ObjectNode d2) {
                                    d1.val += d2.val;
                                    return d1;
                                }
                            }, rootRank, rootThreadId);
                            info("thread reduce ObjectNode map takes:" + (System.currentTimeMillis() - start));

                            success = true;
                            if (rank == rootRank && tidx == rootThreadId) {
                                for (int i = 0; i < objSize; i++) {
                                    ObjectNode val = retMap.get(i + "");
                                    if (val == null || val.val != slaveNum * threadNum) {
                                        success = false;
                                    }
                                }

                                for (int r = 0; r < slaveNum * threadNum; r++) {
                                    String key = -(r + 1) + "";
                                    ObjectNode val = retMap.get(key);
                                    if (val == null || val.val != 1) {
                                        success = false;
                                    }
                                }

                                if (retMap.size() != objSize + slaveNum * threadNum) {
                                    success = false;
                                }
                            }

                            if (!success) {
                                info("thread reduce ObjectNode map error:" + retMap);
                                threadCommSlave.close(1);
                            }

                            if (objSize < 500 && rank == rootRank && tidx == rootThreadId) {
                                info("thread reduce ObjectNode map:" + retMap, false);
                            }
                            info("thread reduce ObjectNode map success!");

                            // reduce map set union
                            Set<Integer> set = new HashSet<>();
                            for (int i = 0; i < objSize; i++) {
                                set.add(i);
                            }
                            idx = rank * threadNum + tidx;
                            set.add(-(idx + 1));
                            start = System.currentTimeMillis();
                            Set<Integer> reducedMapSetU = threadCommSlave.reduceSetUnion(set, rootRank, rootThreadId, KryoUtils.getDefaultSerializer(Integer.class), Integer.class);
                            info("thread reduce map set union takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                if (reducedMapSetU.size() != slaveNum * threadNum + objSize) {
                                    success = false;
                                }

                                for (int i = 0; i < objSize; i++) {
                                    if (!reducedMapSetU.contains(i)) {
                                        success = false;
                                    }
                                }

                                for (int r = 0; r < slaveNum * threadNum; r++) {
                                    if (!reducedMapSetU.contains(-(r + 1))) {
                                        success = false;
                                    }
                                }

                                if (!success) {
                                    info("thread reduce map set union error:" + reducedMapSetU, false);
                                    threadCommSlave.close(1);
                                }

                                if (success) {
                                    info("thread reduce map set union success!");
                                }

                            }

                            // reduce map set intersection
                            set = new HashSet<>();
                            for (int i = 0; i < objSize; i++) {
                                set.add(i);
                            }
                            set.add(-(idx + 1));
                            start = System.currentTimeMillis();
                            Set<Integer> reducedMapSetI = threadCommSlave.reduceSetIntersection(set, rootRank, rootThreadId, KryoUtils.getDefaultSerializer(Integer.class), Integer.class);

                            info("thread reduce map set intersection takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                if (slaveNum * threadNum > 1) {
                                    if (reducedMapSetI.size() != objSize) {
                                        success = false;
                                    }

                                    for (int i = 0; i < objSize; i++) {
                                        if (!reducedMapSetI.contains(i)) {
                                            success = false;
                                        }
                                    }
                                } else {
                                    if (reducedMapSetI.size() != objSize + slaveNum * threadNum) {
                                        success = false;
                                    }

                                    for (int i = 0; i < objSize; i++) {
                                        if (!reducedMapSetI.contains(i)) {
                                            success = false;
                                        }
                                    }

                                    for (int r = 0; r < slaveNum * threadNum; r++) {
                                        if (!reducedMapSetI.contains(r)) {
                                            success = false;
                                        }
                                    }
                                }


                                if (!success) {
                                    info("thread reduce map set intersection error:" + reducedMapSetI, false);
                                    threadCommSlave.close(1);
                                }

                                if (success) {
                                    info("thread reduce map set intersection success!");
                                }

                            }

                            // reduce map list concat
                            List<Integer> list = new ArrayList<>();
                            list.add(idx);

                            start = System.currentTimeMillis();
                            List<Integer> reducedMapList = threadCommSlave.reduceListConcat(list, rootRank, rootThreadId, KryoUtils.getDefaultSerializer(Integer.class), Integer.class);
                            info("thread reduce map list concat takes:" + (System.currentTimeMillis() - start));

                            if (rank == rootRank && tidx == rootThreadId) {
                                if (reducedMapList.size() != slaveNum * threadNum) {
                                    info("thread reduce map list concat error:" + reducedMapList, false);
                                    threadCommSlave.close(1);
                                    success = false;
                                }
                                for (int r = 0; r < slaveNum * threadNum; r++) {
                                    success = false;
                                    for (int val : reducedMapList) {
                                        if (r == val) {
                                            success = true;
                                            break;
                                        }
                                    }

                                    if (!success) {
                                        break;
                                    }
                                }

                                if (!success) {
                                    info("thread reduce map list concat error:" + reducedMapList, false);
                                    threadCommSlave.close(1);
                                }

                                if (success) {
                                    info("thread reduce map list concat success!");
                                }

                            }

                            // single ObjectNode
                            ObjectNode singleObject = threadCommSlave.reduce(new ObjectNode(1), Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), new IObjectOperator<ObjectNode>() {
                                @Override
                                public ObjectNode apply(ObjectNode d1, ObjectNode d2) {
                                    d1.val += d2.val;
                                    return d1;
                                }
                            }, rootRank, rootThreadId);
                            if (rank == rootRank && tidx == rootThreadId && singleObject.val != slaveNum * threadNum) {
                                info("thread reduce single ObjectNode error:" + singleObject, false);
                                threadCommSlave.close(1);
                            }
                            info("thread reduce single ObjectNode success!");
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
