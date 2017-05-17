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

import com.fenbi.mp4j.check.ProcessCheck;
import com.fenbi.mp4j.comm.ProcessCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xialong
 */
public class ProcessGatherCheck extends ProcessCheck {

    public ProcessGatherCheck(ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress) {
        super(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress);
    }

    @Override
    public void check() throws Mp4jException {
        int rank = slave.getRank();
        int slaveNum = slave.getSlaveNum();
        long start;
        long []arr = new long[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");

            // long array
            info("begin to gather long arr...");
            int avgnum = arrSize / slaveNum;

            int rootRank = 0;
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
                arr[i] = rank;
            }
            start = System.currentTimeMillis();
            slave.gatherArray(arr, Operands.LONG_OPERAND(compress), froms, tos, rootRank);
            info("gather long arr takes:" + (System.currentTimeMillis() - start));

            if (rank == rootRank) {
                for (int i = 0; i < arr.length; i++) {
                    int r = avgnum == 0 ? slaveNum - 1 : Math.min(i / avgnum, slaveNum - 1);
                    if (arr[i] != r) {
                        info("gather long array error:" + Arrays.toString(arr), false);
                        slave.close(1);
                    }
                }
            }
            info("gather long arr success!");
            if (rank == rootRank && arrSize < 500) {
                slave.info("gather result:" + Arrays.toString(arr), false);
                LOG.info("gather result:" + Arrays.toString(arr));
            }

//        // list
//        info("begin to gather long list...");
//        List<Long> list = new ArrayList<>(arrSize);
//        for (int i = 0; i < arrSize; i++) {
//            list.add(new Long(rank));
//        }
//
//        List<List<Long>> retListList = slave.gatherList(list, Operands.LONG_OPERAND(compress), rootRank);
//
//        if (rank == rootRank) {
//            if (retListList.size() != slaveNum) {
//                info("gather long list error, ret list list size:" + retListList.size() + " != slave num:" + slaveNum);
//                slave.close(1);
//            }
//            Map<Long, Integer> cntMap = new HashMap<>();
//            for (List<Long> retList : retListList) {
//                for (Long val : retList) {
//                    Integer cnt = cntMap.get(val);
//                    if (cnt == null) {
//                        cntMap.put(val, 1);
//                    } else {
//                        cntMap.put(val, cnt + 1);
//                    }
//                }
//            }
//
//            if (cntMap.size() != slaveNum) {
//                info("gather long list error: cntmap size:" + cntMap.size());
//                slave.close(1);
//            }
//            for (Map.Entry<Long, Integer> entry : cntMap.entrySet()) {
//                if (entry.getValue() != arrSize) {
//                    info("gather long list error: cnt map entry:" + entry);
//                    slave.close(1);
//                }
//            }
//        }
//        info("gather long list success!");
//
//        // set
//        info("begin to gather long set...");
//        Set<Long> set = new HashSet<>(2 * arrSize);
//        for (int i = rank * arrSize; i < (rank + 2) * arrSize; i++) {
//            set.add(new Long(i));
//        }
//        Set<Long> retSet = slave.gatherSet(set, Operands.LONG_OPERAND(compress), rootRank);
//        if (rank == rootRank) {
//            boolean success = true;
//            if (retSet.size() != (slaveNum + 1) * arrSize) {
//                info("gather long set retSet size:" + retSet.size() + ", expected size:" + (slaveNum + 1) * arrSize);
//                success = false;
//            }
//            for (int i = 0; i < (slaveNum + 1) * arrSize; i++) {
//                if (!retSet.contains(new Long(i))) {
//                    info("gather long set retSet not contain:" + i);
//                    success = false;
//                }
//            }
//
//            if (!success) {
//                info("gather long set error:" + retSet);
//                slave.close(1);
//            }
//         }
//        info("gather long set success!");

            // map
            info("begin to gather long map...");
            Map<String, Long> map = new HashMap<>(objSize);
            for (int i = rank * objSize; i < (rank + 1) * objSize; i++) {
                map.put(i + "", new Long(i));
            }
            start = System.currentTimeMillis();
            Map<String, Long> retMap = slave.gatherMap(map, Operands.LONG_OPERAND(compress), rootRank);
            info("gather long map takes:" + (System.currentTimeMillis() - start));

            if (rank == rootRank) {
                boolean success = true;
                if (retMap.size() != slaveNum * objSize) {
                    info("gather long map retMap size:" + retMap.size() + ", expected size:" + slaveNum * objSize);
                    success = false;
                }

                for (int i = 0; i < slaveNum * objSize; i++) {
                    Long val = retMap.get(i + "");
                    if (val.intValue() != i) {
                        info("gather long map key:" + i + "'s value=" + val + ", expected val:" + i);
                        success = false;
                    }
                }

                if (!success) {
                    info("gather long map error:" + retMap);
                    slave.close(1);
                }
            }
            if (objSize < 500) {
                info("gather long map result:" + retMap);
            }
            info("gather long map success!");

        }

    }
}
