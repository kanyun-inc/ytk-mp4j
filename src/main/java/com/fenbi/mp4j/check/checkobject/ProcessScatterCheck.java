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

import com.fenbi.mp4j.check.ProcessCheck;
import com.fenbi.mp4j.comm.ProcessCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;

import java.util.*;

/**
 * @author xialong
 */
public class ProcessScatterCheck extends ProcessCheck {

    public ProcessScatterCheck(ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress) {
        super(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress);
    }

    @Override
    public void check() throws Mp4jException {
        int rank = slave.getRank();
        int slaveNum = slave.getSlaveNum();
        boolean success = true;
        long start;
        ObjectNode[] arr = new ObjectNode[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");

            // ObjectNode array
            info("begin to scatter ObjectNode arr...");
            int avgnum = arrSize / slaveNum;

            int rootRank = 0;
            int[] recvfroms = new int[slaveNum];
            int[] recvtos = new int[slaveNum];

            for (int r = 0; r < slaveNum; r++) {
                recvfroms[r] = r * avgnum;
                recvtos[r] = (r + 1) * avgnum;

                if (r == slaveNum - 1) {
                    recvtos[r] = arrSize;
                }
            }

            for (int i = 0; i < arrSize; i++) {
                arr[i] = new ObjectNode(-1);
            }

            if (rank == rootRank) {
                for (int r = 0; r < slaveNum; r++) {
                    for (int i = recvfroms[r]; i < recvtos[r]; i++) {
                        arr[i] = new ObjectNode(r);
                    }
                }
            }
            start = System.currentTimeMillis();
            slave.scatterArray(arr, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), recvfroms, recvtos, rootRank);
            info("scatter ObjectNode arr takes:" + (System.currentTimeMillis() - start));

            for (int i = recvfroms[rank]; i < recvtos[rank]; i++) {
                if (arr[i].val != rank) {
                    info("scatter ObjectNode result error, rank:" + rank + ", arr:" + Arrays.toString(arr), false);
                    slave.close(1);
                }
            }

            info("scatter ObjectNode arr success!");
            if (arrSize < 500) {
                info("scatter result:" + Arrays.toString(arr), false);
            }


            // map
            info("begin to scatter ObjectNode map...");
            List<Map<String, ObjectNode>> mapList = new ArrayList<>();
            if (rank == rootRank) {
                for (int i = 0; i < slaveNum; i++) {
                    Map<String, ObjectNode> map = new HashMap<>(objSize);
                    mapList.add(map);
                    for (int j = i * objSize; j < (i + 1) * objSize; j++) {
                        map.put(j + "", new ObjectNode(j));
                    }
                }

                LOG.info("root origin:" + mapList);
            }
            start = System.currentTimeMillis();
            Map<String, ObjectNode> retMap = slave.scatterMap(mapList, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), rootRank);
            info("scatter ObjectNode map takes:" + (System.currentTimeMillis() - start));

            success = true;
            if (retMap.size() != objSize) {
                success = false;
            }

            for (int i = rank * objSize; i < (rank + 1) * objSize; i++) {
                ObjectNode val = retMap.getOrDefault(i + "", new ObjectNode(-1));
                if (val.val != i) {
                    success = false;
                }
            }

            if (!success) {
                info("scatter ObjectNode map error:" + retMap, false);
                slave.close(1);
            }

            if (objSize < 500) {
                info("scatter result:" + retMap, false);
            }
            info("scatter ObjectNode map success!");
        }

    }
}
