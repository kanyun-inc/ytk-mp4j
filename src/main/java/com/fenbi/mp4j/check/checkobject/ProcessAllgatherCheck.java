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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xialong
 */
public class ProcessAllgatherCheck extends ProcessCheck {

    public ProcessAllgatherCheck(ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress) {
        super(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress);
    }

    @Override
    public void check() throws Mp4jException {
        int rank = slave.getRank();
        int slaveNum = slave.getSlaveNum();
        boolean success = true;
        long start;
        ObjectNode []arr = new ObjectNode[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");
            // ObjectNode array
            info("begin to allgather ObjectNode arr...");
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
                arr[i] = new ObjectNode(rank);
            }

            start = System.currentTimeMillis();
            slave.allgatherArray(arr, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), froms, tos);
            info("allgather ObjectNode arr takes:" + (System.currentTimeMillis() - start));

            for (int i = 0; i < arr.length; i++) {
                int r = avgnum == 0 ? slaveNum - 1 : Math.min(i / avgnum, slaveNum - 1);
                if (arr[i].val != r) {
                    info("allgather ObjectNode array error:" + Arrays.toString(arr), false);
                    slave.close(1);
                }
            }
            info("allgather ObjectNode arr success!");
            if (arrSize < 500) {
                info("allgather result:" + Arrays.toString(arr));
            }

            // map
            info("begin to allgather ObjectNode map...");
            Map<String, ObjectNode> map = new HashMap<>(objSize);
            for (int i = rank * objSize; i < (rank + 1) * objSize; i++) {
                map.put(i + "", new ObjectNode(i));
            }
            start = System.currentTimeMillis();
            List<Map<String, ObjectNode>> retMapList = slave.allgatherMap(map, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class));
            info("allgather ObjectNode map takes:" + (System.currentTimeMillis() - start));

            success = true;
            if (retMapList.size() != slaveNum) {
                info("allgather ObjectNode map retMapList size:" + retMapList.size() + ", expected size:" + slaveNum);
                success = false;
            }

            for (int i = 0; i < slaveNum; i++) {
                int idxstart = i * objSize;
                Map<String, ObjectNode> retMap = retMapList.get(i);
                for (int j = 0; j < objSize; j++) {
                    ObjectNode val = retMap.get((idxstart + j) + "");
                    if (val.val != (idxstart + j)) {
                        success = false;
                    }
                }
            }

            if (!success) {
                info("allgather ObjectNode map error:" + retMapList);
                slave.close(1);
            }

            if (objSize < 500) {
                info("allgather ObjectNode map list:" + retMapList.toString());
            }
            info("allgather ObjectNode map success!");
        }

    }
}
