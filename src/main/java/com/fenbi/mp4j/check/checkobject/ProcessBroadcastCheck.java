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
public class ProcessBroadcastCheck extends ProcessCheck {

    public ProcessBroadcastCheck(ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress) {
        super(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress);
    }

    @Override
    public void check() throws Mp4jException {
        int rank = slave.getRank();
        int slaveNum = slave.getSlaveNum();
        boolean success = true;
        long start;
        int rootRank = 0;
        ObjectNode []arr = new ObjectNode[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");
            // ObjectNode array
            info("begin to broadcast ObjectNode arr...");

            for (int i = 0; i < arrSize; i++) {
                if (rank == rootRank) {
                    arr[i] = new ObjectNode(1);
                } else {
                    arr[i] = new ObjectNode(-1);
                }
            }


            start = System.currentTimeMillis();
            arr = slave.broadcastArray(arr, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), 0, arrSize, rootRank);
            info("broadcast ObjectNode arr takes:" + (System.currentTimeMillis() - start));

            for (int i = 0; i < arr.length; i++) {
                if (arr[i].val != 1) {
                    info("broadcast ObjectNode array error:" + Arrays.toString(arr), false);
                    slave.close(1);
                }
            }
            info("broadcast ObjectNode arr success!");
            if (arrSize < 500) {
                info("broadcast result:" + Arrays.toString(arr));
            }

            // map
            info("begin to broadcast ObjectNode map...");
            Map<String, ObjectNode> map = new HashMap<>();
            if (rank == rootRank) {
                for (int i = 0; i < objSize; i++) {
                    map.put(i + "", new ObjectNode(i));
                }
            }

            start = System.currentTimeMillis();
            Map<String, ObjectNode> retMap = slave.broadcastMap(map, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), rootRank);
            info("broadcast ObjectNode map takes:" + (System.currentTimeMillis() - start));

            success = true;

            for (int i = 0; i < objSize; i++) {
                String key = i + "";
                ObjectNode val = retMap.get(key);
                if (val == null || val.val != i) {
                    success = false;
                }

            }

            if (!success) {
                info("broadcast ObjectNode map error:" + retMap);
                slave.close(1);
            }

            if (objSize < 500) {
                info("broadcast ObjectNode map list:" + retMap.toString());
            }
            info("broadcast ObjectNode map success!");

            // single obj
            ObjectNode singleObject = slave.broadcast(new ObjectNode(rank), Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), rootRank);
            if (singleObject.val != rootRank) {
                info("broadcast single object error:" + singleObject, false);
                slave.close(1);
            }
            info("broadcast single object success!");
        }

    }
}
