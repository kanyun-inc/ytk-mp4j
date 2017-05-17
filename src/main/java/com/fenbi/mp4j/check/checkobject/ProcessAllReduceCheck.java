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
import com.fenbi.mp4j.operator.IObjectOperator;
import com.fenbi.mp4j.operator.Operators;
import com.fenbi.mp4j.utils.KryoUtils;

import java.util.*;

/**
 * @author xialong
 */
public class ProcessAllReduceCheck extends ProcessCheck {

    public ProcessAllReduceCheck(ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress) {
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
            info("begin to allreduce ObjectNode arr...");

            for (int i = 0; i < arrSize; i++) {
                arr[i] = new ObjectNode(1);
            }
            start = System.currentTimeMillis();
            slave.allreduceArray(arr, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), new IObjectOperator<ObjectNode>() {
                @Override
                public ObjectNode apply(ObjectNode d1, ObjectNode d2) {
                    d1.val += d2.val;
                    return d1;
                }
            }, 0, arrSize);
            info("allreduce ObjectNode arr takes:" + (System.currentTimeMillis() - start));

            for (int i = 0; i < arrSize; i++) {
                if (arr[i].val != slaveNum) {
                    success = false;
                }
            }

            if (!success) {
                info("allreduce ObjectNode arr error", false);
                slave.close(1);
            }

            info("allreduce ObjectNode arr success!");
            if (arrSize < 500) {
                info("allreduce result:" + Arrays.toString(arr));
            }

            // map
            info("begin to allreduce ObjectNode map...");
            Map<String, ObjectNode> map = new HashMap<>(objSize);
            for (int i = 0; i < objSize; i++) {
                map.put(i + "", new ObjectNode(1));
            }
            map.put(-(rank + 1) + "", new ObjectNode(1));



            start = System.currentTimeMillis();
            Map<String, ObjectNode> retMap = slave.allreduceMap(map, Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), new IObjectOperator<ObjectNode>() {
                @Override
                public ObjectNode apply(ObjectNode d1, ObjectNode d2) {
                    d1.val += d2.val;
                    return d1;
                }
            });
            info("allreduce ObjectNode map takes:" + (System.currentTimeMillis() - start));

            success = true;
            for (int i = 0; i < objSize; i++) {
                ObjectNode val = retMap.get(i + "");
                if (val == null || val.val != slaveNum) {
                    success = false;
                }
            }

            for (int r = 0; r < slaveNum; r++) {
                String key = -(r + 1) + "";
                ObjectNode val = retMap.get(key);
                if (val == null || val.val != 1) {
                    success = false;
                }
            }

            if (retMap.size() != objSize + slaveNum) {
                success = false;
            }

            if (!success) {
                info("allreduce ObjectNode map error:" + retMap);
                slave.close(1);
            }

            if (objSize < 500) {
                info("allreduce ObjectNode map:" + retMap);
            }
            info("allreduce ObjectNode map success!");

            // allreduce map set union
            Set<Integer> set = new HashSet<>();
            for (int i = 0; i < objSize; i++) {
                set.add(i);
            }
            set.add(-(rank + 1));
            start = System.currentTimeMillis();
            Set<Integer> reducedMapSetU = slave.allreduceSetUnion(set, KryoUtils.getDefaultSerializer(Integer.class), Integer.class);
            info("allreduce map set union takes:" + (System.currentTimeMillis() - start));

            if (reducedMapSetU.size() != slaveNum + objSize) {
                success = false;
            }

            for (int i = 0; i < objSize; i++) {
                if (!reducedMapSetU.contains(i)) {
                    success = false;
                }
            }

            for (int r = 0; r < slaveNum; r++) {
                if (!reducedMapSetU.contains(-(r + 1))) {
                    success = false;
                }
            }

            if (!success) {
                info("allreduce map set union error:" + reducedMapSetU, false);
                slave.close(1);
            }

            if (success) {
                info("allreduce map set union success!");
            }


            // allreduce map set intersection
            set = new HashSet<>();
            for (int i = 0; i < objSize; i++) {
                set.add(i);
            }
            set.add(-(rank + 1));
            start = System.currentTimeMillis();
            Set<Integer> reducedMapSetI = slave.allreduceSetIntersection(set, KryoUtils.getDefaultSerializer(Integer.class), Integer.class);
            info("allreduce map set intersection takes:" + (System.currentTimeMillis() - start));

            if (slaveNum > 1) {
                if (reducedMapSetI.size() != objSize) {
                    success = false;
                }

                for (int i = 0; i < objSize; i++) {
                    if (!reducedMapSetI.contains(i)) {
                        success = false;
                    }
                }
            } else {
                if (reducedMapSetI.size() != objSize + slaveNum) {
                    success = false;
                }

                for (int i = 0; i < objSize; i++) {
                    if (!reducedMapSetI.contains(i)) {
                        success = false;
                    }
                }

                for (int r = 0; r < slaveNum; r++) {
                    if (!reducedMapSetI.contains(-(r + 1))) {
                        success = false;
                    }
                }
            }


            if (!success) {
                info("allreduce map set intersection error:" + reducedMapSetI, false);
                slave.close(1);
            }

            if (success) {
                info("allreduce map set intersection success!");
            }


            // allreduce map list concat
            List<Integer> list = new ArrayList<>();
            list.add(rank);

            start = System.currentTimeMillis();
            List<Integer> reducedMapList = slave.allreduceListConcat(list, KryoUtils.getDefaultSerializer(Integer.class), Integer.class);
            info("allreduce map list concat takes:" + (System.currentTimeMillis() - start));

            if (reducedMapList.size() != slaveNum) {
                info("allreduce map list concat error:" + reducedMapList, false);
                slave.close(1);
                success = false;
            }
            for (int r = 0; r < slaveNum; r++) {
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
                info("allreduce map list concat error:" + reducedMapList, false);
                slave.close(1);
            }

            if (success) {
                info("allreduce map list concat success!");
            }

            // single ObjectNode
            ObjectNode singleObject = slave.allreduce(new ObjectNode(1), Operands.OBJECT_OPERAND(new ObjectNodeSerializer(), ObjectNode.class), new IObjectOperator<ObjectNode>() {
                @Override
                public ObjectNode apply(ObjectNode d1, ObjectNode d2) {
                    d1.val += d2.val;
                    return d1;
                }
            });
            if (singleObject.val != slaveNum) {
                info("allreduce single ObjectNode error:" + singleObject, false);
                slave.close(1);
            }
            info("allreduce single ObjectNode success!");

        }

    }
}
