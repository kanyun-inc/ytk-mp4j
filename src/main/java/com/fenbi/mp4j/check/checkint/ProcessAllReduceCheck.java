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

package com.fenbi.mp4j.check.checkint;

import com.fenbi.mp4j.check.ProcessCheck;
import com.fenbi.mp4j.comm.ProcessCommSlave;
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
        int []arr = new int[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");

            // int array
            info("begin to allreduce int arr...");

            for (int i = 0; i < arrSize; i++) {
                arr[i] = 1;
            }
            start = System.currentTimeMillis();
            slave.allreduceArray(arr, Operands.INT_OPERAND(compress), Operators.Int.SUM, 0, arrSize);
            info("allreduce int arr takes:" + (System.currentTimeMillis() - start));

            for (int i = 0; i < arrSize; i++) {
                if (arr[i] != slaveNum) {
                    success = false;
                }
            }

            if (!success) {
                info("allreduce int arr error", false);
                slave.close(1);
            }

            info("allreduce int arr success!");
            if (arrSize < 500) {
                info("allreduce result:" + Arrays.toString(arr));
            }

            // map
            info("begin to allreduce int map...");
            Map<String, Integer> map = new HashMap<>(objSize);
            for (int i = 0; i < objSize; i++) {
                map.put(i + "", new Integer(1));
            }
            map.put(-(rank + 1) + "", new Integer(1));



            start = System.currentTimeMillis();
            Map<String, Integer> retMap = slave.allreduceMap(map, Operands.INT_OPERAND(compress), Operators.Int.SUM);
            info("allreduce int map takes:" + (System.currentTimeMillis() - start));

            success = true;
            for (int i = 0; i < objSize; i++) {
                Integer val = retMap.get(i + "");
                if (val == null || val.intValue() != slaveNum) {
                    success = false;
                }
            }

            for (int r = 0; r < slaveNum; r++) {
                String key = -(r + 1) + "";
                Integer val = retMap.get(key);
                if (val == null || val.intValue() != 1) {
                    success = false;
                }
            }

            if (retMap.size() != objSize + slaveNum) {
                success = false;
            }

            if (!success) {
                info("allreduce int map error:" + retMap);
                slave.close(1);
            }

            if (objSize < 500) {
                info("allreduce int map:" + retMap);
            }
            info("allreduce int map success!");

            // single int
            int singleInt = slave.allreduce(1, Operands.INT_OPERAND(compress), Operators.Int.SUM);
            if (singleInt != slaveNum) {
                info("allreduce single int error:" + singleInt, false);
                slave.close(1);
            }
            info("allreduce single int success!");
        }

    }
}
