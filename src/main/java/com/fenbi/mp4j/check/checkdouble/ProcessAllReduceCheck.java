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

package com.fenbi.mp4j.check.checkdouble;

import com.fenbi.mp4j.check.ProcessCheck;
import com.fenbi.mp4j.comm.ProcessCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.operator.IIntOperator;
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
        double []arr = new double[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");

            // double array
            info("begin to allreduce double arr...");

            for (int i = 0; i < arrSize; i++) {
                arr[i] = 1;
            }
            start = System.currentTimeMillis();
            slave.allreduceArray(arr, Operands.DOUBLE_OPERAND(compress), Operators.Double.SUM, 0, arrSize);
            info("allreduce double arr takes:" + (System.currentTimeMillis() - start));

            for (int i = 0; i < arrSize; i++) {
                if (arr[i] != slaveNum) {
                    success = false;
                }
            }

            if (!success) {
                info("allreduce double arr error", false);
                slave.close(1);
            }

            info("allreduce double arr success!");
            if (arrSize < 500) {
                info("allreduce result:" + Arrays.toString(arr));
            }

            // map
            info("begin to allreduce double map...");
            Map<String, Double> map = new HashMap<>(objSize);
            for (int i = 0; i < objSize; i++) {
                map.put(i + "", new Double(1));
            }
            map.put(-(rank + 1) + "", new Double(1));



            start = System.currentTimeMillis();
            Map<String, Double> retMap = slave.allreduceMap(map, Operands.DOUBLE_OPERAND(compress), Operators.Double.SUM);
            info("allreduce double map takes:" + (System.currentTimeMillis() - start));

            success = true;
            for (int i = 0; i < objSize; i++) {
                Double val = retMap.get(i + "");
                if (val == null || val.intValue() != slaveNum) {
                    success = false;
                }
            }

            for (int r = 0; r < slaveNum; r++) {
                String key = -(r + 1) + "";
                Double val = retMap.get(key);
                if (val == null || val.intValue() != 1) {
                    success = false;
                }
            }

            if (retMap.size() != objSize + slaveNum) {
                success = false;
            }

            if (!success) {
                info("allreduce double map error:" + retMap);
                slave.close(1);
            }

            if (objSize < 500) {
                info("allreduce double map:" + retMap);
            }
            info("allreduce double map success!");

            // single double
            double singleDouble = slave.allreduce(1.0, Operands.DOUBLE_OPERAND(compress), Operators.Double.SUM);
            if (((int)singleDouble) != slaveNum) {
                info("allreduce single double error:" + singleDouble, false);
                slave.close(1);
            }
            info("allreduce single double success!");
        }

    }
}
