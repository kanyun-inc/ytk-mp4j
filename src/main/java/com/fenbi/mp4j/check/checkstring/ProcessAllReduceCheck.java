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

package com.fenbi.mp4j.check.checkstring;

import com.fenbi.mp4j.check.ProcessCheck;
import com.fenbi.mp4j.comm.ProcessCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.operator.IStringOperator;
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
        String []arr = new String[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");

            // String array
            info("begin to allreduce String arr...");

            for (int i = 0; i < arrSize; i++) {
                arr[i] = 1 + "";
            }
            start = System.currentTimeMillis();
            slave.allreduceArray(arr, Operands.STRING_OPERAND(compress), new IStringOperator() {
                @Override
                public String apply(String s1, String s2) {
                    return String.valueOf(Integer.parseInt(s1) + Integer.parseInt(s2));
                }
            }, 0, arrSize);
            info("allreduce String arr takes:" + (System.currentTimeMillis() - start));

            for (int i = 0; i < arrSize; i++) {
                if (Integer.parseInt(arr[i]) != slaveNum) {
                    success = false;
                }
            }

            if (!success) {
                info("allreduce String arr error", false);
                slave.close(1);
            }

            info("allreduce String arr success!");
            if (arrSize < 500) {
                info("allreduce result:" + Arrays.toString(arr));
            }

            // map
            info("begin to allreduce String map...");
            Map<String, String> map = new HashMap<>(objSize);
            for (int i = 0; i < objSize; i++) {
                map.put(i + "", new String(1 + ""));
            }
            map.put(-(rank + 1) + "", new String(1 + ""));



            start = System.currentTimeMillis();
            Map<String, String> retMap = slave.allreduceMap(map, Operands.STRING_OPERAND(compress), new IStringOperator() {
                @Override
                public String apply(String s1, String s2) {
                    return String.valueOf(Integer.parseInt(s1) + Integer.parseInt(s2));
                }
            });
            info("allreduce String map takes:" + (System.currentTimeMillis() - start));

            success = true;
            for (int i = 0; i < objSize; i++) {
                String val = retMap.get(i + "");
                if (val == null || Integer.parseInt(val) != slaveNum) {
                    success = false;
                }
            }

            for (int r = 0; r < slaveNum; r++) {
                String key = -(r + 1) + "";
                String val = retMap.get(key);
                if (val == null || Integer.parseInt(val) != 1) {
                    success = false;
                }
            }

            if (retMap.size() != objSize + slaveNum) {
                success = false;
            }

            if (!success) {
                info("allreduce String map error:" + retMap);
                slave.close(1);
            }

            if (objSize < 500) {
                info("allreduce String map:" + retMap);
            }
            info("allreduce String map success!");


            // single String
            String singleString = slave.allreduce(1 + "", Operands.STRING_OPERAND(compress), new IStringOperator() {
                @Override
                public String apply(String s1, String s2) {
                    return String.valueOf(Integer.parseInt(s1) + Integer.parseInt(s2));
                }
            });
            if (Integer.parseInt(singleString) != slaveNum) {
                info("allreduce single String error:" + singleString, false);
                slave.close(1);
            }
            info("allreduce single String success!");
        }

    }
}
