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
        String []arr = new String[arrSize];

        for (int rt = 1; rt <= runTime; rt++) {
            info("run time:" + rt + "...");
            // String array
            info("begin to broadcast String arr...");

            for (int i = 0; i < arrSize; i++) {
                if (rank == rootRank) {
                    arr[i] = 1 + "";
                } else {
                    arr[i] = -1 + "";
                }
            }


            start = System.currentTimeMillis();
            arr = slave.broadcastArray(arr, Operands.STRING_OPERAND(compress), 0, arrSize, rootRank);
            info("broadcast String arr takes:" + (System.currentTimeMillis() - start));

            for (int i = 0; i < arr.length; i++) {
                if (Integer.parseInt(arr[i]) != 1) {
                    info("broadcast String array error:" + Arrays.toString(arr), false);
                    slave.close(1);
                }
            }
            info("broadcast String arr success!");
            if (arrSize < 500) {
                info("broadcast result:" + Arrays.toString(arr));
            }

            // map
            info("begin to broadcast String map...");
            Map<String, String> map = new HashMap<>();
            if (rank == rootRank) {
                for (int i = 0; i < objSize; i++) {
                    map.put(i + "", new String(i + ""));
                }
            }

            start = System.currentTimeMillis();
            Map<String, String> retMap = slave.broadcastMap(map, Operands.STRING_OPERAND(compress), rootRank);
            info("broadcast String map takes:" + (System.currentTimeMillis() - start));

            success = true;

            for (int i = 0; i < objSize; i++) {
                String key = i + "";
                String val = retMap.get(key);
                if (val == null || Integer.parseInt(val) != i) {
                    success = false;
                }

            }

            if (!success) {
                info("broadcast String map error:" + retMap);
                slave.close(1);
            }

            if (objSize < 500) {
                info("broadcast String map list:" + retMap.toString());
            }
            info("broadcast String map success!");

            // single string
            String singleString = slave.broadcast(rank + "", Operands.STRING_OPERAND(compress), rootRank);
            if (Integer.parseInt(singleString) != rootRank) {
                info("broadcast single String error:" + singleString, false);
                slave.close(1);
            }
            info("broadcast single String success!");
        }

    }
}
