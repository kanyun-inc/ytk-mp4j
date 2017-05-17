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

/**
 * @author xialong
 */
public class ProcessRpcAllReduceCheck extends ProcessCheck {

    public ProcessRpcAllReduceCheck(ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress) {
        super(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress);
    }

    @Override
    public void check() throws Mp4jException {
        int rank = slave.getRank();
        int slaveNum = slave.getSlaveNum();
        boolean success = true;
        long start;
        String []arr = new String[arrSize];
        long tarr = 0;
        long tarrrpc = 0;
        long tarrrpcs = 0;

        for (int rt = 1; rt <= runTime; rt++) {
            for (int i = 0; i < arrSize; i++) {
                arr[i] = 1 + "";
            }
            start = System.currentTimeMillis();
            slave.allreduceArray(arr, Operands.STRING_OPERAND(compress), new IStringOperator() {
                @Override
                public String apply(String s1, String s2) {
                    return (Integer.parseInt(s1) + Integer.parseInt(s2)) + "";
                }
            }, 0, arrSize);
            tarr += System.currentTimeMillis() - start;

            for (int i = 0; i < arrSize; i++) {
                if (Integer.parseInt(arr[i]) != slaveNum) {
                    success = false;
                }
            }

            if (!success) {
                info("allreduce String arr error", false);
                slave.close(1);
            }
        }

        info("allreduce String arr takes:" + tarr + ", times:" + runTime);

        for (int rt = 1; rt <= runTime; rt++) {
            for (int i = 0; i < arrSize; i++) {
                arr[i] = 1 + "";
            }
            start = System.currentTimeMillis();
            arr = slave.allreduceArrayRpc(arr, Operands.STRING_OPERAND(), new IStringOperator() {
                @Override
                public String apply(String s1, String s2) {
                    return (Integer.parseInt(s1) + Integer.parseInt(s2)) + "";
                }
            });
            tarrrpc += System.currentTimeMillis() - start;

            for (int i = 0; i < arrSize; i++) {
                if (Integer.parseInt(arr[i]) != slaveNum) {
                    success = false;
                }
            }
            if (!success) {
                info("rpc array allreduce check error!" + Arrays.toString(arr));
                slave.close(1);
            }
        }

        info("rpc allreduce String arr takes:" + tarrrpc + ", times:" + runTime);

        for (int rt = 1; rt <= runTime; rt++) {
            start = System.currentTimeMillis();
            String ret = slave.allreduceRpc(1 + "", Operands.STRING_OPERAND(), new IStringOperator() {
                        @Override
                        public String apply(String s1, String s2) {
                            return (Integer.parseInt(s1) + Integer.parseInt(s2)) + "";
                        }

                        ;
                    });
            tarrrpcs += System.currentTimeMillis() - start;

            if (Integer.parseInt(ret) != slaveNum) {
                info("rpc single allreduce check error!" + Arrays.toString(arr));
                slave.close(1);
            }
        }

        info("rpc single allreduce String arr takes:" + tarrrpcs + ", times:" + runTime);

    }
}
