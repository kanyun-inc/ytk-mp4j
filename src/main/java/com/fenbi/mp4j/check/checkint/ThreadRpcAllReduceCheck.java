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

import com.fenbi.mp4j.check.ThreadCheck;
import com.fenbi.mp4j.comm.ThreadCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operand.Operands;
import com.fenbi.mp4j.operator.Operators;

import java.util.Arrays;

/**
 * @author xialong
 */
public class ThreadRpcAllReduceCheck extends ThreadCheck {


    public ThreadRpcAllReduceCheck(ThreadCommSlave threadCommSlave, String serverHostName, int serverHostPort,
                                   int arrSize, int objSize, int runTime, int threadNum, boolean compress) {
        super(threadCommSlave, serverHostName, serverHostPort,
                arrSize, objSize, runTime, threadNum, compress);
    }

    @Override
    public void check() throws Mp4jException {
        final int[][] arr = new int[threadNum][arrSize];
        int slaveNum = threadCommSlave.getSlaveNum();
        int rank = threadCommSlave.getRank();
        int rootRank = 0;
        int rootThreadId = 0;

        Thread[] threads = new Thread[threadNum];
        for (int t = 0; t < threadNum; t++) {
            final int tidx = t;
            threads[t] = new Thread() {
                @Override
                public void run() {
                    try {
                        // set thread id
                        threadCommSlave.setThreadId(tidx);
                        boolean success = true;
                        long start;

                        long tarr = 0;
                        long tarrrpc = 0;
                        long tarrrpcs = 0;

                        int []arr = new int[arrSize];

                        for (int rt = 1; rt <= runTime; rt++) {
                            for (int i = 0; i < arrSize; i++) {
                                arr[i] = 1;
                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.allreduceArray(arr, Operands.INT_OPERAND(compress), Operators.Int.SUM, 0, arrSize);
                            tarr += (System.currentTimeMillis() - start);

                            for (int i = 0; i < arrSize; i++) {
                                if (arr[i] != slaveNum * threadNum) {
                                    success = false;
                                }
                            }
                            if (!success) {
                                info("thread allreduce int arr error", false);
                                if (arrSize < 500) {
                                    info("thread allreduce result:" + Arrays.toString(arr), false);
                                }
                                threadCommSlave.close(1);
                            }
                        }
                        info("thread allreduce int arr takes:" + tarr + ", times:" + runTime);

                        for (int rt = 1; rt <= runTime; rt++) {
                            for (int i = 0; i < arrSize; i++) {
                                arr[i] = 1;
                            }
                            start = System.currentTimeMillis();
                            threadCommSlave.allreduceArrayRpc(arr, Operands.INT_OPERAND(compress), Operators.Int.SUM);
                            tarrrpc += (System.currentTimeMillis() - start);

                            for (int i = 0; i < arrSize; i++) {
                                if (arr[i] != slaveNum * threadNum) {
                                    success = false;
                                }
                            }
                            if (!success) {
                                info("thread rpc allreduce int arr error", false);
                                if (arrSize < 500) {
                                    info("thread rpc allreduce result:" + Arrays.toString(arr), false);
                                }
                                threadCommSlave.close(1);
                            }
                        }
                        info("thread rpc allreduce int arr takes:" + tarrrpc + ", times:" + runTime);


                        for (int rt = 1; rt <= runTime; rt++) {
                            start = System.currentTimeMillis();
                            int ret = threadCommSlave.allreduceRpc(1, Operands.INT_OPERAND(compress), Operators.Int.SUM);
                            tarrrpcs += (System.currentTimeMillis() - start);

                            for (int i = 0; i < arrSize; i++) {
                                if (arr[i] != slaveNum * threadNum) {
                                    success = false;
                                }
                            }
                            if (!success) {
                                info("thread single rpc allreduce int arr error", false);
                                if (arrSize < 500) {
                                    info("thread single rpc allreduce result:" + Arrays.toString(arr), false);
                                }
                                threadCommSlave.close(1);
                            }
                        }
                        info("thread single rpc allreduce int arr takes:" + tarrrpcs + ", times:" + runTime);

                    } catch (Exception e) {
                        try {
                            threadCommSlave.exception(e);
                        } catch (Mp4jException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            };
            threads[t].start();
        }

        for (int t = 0; t < threadNum; t++) {
            try {
                threads[t].join();
            } catch (InterruptedException e) {
                throw new Mp4jException(e);
            }
        }
    }
}
