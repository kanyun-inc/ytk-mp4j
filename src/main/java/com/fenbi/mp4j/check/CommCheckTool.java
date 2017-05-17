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

package com.fenbi.mp4j.check;

import com.fenbi.mp4j.comm.ProcessCommSlave;
import com.fenbi.mp4j.comm.ThreadCommSlave;
import com.fenbi.mp4j.exception.Mp4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xialong on 1/8/17.
 */
public class CommCheckTool {
    public static final Logger LOG = LoggerFactory.getLogger(CommCheckTool.class);
    public static void main(String []args) {
        String loginName = args[0];
        String serverHostName = args[1];
        int serverHostPort = Integer.parseInt(args[2]);
        int arrSize = Integer.parseInt(args[3]);
        int objSize = Integer.parseInt(args[4]);
        int runTime = Integer.parseInt(args[5]);
        int threadnum = Integer.parseInt(args[6]);
        String mode = args[7];
        boolean compress = Boolean.parseBoolean(args[8]);
        boolean testRpc = Boolean.parseBoolean(args[9]);

        ProcessCommSlave processCommSlave = null;
        ThreadCommSlave threadCommSlave = null;
        int closecode = 0;

        try {
            List<CommCheck> checks = new ArrayList<>();
            if (mode.equalsIgnoreCase("process")) {
                processCommSlave = new ProcessCommSlave(loginName, serverHostName, serverHostPort);
                checks = createProcessCheckList(processCommSlave,
                        serverHostName, serverHostPort, arrSize, objSize, runTime, compress, testRpc);
            } else if (mode.equalsIgnoreCase("thread")) {
                threadCommSlave = new ThreadCommSlave(loginName, threadnum, serverHostName, serverHostPort);
                checks = createThreadCheckList(threadCommSlave,
                        serverHostName, serverHostPort, arrSize, objSize, runTime, threadnum, compress, testRpc);
            } else {
                LOG.error("error model");
            }

            for (CommCheck commCheck : checks) {
                commCheck.check();
            }


        } catch (Exception e) {
            closecode = 1;
            LOG.error("error", e);
            try {
                processCommSlave.exception(e);
            } catch (Mp4jException e1) {
                LOG.error("error", e);
            }
        } finally {
            if (mode.equalsIgnoreCase("process")) {
                try {
                    LOG.info("finally close");
                    processCommSlave.close(closecode);
                } catch (Mp4jException e) {
                    LOG.error("error", e);
                }
            } else if (mode.equalsIgnoreCase("thread")) {
                try {
                    LOG.info("finally close");
                    threadCommSlave.close(closecode);
                } catch (Mp4jException e) {
                    LOG.error("error", e);
                }
            } else {
                LOG.error("error model");
            }

            System.exit(closecode);
        }

    }

    public static List<CommCheck> createProcessCheckList(
            ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress, boolean testRpc) {
        List<CommCheck> checks = new ArrayList<>();
        // double
        checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkdouble.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }


        // float
        checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkfloat.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }

        // long
        checks.add(new com.fenbi.mp4j.check.checklong.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checklong.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }

        // int
        checks.add(new com.fenbi.mp4j.check.checkint.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkint.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }

        // short
        checks.add(new com.fenbi.mp4j.check.checkshort.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkshort.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }

        // byte
        checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkbyte.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }

        // string
        checks.add(new com.fenbi.mp4j.check.checkstring.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkstring.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }

        // object
        checks.add(new com.fenbi.mp4j.check.checkobject.ProcessGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ProcessScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ProcessAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ProcessReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ProcessBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ProcessReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ProcessAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkobject.ProcessRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, compress));
        }

        return checks;
    }

    public static List<CommCheck> createThreadCheckList(
            ThreadCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, int threadNum, boolean compress, boolean testRpc) {
        List<CommCheck> checks = new ArrayList<>();
        // double
        checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkdouble.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }


        // float
        checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkfloat.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }

        // long
        checks.add(new com.fenbi.mp4j.check.checklong.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checklong.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checklong.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }

        // int
        checks.add(new com.fenbi.mp4j.check.checkint.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkint.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkint.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }

        // short
        checks.add(new com.fenbi.mp4j.check.checkshort.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkshort.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkshort.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }

        // byte
        checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkbyte.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }

        // string
        checks.add(new com.fenbi.mp4j.check.checkstring.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkstring.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkstring.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }

        // object
        checks.add(new com.fenbi.mp4j.check.checkobject.ThreadGatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ThreadAllgatherCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ThreadBroadcastCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ThreadScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ThreadReduceScatterCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ThreadReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        checks.add(new com.fenbi.mp4j.check.checkobject.ThreadAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));

        if (testRpc) {
            checks.add(new com.fenbi.mp4j.check.checkobject.ThreadRpcAllReduceCheck(slave, serverHostName, serverHostPort, arrSize, objSize, runTime, threadNum, compress));
        }

        return checks;
    }


}
