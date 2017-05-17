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
import com.fenbi.mp4j.exception.Mp4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xialong on 1/8/17.
 */
public abstract class ProcessCheck implements CommCheck {
    public static final Logger LOG = LoggerFactory.getLogger(ProcessCheck.class);
    public final ProcessCommSlave slave;
    public final String serverHostName;
    public final int serverHostPort;
    public final int arrSize;
    public final int objSize;
    public final int runTime;
    public final boolean compress;

    public ProcessCheck(ProcessCommSlave slave, String serverHostName, int serverHostPort, int arrSize, int objSize, int runTime, boolean compress) {
        this.slave = slave;
        this.serverHostName = serverHostName;
        this.serverHostPort = serverHostPort;
        this.arrSize = arrSize;
        this.objSize = objSize;
        this.runTime = runTime;
        this.compress = compress;
    }

    public void info(String info, boolean single) throws Mp4jException {
        slave.info(info, single);
        LOG.info(info);
    }

    public void info(String info) throws Mp4jException {
        info(info, true);
    }
}
