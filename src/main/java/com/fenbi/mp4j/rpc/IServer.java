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

package com.fenbi.mp4j.rpc;

import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operator.IIntOperator;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.util.concurrent.BrokenBarrierException;

/**
 * @author xialong
 */
public interface IServer extends VersionedProtocol {
    public static final long versionID = 1L;

    public Text getAllSlavesInfo(Text slaveSock) throws Mp4jException, BrokenBarrierException, InterruptedException;
    public void info(int rank, Text info) throws Mp4jException;
    public void debug(int rank, Text debug) throws Mp4jException;
    public void error(int rank, Text error) throws Mp4jException;
    public void close(int rank, int code) throws Mp4jException;
    public void shutdown(int error, Text message) throws Mp4jException;
    public void killMe(int rank, Text script) throws Mp4jException;
    public void writeFile(Text content, Text fileName) throws Mp4jException;
    public void heartbeat(int rank) throws Mp4jException;

    public void barrier() throws Mp4jException;
    public int exchange(int rank) throws Mp4jException;

    public ArrayPrimitiveWritable primitiveArrayAllReduce(ArrayPrimitiveWritable arrayPrimitiveWritable, int rank) throws Mp4jException;
    public ArrayPrimitiveWritable arrayAllReduce(ArrayPrimitiveWritable byteSerialArray, int rank) throws Mp4jException;


}
