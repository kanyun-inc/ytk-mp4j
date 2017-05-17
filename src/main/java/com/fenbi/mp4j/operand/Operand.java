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

package com.fenbi.mp4j.operand;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.meta.MetaData;
import com.fenbi.mp4j.operator.Collective;
import com.fenbi.mp4j.operator.Container;
import com.fenbi.mp4j.operator.IOperator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author xialong
 */
public abstract class Operand {

    Collective collective;
    Container container;

    public boolean isCompress() {
        return compress;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    boolean compress = false;

    public Collective getCollective() {
        return collective;
    }

    public void setCollective(Collective collective) {
        this.collective = collective;
    }

    public Container getContainer() {
        return container;
    }

    public void setContainer(Container container) {
        this.container = container;
    }

    public abstract void send(Output output, MetaData metaData) throws IOException, Mp4jException;
    public abstract MetaData recv(Input input, MetaData metaData) throws IOException, Mp4jException;
    public abstract void setOperator(IOperator operator);
    public abstract void threadCopy(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException;
    public abstract void threadArrayAllCopy(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException;
    public abstract void threadReduce(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException;
    public abstract void threadMerge(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException;
}
