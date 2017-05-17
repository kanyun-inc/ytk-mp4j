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

package com.fenbi.mp4j.meta;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xialong
 */
public class ArrayMetaData<T> extends MetaData<T> implements Writable{
    private static final Logger LOG = LoggerFactory.getLogger(ArrayMetaData.class);
    List<Integer> ranks = new ArrayList<>();
    List<Integer> segFroms = new ArrayList<>();
    List<Integer> segTos = new ArrayList<>();
    T arrData;

    public ArrayMetaData() {}

    public void insert(int ...vals) {
        segNum++;
        ranks.add(vals[0]);
        segFroms.add(vals[1]);
        segTos.add(vals[2]);
    }

    public int getRank(int idx) {
        return ranks.get(idx);
    }

    public int getFrom(int idx) {
        return segFroms.get(idx);
    }

    public int getTo(int idx) {
        return segTos.get(idx);
    }

    public List<Integer> getRanks() {
        return ranks;
    }

    public void setRanks(List<Integer> ranks) {
        this.ranks = ranks;
    }

    public List<Integer> getSegFroms() {
        return segFroms;
    }

    public void setSegFroms(List<Integer> segFroms) {
        this.segFroms = segFroms;
    }

    public List<Integer> getSegTos() {
        return segTos;
    }

    public void setSegTos(List<Integer> segTos) {
        this.segTos = segTos;
    }

    @Override
    public T getArrData() {
        return arrData;
    }

    @Override
    public MetaData setArrData(T arrData) {
        this.arrData = arrData;
        return this;
    }

    @Override
    public void send(Output output) throws IOException {
        output.writeInt(srcRank);
        output.writeInt(destRank);
        output.writeInt(step);
        output.writeInt(sum);
        output.writeInt(segNum);
        for (int i = 0; i < segNum; i++) {
            output.writeInt(ranks.get(i));
            output.writeInt(segFroms.get(i));
            output.writeInt(segTos.get(i));
        }
        output.flush();
    }

    @Override
    public ArrayMetaData recv(Input input) throws IOException {
        ArrayMetaData metaData = new ArrayMetaData();
        metaData.srcRank = input.readInt();
        metaData.destRank = input.readInt();
        metaData.step = input.readInt();
        metaData.sum = input.readInt();
        int segNum = input.readInt();
        for (int i = 0; i < segNum; i++) {
            int rank = input.readInt();
            int from = input.readInt();
            int to = input.readInt();
           metaData.insert(rank, from, to);
        }

        return metaData;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(srcRank);
        dataOutput.writeInt(destRank);
        dataOutput.writeInt(step);
        dataOutput.writeInt(sum);
        dataOutput.writeInt(segNum);
        for (int i = 0; i < segNum; i++) {
            dataOutput.writeInt(ranks.get(i));
            dataOutput.writeInt(segFroms.get(i));
            dataOutput.writeInt(segTos.get(i));
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        srcRank = dataInput.readInt();
        destRank = dataInput.readInt();
        step = dataInput.readInt();
        sum = dataInput.readInt();
        int segNum = dataInput.readInt();
        for (int i = 0; i < segNum; i++) {
            int rank = dataInput.readInt();
            int from = dataInput.readInt();
            int to = dataInput.readInt();
            insert(rank, from, to);
        }
    }

    @Override
    public String toString() {
        return super.toString() + ", ranks:" + ranks + ", segfroms:" + segFroms + ", segtos:" + segTos;
    }
}
