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
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.operator.Collective;
import com.fenbi.mp4j.operator.Container;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author xialong
 */
public abstract class MetaData<T> implements Writable {
    int srcRank;
    int destRank;
    int step = 0;
    int sum = 1;
    int segNum = 0;

    Collective collective;

    public abstract void insert(int ...vals);

    public int getSrcRank() {
        return srcRank;
    }

    public MetaData setSrcRank(int srcRank) {
        this.srcRank = srcRank;
        return this;
    }

    public int getDestRank() {
        return destRank;
    }

    public MetaData setDestRank(int destRank) {
        this.destRank = destRank;
        return this;
    }

    public MetaData setStep(int step) {
        this.step = step;
        return this;
    }

    public int getStep() {
        return step;
    }

    public void stepIncr(int delta) {
        step += delta;
    }

    public int getSum() {
        return sum;
    }

    public MetaData setSum(int sum) {
        this.sum = sum;
        return this;
    }

    public int getSegNum() {
        return segNum;
    }

    public MetaData setSegNum(int segNum) {
        this.segNum = segNum;
        return this;
    }

    public Collective getCollective() {
        return collective;
    }

    public MetaData setCollective(Collective collective) {
        this.collective = collective;
        return this;
    }

    public abstract void send(Output output) throws IOException;

    public abstract MetaData recv(Input input) throws IOException;

//    public abstract MetaData merge(MetaData that);

    public T getArrData() throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public MetaData setArrData(T arrData) throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public List<List<T>> getListDataList() throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public MetaData setListDataList(List<List<T>> listDataList) throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public List<Set<T>> getSetDataList() throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public MetaData setSetDataList(List<Set<T>> setDataList) throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public List<Map<String, T>> getMapDataList() throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public MetaData setMapDataList(List<Map<String, T>> mapDataList) throws Mp4jException {
        throw new Mp4jException("error invocation!");
    }

    public ArrayMetaData<T> convertToArrayMetaData() {
        return (ArrayMetaData<T>)this;
    }

    public MapMetaData<T> convertToMapMetaData() {
        return (MapMetaData<T>)this;
    }

    @Override
    public abstract void write(DataOutput dataOutput) throws IOException;

    @Override
    public abstract void readFields(DataInput dataInput) throws IOException;

    @Override
    public String toString() {
        return "MetaData{" +
                "srcRank=" + srcRank +
                ", destRank=" + destRank +
                ", step=" + step +
                ", sum=" + sum +
                ", segNum=" + segNum +
                ", collective=" + collective +
                '}';
    }

    public static <T> ArrayMetaData<T> newArrayMetaData() {
        return new ArrayMetaData<>();
    }

    public static <T> MapMetaData<T> newMapMetaData() {
        return new MapMetaData<>();
    }

    public static <T> MetaData<T> newMetaData(Container container) {
        switch (container) {
            case ARRAY:
                return newArrayMetaData();
            case MAP:
                return newMapMetaData();
            default:
                return null;
        }
    }

}
