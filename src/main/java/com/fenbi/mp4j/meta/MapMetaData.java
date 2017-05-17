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
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xialong
 */
public class MapMetaData<T> extends MetaData<T> implements Writable {
    List<Integer> ranks = new ArrayList<>();
    List<Integer> dataNums = new ArrayList<>();

    List<Map<String, T>> mapDataList;

    public MapMetaData() {}

    public int getRank(int idx) {
        return ranks.get(idx);
    }


    public int getDataNum(int idx) {
        return dataNums.get(idx);
    }

    public List<Integer> getRanks() {
        return ranks;
    }

    public void setRanks(List<Integer> ranks) {
        this.ranks = ranks;
    }

    public List<Integer> getDataNums() {
        return dataNums;
    }

    public void setDataNums(List<Integer> dataNums) {
        this.dataNums = dataNums;
    }

    @Override
    public List<Map<String, T>> getMapDataList() {
        return mapDataList;
    }

    @Override
    public MetaData setMapDataList(List<Map<String, T>> mapDataList) {
        this.mapDataList = mapDataList;
        return this;
    }

    @Override
    public void insert(int ...vals) {
        segNum ++;
        ranks.add(vals[0]);
        dataNums.add(vals[1]);
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
            dataOutput.writeInt(dataNums.get(i));
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        srcRank = dataInput.readInt();
        destRank = dataInput.readInt();
        step = dataInput.readInt();
        sum = dataInput.readInt();
        segNum = dataInput.readInt();
        for (int i = 0; i < segNum; i++) {
            ranks.add(dataInput.readInt());
            dataNums.add(dataInput.readInt());
        }

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
            output.writeInt(dataNums.get(i));
        }
        output.flush();
    }

    @Override
    public MapMetaData recv(Input input) throws IOException {
        MapMetaData metaData = new MapMetaData();
        metaData.srcRank = input.readInt();
        metaData.destRank = input.readInt();
        metaData.step = input.readInt();
        metaData.sum = input.readInt();
        int segNum = input.readInt();
        for (int i = 0; i < segNum; i++) {
            int rank = input.readInt();
            metaData.insert(rank, input.readInt());
        }
        return metaData;
    }

    @Override
    public String toString() {
        return super.toString() + ", ranks:" + ranks + ", datanums:" + dataNums;
    }

}
