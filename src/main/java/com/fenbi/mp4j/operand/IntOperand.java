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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DeflateSerializer;
import com.fenbi.mp4j.exception.Mp4jException;
import com.fenbi.mp4j.meta.ArrayMetaData;
import com.fenbi.mp4j.meta.MapMetaData;
import com.fenbi.mp4j.meta.MetaData;
import com.fenbi.mp4j.operator.*;
import com.fenbi.mp4j.utils.KryoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author xialong
 */
public class IntOperand extends Operand {
    public static final Logger LOG = LoggerFactory.getLogger(IntOperand.class);
    public IIntOperator operator;


    public static class Mp4jIntArraySerializer extends Serializer<ArrayMetaData<int[]>> {
        ArrayMetaData<int[]> arrayMetaData;
        ArrayMetaData<int[]> thatArrMetaData;
        public Mp4jIntArraySerializer(ArrayMetaData<int[]> arrayMetaData) {
            this.setAcceptsNull(true);
            this.arrayMetaData = arrayMetaData;
        }

        public ArrayMetaData<int[]> getThatArrMetaData() {
            return thatArrMetaData;
        }

        public void write(Kryo kryo, Output output, ArrayMetaData<int[]> object) {
            try {
                int[] arrData = arrayMetaData.getArrData();
                arrayMetaData.send(output);
                int arrSegNum = arrayMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = arrayMetaData.getFrom(i);
                    int to = arrayMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        output.writeInt(arrData[j]);
                    }
                }
            } catch (IOException e) {
                LOG.error("double array write exception", e);
                System.exit(1);
            }
        }

        public ArrayMetaData<int[]> read(Kryo kryo, Input input, Class<ArrayMetaData<int[]>> type) {
            try {
                int[] arrData = arrayMetaData.getArrData();
                thatArrMetaData = arrayMetaData.recv(input);
                int arrSegNum = thatArrMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = thatArrMetaData.getFrom(i);
                    int to = thatArrMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        arrData[j] = input.readInt();
                    }
                }
                thatArrMetaData.setArrData(arrData);
            } catch (IOException e) {
                LOG.error("double array read exception", e);
                System.exit(1);
            }
            return thatArrMetaData;

        }

    }

    public static class Mp4jIntMapSerializer extends Serializer<MapMetaData<Integer>> {
        MapMetaData<Integer> mapMetaData;
        MapMetaData<Integer> thatMapMetaData;

        public Mp4jIntMapSerializer(MapMetaData<Integer> mapMetaData) {
            this.setAcceptsNull(true);
            this.mapMetaData = mapMetaData;
        }

        public MapMetaData<Integer> getThatMapMetaData() {
            return thatMapMetaData;
        }

        public void write(Kryo kryo, Output output, MapMetaData<Integer> object) {
            try {
                List<Map<String, Integer>> mapDataList = mapMetaData.getMapDataList();
                mapMetaData.send(output);
                int mapSegNum = mapMetaData.getSegNum();
                for (int i = 0; i < mapSegNum; i++) {
                    Map<String, Integer> mapData = mapDataList.get(i);
                    for (Map.Entry<String, Integer> entry : mapData.entrySet()) {
                        output.writeString(entry.getKey());
                        output.writeInt(entry.getValue());
                    }
                    if (mapMetaData.getCollective() == Collective.GATHER ||
                            mapMetaData.getCollective() == Collective.SCATTER ||
                            mapMetaData.getCollective() == Collective.REDUCE_SCATTER) {
                        mapData.clear();
                    }
                }

            } catch (IOException e) {
                LOG.error("double array write exception", e);
                System.exit(1);
            }
        }

        @Override
        public MapMetaData<Integer> read(Kryo kryo, Input input, Class<MapMetaData<Integer>> type) {
            try {
                thatMapMetaData = mapMetaData.recv(input);
                int thatMapSegNum = thatMapMetaData.getSegNum();
                List<Map<String, Integer>> mapDataList = new ArrayList<>(thatMapSegNum);
                thatMapMetaData.setMapDataList(mapDataList);

                for (int i = 0; i < thatMapSegNum; i++) {
                    int dataNum = thatMapMetaData.getDataNum(i);
                    Map<String, Integer> mapData = new HashMap<>(dataNum);
                    mapDataList.add(mapData);
                    for (int j = 0; j < dataNum; j++) {
                        String key = input.readString();
                        Integer val = input.readInt();
                        mapData.put(key, val);
                    }
                }
            } catch (IOException e) {
                LOG.error("double array read exception", e);
                System.exit(1);
            }
            return thatMapMetaData;
        }
    }

    public static class Mp4jIntArrayReduceSerializer extends Serializer<ArrayMetaData<int[]>> {
        ArrayMetaData<int[]> arrayMetaData;
        ArrayMetaData<int[]> thatArrMetaData;
        IIntOperator operator;

        public Mp4jIntArrayReduceSerializer(ArrayMetaData<int[]> arrayMetaData, IIntOperator operator) {
            this.setAcceptsNull(true);
            this.arrayMetaData = arrayMetaData;
            this.operator = operator;
        }

        public ArrayMetaData<int[]> getThatArrMetaData() {
            return thatArrMetaData;
        }

        public void write(Kryo kryo, Output output, ArrayMetaData<int[]> object) {
        }

        public ArrayMetaData<int[]> read(Kryo kryo, Input input, Class<ArrayMetaData<int[]>> type) {
            try {
                int[] arrData = arrayMetaData.getArrData();
                thatArrMetaData = arrayMetaData.recv(input);
                int arrSegNum = thatArrMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = thatArrMetaData.getFrom(i);
                    int to = thatArrMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        arrData[j] = operator.apply(arrData[j], input.readInt());
                    }
                }
                thatArrMetaData.setArrData(arrData);
            } catch (IOException e) {
                LOG.error("double array read exception", e);
                System.exit(1);
            }
            return thatArrMetaData;
        }
    }

    public static class Mp4jIntMapReduceSerializer extends Serializer<MapMetaData<Integer>> {
        MapMetaData<Integer> mapMetaData;
        MapMetaData<Integer> thatMapMetaData;
        IIntOperator operator;
        public Mp4jIntMapReduceSerializer(MapMetaData<Integer> mapMetaData, IIntOperator operator) {
            this.setAcceptsNull(true);
            this.mapMetaData = mapMetaData;
            this.operator = operator;
        }

        public MapMetaData<Integer> getThatMapMetaData() {
            return thatMapMetaData;
        }

        public void write(Kryo kryo, Output output, MapMetaData<Integer> object) {
        }

        public MapMetaData<Integer> read(Kryo kryo, Input input, Class<MapMetaData<Integer>> type) {
            try {
                thatMapMetaData = mapMetaData.recv(input);
                int thatMapSegNum = thatMapMetaData.getSegNum();
                List<Map<String, Integer>> thatMapListData = new ArrayList<>(thatMapSegNum);
                List<Integer> thatDataNums = new ArrayList<>(thatMapSegNum);
                for (int i = 0; i < thatMapSegNum; i++) {
                    Map<String, Integer> thisMapData = mapMetaData.getMapDataList().get(i);
                    int dataNum = thatMapMetaData.getDataNum(i);
                    for (int j = 0; j < dataNum; j++) {
                        String key = input.readString();
                        Integer val = input.readInt();

                        Integer thisVal = thisMapData.get(key);
                        if (thisVal == null) {
                            thisMapData.put(key, val);
                        } else {
                            thisMapData.put(key, operator.apply(thisVal, val));
                        }
                    }
                    thatMapListData.add(thisMapData);
                    thatDataNums.add(thisMapData.size());
                }

                thatMapMetaData.setMapDataList(thatMapListData);
                thatMapMetaData.setDataNums(thatDataNums);

            } catch (IOException e) {
                LOG.error("double array read exception", e);
                System.exit(1);
            }
            return thatMapMetaData;
        }
    }

    @Override
    public void send(Output output, MetaData metaData) throws IOException, Mp4jException {
        Kryo kryo = KryoUtils.getKryo();
        try {
            switch (container) {
                case ARRAY:
                    Serializer arrSerializer = new IntOperand.Mp4jIntArraySerializer(metaData.convertToArrayMetaData());
                    if (compress) {
                        arrSerializer = new DeflateSerializer(arrSerializer);
                    }
                    arrSerializer.write(kryo, output, null);
                    break;
                case MAP:
                    Serializer mapSerializer = new IntOperand.Mp4jIntMapSerializer(metaData.convertToMapMetaData());
                    if (compress) {
                        mapSerializer = new DeflateSerializer(mapSerializer);
                    }
                    mapSerializer.write(kryo, output, null);

                    break;
                default:
                    throw new Mp4jException("unsupported container:" + container);
            }
        } catch (Exception e) {
            LOG.error("send exception", e);
            throw new Mp4jException(e);
        } finally {
            output.close();
        }


    }

    @Override
    public MetaData recv(Input input, MetaData metaData) throws IOException, Mp4jException {
        MetaData retMetaData = null;
        Kryo kryo = KryoUtils.getKryo();
        try {
            switch (collective) {
                case GATHER:
                case SCATTER:
                case ALL_GATHER:
                    switch (container) {
                        case ARRAY:
                            Serializer<ArrayMetaData<int[]>> arrSerializer = new IntOperand.Mp4jIntArraySerializer(metaData.convertToArrayMetaData());
                            if (compress) {
                                arrSerializer = new DeflateSerializer(arrSerializer);
                            }
                            retMetaData = arrSerializer.read(kryo, input, null);
                            break;
                        case MAP:
                            Serializer<MapMetaData<Integer>> mapSerializer = new IntOperand.Mp4jIntMapSerializer(metaData.convertToMapMetaData());
                            if (compress) {
                                mapSerializer = new DeflateSerializer(mapSerializer);
                            }
                            retMetaData = mapSerializer.read(kryo, input, null);
                            break;

                        default:
                            throw new Mp4jException("unsupported container:" + container);
                    }
                    break;

                case REDUCE_SCATTER:
                    switch (container) {
                        case ARRAY:
                            Serializer<ArrayMetaData<int[]>> arrSerializer = new IntOperand.Mp4jIntArrayReduceSerializer(metaData.convertToArrayMetaData(), operator);
                            if (compress) {
                                arrSerializer = new DeflateSerializer(arrSerializer);
                            }
                            retMetaData = arrSerializer.read(kryo, input, null);
                            break;
                        case MAP:
                            Serializer<MapMetaData<Integer>> mapSerializer = new IntOperand.Mp4jIntMapReduceSerializer(metaData.convertToMapMetaData(), operator);
                            if (compress) {
                                mapSerializer = new DeflateSerializer(mapSerializer);
                            }
                            retMetaData = mapSerializer.read(kryo, input, null);
                            break;
                        default:
                            throw new Mp4jException("unsupported container:" + container);
                    }
                    break;
                default:
                    throw new Mp4jException("unsupported basic collective:" + collective);
            }
        } catch (Exception e) {
            throw new Mp4jException(e);
        } finally {
            input.close();
        }
        return retMetaData;

    }

    @Override
    public void setOperator(IOperator operator) {
        this.operator = (IIntOperator)operator;
    }

    @Override
    public void threadCopy(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {
        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<int[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<int[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            int[] fromArrayData = fromArrayMetaData.getArrData();
            int[]  toArrayData = toArrayMetaData.getArrData();
            int segNum = fromArrayMetaData.getSegNum();
            for (int i = 0; i < segNum; i++) {
                int from = fromArrayMetaData.getFrom(i) >= 0 ? fromArrayMetaData.getFrom(i) : 0;
                int to = fromArrayMetaData.getTo(i) >= 0 ? fromArrayMetaData.getTo(i) : toArrayData.length;
//                for (int j = from; j < to; j++) {
//                    toArrayData[j] = fromArrayData[j];
//                }
                System.arraycopy(fromArrayData, from, toArrayData, from, to - from);
            }

        } else if (fromMetaData instanceof MapMetaData) {
            toMetaData.setMapDataList(fromMetaData.getMapDataList());
        } else {
            throw new Mp4jException("unknown format metadata!" + fromMetaData);
        }
    }

    @Override
    public void threadArrayAllCopy(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {
        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<int[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<int[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            int[] fromArrayData = fromArrayMetaData.getArrData();
            int[]  toArrayData = toArrayMetaData.getArrData();
            System.arraycopy(fromArrayData, 0, toArrayData, 0, fromArrayData.length);

        } else {
            throw new Mp4jException("threadArrayAllCopy unsupport format metadata!" + fromMetaData);
        }
    }

    @Override
    public void threadMerge(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {
        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<int[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<int[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            int[] fromArrayData = fromArrayMetaData.getArrData();
            int[]  toArrayData = toArrayMetaData.getArrData();
            int segNum = fromArrayMetaData.getSegNum();
            for (int i = 0; i < segNum; i++) {
                int from = fromArrayMetaData.getFrom(i) >= 0 ? fromArrayMetaData.getFrom(i) : 0;
                int to = fromArrayMetaData.getTo(i) >= 0 ? fromArrayMetaData.getTo(i) : toArrayData.length;
//                for (int j = from; j < to; j++) {
//                    toArrayData[j] = fromArrayData[j];
//                }
                System.arraycopy(fromArrayData, from, toArrayData, from, to - from);
            }
            toArrayMetaData.setSum(toArrayMetaData.getSum() + fromArrayMetaData.getSum());
            toArrayMetaData.setSegNum(toArrayMetaData.getSegNum() + fromArrayMetaData.getSegNum());
            toArrayMetaData.getRanks().addAll(fromArrayMetaData.getRanks());
            toArrayMetaData.getSegFroms().addAll(fromArrayMetaData.getSegFroms());
            toArrayMetaData.getSegTos().addAll(fromArrayMetaData.getSegTos());
        } else if (fromMetaData instanceof MapMetaData) {
            MapMetaData fromMapMetaData = fromMetaData.convertToMapMetaData();
            MapMetaData toMapMetaData = toMetaData.convertToMapMetaData();
            toMetaData.setSum(fromMetaData.getSum() + toMetaData.getSum());
            toMetaData.setSegNum(1);
            toMapMetaData.getRanks().addAll(fromMapMetaData.getRanks());

            List<Map<String, Integer>> fromMapDataList = fromMetaData.getMapDataList();
            Map<String, Integer> thisMap = (Map<String, Integer>)toMapMetaData.getMapDataList().get(0);
            for (Map<String, Integer> thatMap : fromMapDataList) {
                for (Map.Entry<String, Integer> entry : thatMap.entrySet()) {
                    thisMap.put(entry.getKey(), entry.getValue());
                }
            }
            toMapMetaData.setDataNums(Arrays.asList(thisMap.size()));
        } else {
            throw new Mp4jException("unknown format metadata!" + fromMetaData);
        }
    }

    @Override
    public void threadReduce(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {

        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<int[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<int[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            int []fromArrayData = fromArrayMetaData.getArrData();
            int []toArrayData = toArrayMetaData.getArrData();
            int segNum = fromArrayMetaData.getSegNum();
            for (int i = 0; i < segNum; i++) {
                int from = fromArrayMetaData.getFrom(i) >= 0 ? fromArrayMetaData.getFrom(i) : 0;
                int to = fromArrayMetaData.getTo(i) >= 0 ? fromArrayMetaData.getTo(i) : toArrayData.length;
                for (int j = from; j < to; j++) {
                    toArrayData[j] = operator.apply(toArrayData[j], fromArrayData[j]);
                }
            }
            toArrayMetaData.setSum(toArrayMetaData.getSum() + fromArrayMetaData.getSum());
            toArrayMetaData.setSegNum(1);
        } else if (fromMetaData instanceof MapMetaData) {
            MapMetaData<Integer> fromMapMetaData = fromMetaData.convertToMapMetaData();
            MapMetaData<Integer> toMapMetaData = toMetaData.convertToMapMetaData();
            List<Map<String, Integer>> fromMapDataList = fromMapMetaData.getMapDataList();
            List<Map<String, Integer>> toMapDataList = toMapMetaData.getMapDataList();
            for (int i = 0; i < fromMapDataList.size(); i++) {
                Map<String, Integer> fromMapData = fromMapDataList.get(i);
                Map<String, Integer> toMapData = toMapDataList.get(i);
                for (Map.Entry<String, Integer> entry : fromMapData.entrySet()) {
                    String key = entry.getKey();
                    Integer val = entry.getValue();
                    Integer toVal = toMapData.get(key);
                    if (toVal == null) {
                        toMapData.put(key, val);
                    } else {
                        toMapData.put(key, operator.apply(val, toVal));
                    }
                }
            }
            toMetaData.setSum(fromMetaData.getSum() + toMetaData.getSum());
        } else {
            throw new Mp4jException("unknown format metadata!" + fromMetaData);
        }

    }
}
