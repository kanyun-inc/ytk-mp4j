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
public class StringOperand extends Operand {
    public static final Logger LOG = LoggerFactory.getLogger(StringOperand.class);
    public IStringOperator operator;


    public static class Mp4jStringArraySerializer extends Serializer<ArrayMetaData<String[]>> {
        ArrayMetaData<String[]> arrayMetaData;
        ArrayMetaData<String[]> thatArrMetaData;
        public Mp4jStringArraySerializer(ArrayMetaData<String[]> arrayMetaData) {
            this.setAcceptsNull(true);
            this.arrayMetaData = arrayMetaData;
        }

        public ArrayMetaData<String[]> getThatArrMetaData() {
            return thatArrMetaData;
        }

        public void write(Kryo kryo, Output output, ArrayMetaData<String[]> object) {
            try {
                String[] arrData = arrayMetaData.getArrData();
                arrayMetaData.send(output);
                int arrSegNum = arrayMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = arrayMetaData.getFrom(i);
                    int to = arrayMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        output.writeString(arrData[j]);
                    }
                }
            } catch (IOException e) {
                LOG.error("double array write exception", e);
                System.exit(1);
            }
        }

        public ArrayMetaData<String[]> read(Kryo kryo, Input input, Class<ArrayMetaData<String[]>> type) {
            try {
                String[] arrData = arrayMetaData.getArrData();
                thatArrMetaData = arrayMetaData.recv(input);
                int arrSegNum = thatArrMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = thatArrMetaData.getFrom(i);
                    int to = thatArrMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        arrData[j] = input.readString();
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

    public static class Mp4jStringMapSerializer extends Serializer<MapMetaData<String>> {
        MapMetaData<String> mapMetaData;
        MapMetaData<String> thatMapMetaData;

        public Mp4jStringMapSerializer(MapMetaData<String> mapMetaData) {
            this.setAcceptsNull(true);
            this.mapMetaData = mapMetaData;
        }

        public MapMetaData<String> getThatMapMetaData() {
            return thatMapMetaData;
        }

        public void write(Kryo kryo, Output output, MapMetaData<String> object) {
            try {
                List<Map<String, String>> mapDataList = mapMetaData.getMapDataList();
                mapMetaData.send(output);
                int mapSegNum = mapMetaData.getSegNum();
                for (int i = 0; i < mapSegNum; i++) {
                    Map<String, String> mapData = mapDataList.get(i);
                    for (Map.Entry<String, String> entry : mapData.entrySet()) {
                        output.writeString(entry.getKey());
                        output.writeString(entry.getValue());
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
        public MapMetaData<String> read(Kryo kryo, Input input, Class<MapMetaData<String>> type) {
            try {
                thatMapMetaData = mapMetaData.recv(input);
                int thatMapSegNum = thatMapMetaData.getSegNum();
                List<Map<String, String>> mapDataList = new ArrayList<>(thatMapSegNum);
                thatMapMetaData.setMapDataList(mapDataList);

                for (int i = 0; i < thatMapSegNum; i++) {
                    int dataNum = thatMapMetaData.getDataNum(i);
                    Map<String, String> mapData = new HashMap<>(dataNum);
                    mapDataList.add(mapData);
                    for (int j = 0; j < dataNum; j++) {
                        String key = input.readString();
                        String val = input.readString();
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

    public static class Mp4jStringArrayReduceSerializer extends Serializer<ArrayMetaData<String[]>> {
        ArrayMetaData<String[]> arrayMetaData;
        ArrayMetaData<String[]> thatArrMetaData;
        IStringOperator operator;

        public Mp4jStringArrayReduceSerializer(ArrayMetaData<String[]> arrayMetaData, IStringOperator operator) {
            this.setAcceptsNull(true);
            this.arrayMetaData = arrayMetaData;
            this.operator = operator;
        }

        public ArrayMetaData<String[]> getThatArrMetaData() {
            return thatArrMetaData;
        }

        public void write(Kryo kryo, Output output, ArrayMetaData<String[]> object) {
        }

        public ArrayMetaData<String[]> read(Kryo kryo, Input input, Class<ArrayMetaData<String[]>> type) {
            try {
                String[] arrData = arrayMetaData.getArrData();
                thatArrMetaData = arrayMetaData.recv(input);
                int arrSegNum = thatArrMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = thatArrMetaData.getFrom(i);
                    int to = thatArrMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        arrData[j] = operator.apply(arrData[j], input.readString());
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

    public static class Mp4jStringMapReduceSerializer extends Serializer<MapMetaData<String>> {
        MapMetaData<String> mapMetaData;
        MapMetaData<String> thatMapMetaData;
        IStringOperator operator;
        public Mp4jStringMapReduceSerializer(MapMetaData<String> mapMetaData, IStringOperator operator) {
            this.setAcceptsNull(true);
            this.mapMetaData = mapMetaData;
            this.operator = operator;
        }

        public MapMetaData<String> getThatMapMetaData() {
            return thatMapMetaData;
        }

        public void write(Kryo kryo, Output output, MapMetaData<String> object) {
        }

        public MapMetaData<String> read(Kryo kryo, Input input, Class<MapMetaData<String>> type) {
            try {
                thatMapMetaData = mapMetaData.recv(input);
                int thatMapSegNum = thatMapMetaData.getSegNum();
                List<Map<String, String>> thatMapListData = new ArrayList<>(thatMapSegNum);
                List<Integer> thatDataNums = new ArrayList<>(thatMapSegNum);
                for (int i = 0; i < thatMapSegNum; i++) {
                    Map<String, String> thisMapData = mapMetaData.getMapDataList().get(i);
                    int dataNum = thatMapMetaData.getDataNum(i);
                    for (int j = 0; j < dataNum; j++) {
                        String key = input.readString();
                        String val = input.readString();

                        String thisVal = thisMapData.get(key);
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
                    Serializer arrSerializer = new StringOperand.Mp4jStringArraySerializer(metaData.convertToArrayMetaData());
                    if (compress) {
                        arrSerializer = new DeflateSerializer(arrSerializer);
                    }
                    arrSerializer.write(kryo, output, null);
                    break;
                case MAP:
                    Serializer mapSerializer = new StringOperand.Mp4jStringMapSerializer(metaData.convertToMapMetaData());
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
                            Serializer<ArrayMetaData<String[]>> arrSerializer = new StringOperand.Mp4jStringArraySerializer(metaData.convertToArrayMetaData());
                            if (compress) {
                                arrSerializer = new DeflateSerializer(arrSerializer);
                            }
                            retMetaData = arrSerializer.read(kryo, input, null);
                            break;
                        case MAP:
                            Serializer<MapMetaData<String>> mapSerializer = new StringOperand.Mp4jStringMapSerializer(metaData.convertToMapMetaData());
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
                            Serializer<ArrayMetaData<String[]>> arrSerializer = new StringOperand.Mp4jStringArrayReduceSerializer(metaData.convertToArrayMetaData(), operator);
                            if (compress) {
                                arrSerializer = new DeflateSerializer(arrSerializer);
                            }
                            retMetaData = arrSerializer.read(kryo, input, null);
                            break;
                        case MAP:
                            Serializer<MapMetaData<String>> mapSerializer = new StringOperand.Mp4jStringMapReduceSerializer(metaData.convertToMapMetaData(), operator);
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
        this.operator = (IStringOperator)operator;
    }

    @Override
    public void threadCopy(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {
        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<String[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<String[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            String[] fromArrayData = fromArrayMetaData.getArrData();
            String[]  toArrayData = toArrayMetaData.getArrData();
            int segNum = fromArrayMetaData.getSegNum();
            for (int i = 0; i < segNum; i++) {
                int from = fromArrayMetaData.getFrom(i) >= 0 ? fromArrayMetaData.getFrom(i) : 0;
                int to = fromArrayMetaData.getTo(i) >= 0 ? fromArrayMetaData.getTo(i) : toArrayData.length;
                for (int j = from; j < to; j++) {
                    toArrayData[j] = fromArrayData[j];
                }
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
            ArrayMetaData<String[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<String[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            String[] fromArrayData = fromArrayMetaData.getArrData();
            String[]  toArrayData = toArrayMetaData.getArrData();
            System.arraycopy(fromArrayData, 0, toArrayData, 0, fromArrayData.length);

        } else {
            throw new Mp4jException("threadArrayAllCopy unsupport format metadata!" + fromMetaData);
        }
    }

    @Override
    public void threadMerge(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {
        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<String[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<String[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            String[] fromArrayData = fromArrayMetaData.getArrData();
            String[]  toArrayData = toArrayMetaData.getArrData();
            int segNum = fromArrayMetaData.getSegNum();
            for (int i = 0; i < segNum; i++) {
                int from = fromArrayMetaData.getFrom(i) >= 0 ? fromArrayMetaData.getFrom(i) : 0;
                int to = fromArrayMetaData.getTo(i) >= 0 ? fromArrayMetaData.getTo(i) : toArrayData.length;
                for (int j = from; j < to; j++) {
                    toArrayData[j] = fromArrayData[j];
                }
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

            List<Map<String, String>> fromMapDataList = fromMetaData.getMapDataList();
            Map<String, String> thisMap = (Map<String, String>)toMapMetaData.getMapDataList().get(0);
            for (Map<String, String> thatMap : fromMapDataList) {
                for (Map.Entry<String, String> entry : thatMap.entrySet()) {
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
            ArrayMetaData<String[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<String[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            String []fromArrayData = fromArrayMetaData.getArrData();
            String []toArrayData = toArrayMetaData.getArrData();
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
            MapMetaData<String> fromMapMetaData = fromMetaData.convertToMapMetaData();
            MapMetaData<String> toMapMetaData = toMetaData.convertToMapMetaData();
            List<Map<String, String>> fromMapDataList = fromMapMetaData.getMapDataList();
            List<Map<String, String>> toMapDataList = toMapMetaData.getMapDataList();
            for (int i = 0; i < fromMapDataList.size(); i++) {
                Map<String, String> fromMapData = fromMapDataList.get(i);
                Map<String, String> toMapData = toMapDataList.get(i);
                for (Map.Entry<String, String> entry : fromMapData.entrySet()) {
                    String key = entry.getKey();
                    String val = entry.getValue();
                    String toVal = toMapData.get(key);
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
