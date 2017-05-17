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
import com.fenbi.mp4j.operator.Collective;
import com.fenbi.mp4j.operator.IDoubleOperator;
import com.fenbi.mp4j.operator.IOperator;
import com.fenbi.mp4j.utils.KryoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.*;

/**
 * @author xialong
 */
public class DoubleOperand extends Operand {
    public static final Logger LOG = LoggerFactory.getLogger(DoubleOperand.class);
    public IDoubleOperator operator;

    public static class Mp4jDoubleArraySerializer extends Serializer<ArrayMetaData<double[]>> {
        ArrayMetaData<double[]> arrayMetaData;
        ArrayMetaData<double[]> thatArrMetaData;
        public Mp4jDoubleArraySerializer(ArrayMetaData<double[]> arrayMetaData) {
            this.setAcceptsNull(true);
            this.arrayMetaData = arrayMetaData;
        }

        public ArrayMetaData<double[]> getThatArrMetaData() {
            return thatArrMetaData;
        }

        public void write(Kryo kryo, Output output, ArrayMetaData<double[]> object) {
            try {
                double[] arrData = arrayMetaData.getArrData();
                arrayMetaData.send(output);
                int arrSegNum = arrayMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = arrayMetaData.getFrom(i);
                    int to = arrayMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        output.writeDouble(arrData[j]);
                    }
                }
            } catch (IOException e) {
                LOG.error("double array write exception", e);
                System.exit(1);
            }
        }

        public ArrayMetaData<double[]> read(Kryo kryo, Input input, Class<ArrayMetaData<double[]>> type) {
            try {
                double[] arrData = arrayMetaData.getArrData();
                thatArrMetaData = arrayMetaData.recv(input);
                int arrSegNum = thatArrMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = thatArrMetaData.getFrom(i);
                    int to = thatArrMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        arrData[j] = input.readDouble();
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

    public static class Mp4jDoubleMapSerializer extends Serializer<MapMetaData<Double>> {
        MapMetaData<Double> mapMetaData;
        MapMetaData<Double> thatMapMetaData;

        public Mp4jDoubleMapSerializer(MapMetaData<Double> mapMetaData) {
            this.setAcceptsNull(true);
            this.mapMetaData = mapMetaData;
        }

        public MapMetaData<Double> getThatMapMetaData() {
            return thatMapMetaData;
        }

        public void write(Kryo kryo, Output output, MapMetaData<Double> object) {
            try {
                List<Map<String, Double>> mapDataList = mapMetaData.getMapDataList();
                mapMetaData.send(output);
                int mapSegNum = mapMetaData.getSegNum();
                for (int i = 0; i < mapSegNum; i++) {
                    Map<String, Double> mapData = mapDataList.get(i);
                    for (Map.Entry<String, Double> entry : mapData.entrySet()) {
                        output.writeString(entry.getKey());
                        output.writeDouble(entry.getValue());
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
        public MapMetaData<Double> read(Kryo kryo, Input input, Class<MapMetaData<Double>> type) {
            try {
                thatMapMetaData = mapMetaData.recv(input);
                int thatMapSegNum = thatMapMetaData.getSegNum();
                List<Map<String, Double>> mapDataList = new ArrayList<>(thatMapSegNum);
                thatMapMetaData.setMapDataList(mapDataList);

                for (int i = 0; i < thatMapSegNum; i++) {
                    int dataNum = thatMapMetaData.getDataNum(i);
                    Map<String, Double> mapData = new HashMap<>(dataNum);
                    mapDataList.add(mapData);
                    for (int j = 0; j < dataNum; j++) {
                        String key = input.readString();
                        Double val = input.readDouble();
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

    public static class Mp4jDoubleArrayReduceSerializer extends Serializer<ArrayMetaData<double[]>> {
        ArrayMetaData<double[]> arrayMetaData;
        ArrayMetaData<double[]> thatArrMetaData;
        IDoubleOperator operator;

        public Mp4jDoubleArrayReduceSerializer(ArrayMetaData<double[]> arrayMetaData, IDoubleOperator operator) {
            this.setAcceptsNull(true);
            this.arrayMetaData = arrayMetaData;
            this.operator = operator;
        }

        public ArrayMetaData<double[]> getThatArrMetaData() {
            return thatArrMetaData;
        }

        public void write(Kryo kryo, Output output, ArrayMetaData<double[]> object) {
        }

        public ArrayMetaData<double[]> read(Kryo kryo, Input input, Class<ArrayMetaData<double[]>> type) {
            try {
                double[] arrData = arrayMetaData.getArrData();
                thatArrMetaData = arrayMetaData.recv(input);
                int arrSegNum = thatArrMetaData.getSegNum();
                for (int i = 0; i < arrSegNum; i++) {
                    int from = thatArrMetaData.getFrom(i);
                    int to = thatArrMetaData.getTo(i);
                    for (int j = from; j < to; j++) {
                        arrData[j] = operator.apply(arrData[j], input.readDouble());
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

    public static class Mp4jDoubleMapReduceSerializer extends Serializer<MapMetaData<Double>> {
        MapMetaData<Double> mapMetaData;
        MapMetaData<Double> thatMapMetaData;
        IDoubleOperator operator;
        public Mp4jDoubleMapReduceSerializer(MapMetaData<Double> mapMetaData, IDoubleOperator operator) {
            this.setAcceptsNull(true);
            this.mapMetaData = mapMetaData;
            this.operator = operator;
        }

        public MapMetaData<Double> getThatMapMetaData() {
            return thatMapMetaData;
        }

        public void write(Kryo kryo, Output output, MapMetaData<Double> object) {
        }

        public MapMetaData<Double> read(Kryo kryo, Input input, Class<MapMetaData<Double>> type) {
            try {
                thatMapMetaData = mapMetaData.recv(input);
                int thatMapSegNum = thatMapMetaData.getSegNum();
                List<Map<String, Double>> thatMapListData = new ArrayList<>(thatMapSegNum);
                List<Integer> thatDataNums = new ArrayList<>(thatMapSegNum);
                for (int i = 0; i < thatMapSegNum; i++) {
                    Map<String, Double> thisMapData = mapMetaData.getMapDataList().get(i);
                    int dataNum = thatMapMetaData.getDataNum(i);
                    for (int j = 0; j < dataNum; j++) {
                        String key = input.readString();
                        Double val = input.readDouble();

                        Double thisVal = thisMapData.get(key);
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
                    Serializer arrSerializer = new Mp4jDoubleArraySerializer(metaData.convertToArrayMetaData());
                    if (compress) {
                        arrSerializer = new DeflateSerializer(arrSerializer);
                    }

                    arrSerializer.write(kryo, output, null);
                    break;
                case MAP:
                    Serializer mapSerializer = new Mp4jDoubleMapSerializer(metaData.convertToMapMetaData());
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
                            Serializer<ArrayMetaData<double[]>> arrSerializer = new Mp4jDoubleArraySerializer(metaData.convertToArrayMetaData());
                            if (compress) {
                                arrSerializer = new DeflateSerializer(arrSerializer);
                            }
                            retMetaData = arrSerializer.read(kryo, input, null);
                            break;
                        case MAP:
                            Serializer<MapMetaData<Double>> mapSerializer = new Mp4jDoubleMapSerializer(metaData.convertToMapMetaData());
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
                            Serializer<ArrayMetaData<double[]>> arrSerializer = new Mp4jDoubleArrayReduceSerializer(metaData.convertToArrayMetaData(), operator);
                            if (compress) {
                                arrSerializer = new DeflateSerializer(arrSerializer);
                            }
                            retMetaData = arrSerializer.read(kryo, input, null);
                            break;
                        case MAP:
                            Serializer<MapMetaData<Double>> mapSerializer = new Mp4jDoubleMapReduceSerializer(metaData.convertToMapMetaData(), operator);
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
        this.operator = (IDoubleOperator)operator;
    }

    @Override
    public void threadCopy(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {
        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<double[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<double[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            double[] fromArrayData = fromArrayMetaData.getArrData();
            double[]  toArrayData = toArrayMetaData.getArrData();
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
            ArrayMetaData<double[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<double[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            double[] fromArrayData = fromArrayMetaData.getArrData();
            double[]  toArrayData = toArrayMetaData.getArrData();
            System.arraycopy(fromArrayData, 0, toArrayData, 0, fromArrayData.length);

        } else {
            throw new Mp4jException("threadArrayAllCopy unsupport format metadata!" + fromMetaData);
        }
    }

    @Override
    public void threadMerge(MetaData fromMetaData, MetaData toMetaData) throws Mp4jException {
        if (fromMetaData instanceof ArrayMetaData) {
            ArrayMetaData<double[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<double[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            double[] fromArrayData = fromArrayMetaData.getArrData();
            double[]  toArrayData = toArrayMetaData.getArrData();
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

            List<Map<String, Double>> fromMapDataList = fromMetaData.getMapDataList();
            Map<String, Double> thisMap = (Map<String, Double>)toMapMetaData.getMapDataList().get(0);
            for (Map<String, Double> thatMap : fromMapDataList) {
                for (Map.Entry<String, Double> entry : thatMap.entrySet()) {
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
            ArrayMetaData<double[]> fromArrayMetaData = fromMetaData.convertToArrayMetaData();
            ArrayMetaData<double[]> toArrayMetaData = toMetaData.convertToArrayMetaData();
            double[] fromArrayData = fromArrayMetaData.getArrData();
            double[] toArrayData = toArrayMetaData.getArrData();
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
            MapMetaData<Double> fromMapMetaData = fromMetaData.convertToMapMetaData();
            MapMetaData<Double> toMapMetaData = toMetaData.convertToMapMetaData();
            List<Map<String, Double>> fromMapDataList = fromMapMetaData.getMapDataList();
            List<Map<String, Double>> toMapDataList = toMapMetaData.getMapDataList();
            for (int i = 0; i < fromMapDataList.size(); i++) {
                Map<String, Double> fromMapData = fromMapDataList.get(i);
                Map<String, Double> toMapData = toMapDataList.get(i);
                for (Map.Entry<String, Double> entry : fromMapData.entrySet()) {
                    String key = entry.getKey();
                    Double val = entry.getValue();
                    Double toVal = toMapData.get(key);
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
