package org.apache.spark.spark_core_2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.AccumulatorParam;

public class MapAccumulatorUser implements AccumulatorParam<Map<Integer, FormatUserAcitivity>>, Serializable {

    @Override
    public Map<Integer, FormatUserAcitivity> addAccumulator(Map<Integer, FormatUserAcitivity> t1, Map<Integer, FormatUserAcitivity> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<Integer, FormatUserAcitivity> addInPlace(Map<Integer, FormatUserAcitivity> r1, Map<Integer, FormatUserAcitivity> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<Integer, FormatUserAcitivity> zero(final Map<Integer, FormatUserAcitivity> initialValue) {
        return new HashMap<>();
    }

    private Map<Integer, FormatUserAcitivity> mergeMap( Map<Integer, FormatUserAcitivity> map1, Map<Integer, FormatUserAcitivity> map2) {
        Map<Integer, FormatUserAcitivity> result = new HashMap<>(map1);
        map2.forEach((key, value) -> {
        	result.get(key).lastChannel = value.lastChannel;
        	result.get(key).lastEvenType = value.lastEvenType;        	
        	result.get(key).startWatching = value.startWatching;
        });
        return result;
    }

}