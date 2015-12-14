package org.apache.spark.spark_core_2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.AccumulatorParam;

public class MapAccumulatorMedia implements AccumulatorParam<Map<Integer, FormatMediaResult>>, Serializable {

    @Override
    public Map<Integer, FormatMediaResult> addAccumulator(Map<Integer, FormatMediaResult> t1, Map<Integer, FormatMediaResult> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<Integer, FormatMediaResult> addInPlace(Map<Integer, FormatMediaResult> r1, Map<Integer, FormatMediaResult> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<Integer, FormatMediaResult> zero(final Map<Integer, FormatMediaResult> initialValue) {
        return new HashMap<>();
    }

    private Map<Integer, FormatMediaResult> mergeMap( Map<Integer, FormatMediaResult> map1, Map<Integer, FormatMediaResult> map2) {
        Map<Integer, FormatMediaResult> result = new HashMap<>(map1);
        map2.forEach((key, value) -> {
        	result.get(key).increasingCnt += value.increasingCnt;
        	result.get(key).resCount += value.resCount;        	
        	result.get(key).totalTime += value.totalTime;
        	for (int id : value.linearUserIdList){
        		result.get(key).linearUserIdList.add(id);
        	}
        	
        	for (int id : value.nonLinearUserIdList){
        		result.get(key).nonLinearUserIdList.add(id);
        	}
        });
        return result;
    }

}