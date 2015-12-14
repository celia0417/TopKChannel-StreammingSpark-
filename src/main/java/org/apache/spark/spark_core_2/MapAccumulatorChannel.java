package org.apache.spark.spark_core_2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.AccumulatorParam;

public class MapAccumulatorChannel implements AccumulatorParam<Map<String, FormatChannleResult>>, Serializable {

    @Override
    public Map<String, FormatChannleResult> addAccumulator(Map<String, FormatChannleResult> t1, Map<String, FormatChannleResult> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<String, FormatChannleResult> addInPlace(Map<String, FormatChannleResult> r1, Map<String, FormatChannleResult> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<String, FormatChannleResult> zero(final Map<String, FormatChannleResult> initialValue) {
        return new HashMap<>();
    }

    private Map<String, FormatChannleResult> mergeMap( Map<String, FormatChannleResult> map1, Map<String, FormatChannleResult> map2) {
        Map<String, FormatChannleResult> result = new HashMap<>(map1);
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