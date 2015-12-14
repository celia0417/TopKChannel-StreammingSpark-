package org.apache.spark.spark_core_2;


import java.util.*;

public class FormatChannleResult implements java.io.Serializable{
	String channel;
	Set<Integer> linearUserIdList;
	Set<Integer> nonLinearUserIdList;
	int resCount;
	int increasingCnt;
	long totalTime;
	
	public FormatChannleResult(String channel, int UID, boolean b){
		this.channel = channel;
		resCount = 1;
		increasingCnt = 1;
		linearUserIdList = new HashSet<>();
		nonLinearUserIdList = new HashSet<>();
		totalTime = 0;
		if (b) addLinearUser(UID);
		else addNonLinearUser(UID);
	}
	
	public void addCount(){
		resCount++;
	}
	
	public void deductCount(){
		if (resCount > 0 )
			resCount--;
	}
	
	public void addIncreasingCnt(){
		increasingCnt ++;
	}
	
	public void deductIncreasingCnt(){
		increasingCnt--;
	}
	
	public void addLinearUser(int UID) {
		linearUserIdList.add(UID);
	}

	public void removeLinearUser(int UID) {
		linearUserIdList.remove(UID);
	}
	
	public void addNonLinearUser(int UID) {
		nonLinearUserIdList.add(UID);
	}

	public void removeNonLinearUser(int UID) {
		nonLinearUserIdList.remove(UID);
	}
	
	public void addTime (long dif){
		totalTime += dif;
	}
}
