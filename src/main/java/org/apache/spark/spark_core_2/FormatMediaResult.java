package org.apache.spark.spark_core_2;

import java.util.HashSet;
import java.util.Set;

public class FormatMediaResult implements java.io.Serializable{
	int mediaID;
	String mediaName;
	String channel;
	Set<Integer> linearUserIdList;
	Set<Integer> nonLinearUserIdList;

	int resCount;
	int increasingCnt;
	long totalTime;

	public FormatMediaResult(int mediaID, String mediaName, String channel, int UID, boolean flag) {
		this.mediaID = mediaID;
		this.mediaName = mediaName;
		this.channel = channel;
		linearUserIdList = new HashSet<>();
		nonLinearUserIdList = new HashSet<>();
		resCount = 1;
		increasingCnt = 1;
		totalTime = 0;
		if (flag) addLinearUser(UID);
		else addNonLinearUser(UID);
	}

	public void addCount() {
		resCount++;
	}

	public void deductCount(){
		if (resCount > 0)
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
