package org.apache.spark.spark_core_2;

import java.util.HashMap;
import java.util.Map;

public class TVBroadcast implements java.io.Serializable {
	int mediaID;
	String mediaName;
	String channel;
	Map<String, BroadcastTime> btList;
	
	public TVBroadcast (int mediaID, String mediaName, String channel){
		this.mediaID = mediaID;
		this.mediaName = mediaName;
		this.channel = channel;
		this.btList = new HashMap<>();
	}
	
	public void addBroadcast(BroadcastTime bt){
		btList.put(bt.broadcast_day, bt);
	}
	
	public BroadcastTime getBroadcast (String day_week){
		return this.btList.get(day_week);
	}
}
