package org.apache.spark.spark_core_2;

import java.sql.Timestamp;
import java.util.*;

public class FormatView implements java.io.Serializable {
	int UID;
	int MID;
	Timestamp TS;
	String eventType;
	long specificTime;
	String mediaName;
	Map<String, BroadcastTime> broadcastTime;
	String channel;
	String broadcastDay;
	String mediaType;


	public FormatView(int UID, int MID, Timestamp TS, String eventType, long specificTime, String mediaName, String channel, String broadcastDay, String mediaType){
		this.UID = UID;
		this.MID = MID;
		this.TS = TS;
		this.eventType = eventType;
		this.specificTime = specificTime;
		this.mediaName = mediaName;
		this.channel = channel;
		this.broadcastDay = broadcastDay;
		this.mediaType = mediaType;
		this.broadcastTime = new HashMap<>();
	}
	
	public void setTime(BroadcastTime bt){
		this.broadcastTime.put(bt.broadcast_day, bt);
	}
}
