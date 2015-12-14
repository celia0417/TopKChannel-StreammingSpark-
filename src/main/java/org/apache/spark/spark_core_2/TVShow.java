package org.apache.spark.spark_core_2;

import java.io.Serializable;
import java.util.*;

public class TVShow implements Serializable {
	private int id;
	private String name;
	private String channel;
//	private Map<String,BroadcastTime> broadcastTime = new HashMap<String, BroadcastTime>();
	private List<String> broadcastTime = new ArrayList<>();
//	private long start_time;
//	private long end_time;
	private String broadcast_day;
	private String media_type;

	// public TVShow(String id, String name, String channel, String
	// broadcast_time, String broadcast_day, String type) {
	// this.id = id;
	// this.name = name;
	// this.channel = channel;
	// this.broadcast_time = broadcast_time;
	// this.broadcast_day = broadcast_day;
	// this.type = type;
	// }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public List<String> getBroadcastTime() {
		return broadcastTime;
	}

	public void setBroadcastTime(List<String> bt) {
		this.broadcastTime = bt;
	}
	
	
	public String getBroadcastDay() {
		return broadcast_day;
	}

	public void setBroadcastDay(String broadcast_day) {
		this.broadcast_day = broadcast_day;
	}
	
	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public String getType() {
		return media_type;
	}

	public void setType(String media_type) {
		this.media_type = media_type;
	}
}