package org.apache.spark.spark_core_2;

public class BroadcastTime implements java.io.Serializable{
	long start_time;
	long end_time;
	String broadcast_day;
	
	public BroadcastTime(long start_time, long end_time, String broadcast_day){
		this.start_time = start_time;
		this.end_time = end_time;
		this.broadcast_day = broadcast_day;
	}
}
