package org.apache.spark.spark_core_2;

import java.io.Serializable;
import java.sql.Timestamp;

public class Record implements Serializable {
	private int UID;
	private int MID;
	private String EVENTYPE;
	private Timestamp TS;
	private long TIME;
	// constructor , getters and setters

//	public Record(String UID, String MID, String Type, String TS) {
//		this.UID = UID;
//		this.MID = MID;
//		this.Type = Type;
//		this.TS = TS;
//	}
	
	public int getUID() {
		return UID;
	}

	public void setUID(int UID) {
		this.UID = UID;
	}
	
	public int getMID() {
		return MID;
	}

	public void setMID(int MID) {
		this.MID = MID;
	}
	
	public String getType() {
		return EVENTYPE;
	}

	public void setType(String EVENTYPE) {
		this.EVENTYPE = EVENTYPE;
	}
	
	public Timestamp getTS() {
		return TS;
	}

	public void setTS(Timestamp TS) {
		this.TS = TS;
	}
	
	public long getTime() {
		return TIME;
	}

	public void setTime(long TIME) {
		this.TIME = TIME;
	}
}
