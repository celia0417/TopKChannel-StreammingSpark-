package org.apache.spark.spark_core_2;

import java.sql.Timestamp;

public class FormatUserAcitivity implements java.io.Serializable {
	int UID;
	int lastMediaId;
	String lastChannel;

	// TODO:
	Timestamp startWatching;
	String lastEvenType;

	public FormatUserAcitivity(int UID, int lastMediaId, String lastChannel, String lastEventType, Timestamp startWatching) {
		this.UID = UID;
		this.lastMediaId = lastMediaId;
		this.lastChannel = lastChannel;
		this.lastEvenType = lastEventType;
		this.startWatching = startWatching;
	}
}
