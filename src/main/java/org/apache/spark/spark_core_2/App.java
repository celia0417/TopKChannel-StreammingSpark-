package org.apache.spark.spark_core_2;

import org.apache.spark.api.java.*;
	

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.jets3t.service.utils.TimeFormatter;
import java.util.*;

public class App {

	public static void addUserActivity(FormatView fv, Map<Integer, FormatUserAcitivity> formatUserActivityMap) {
		// add
		FormatUserAcitivity fua = new FormatUserAcitivity(fv.UID, fv.MID, fv.channel, fv.eventType, fv.TS);
		formatUserActivityMap.put(fv.UID, fua);
	}

	public static void updateUserActivity(FormatView fv, Map<Integer, FormatUserAcitivity> formatUserActivityMap) {
		// update
		formatUserActivityMap.get(fv.UID).lastChannel = fv.channel;
		formatUserActivityMap.get(fv.UID).lastMediaId = fv.MID;
		formatUserActivityMap.get(fv.UID).lastEvenType = fv.eventType;
	}

	public static void delteUserActivity(int UID, Map<Integer, FormatUserAcitivity> formatUserActivityMap) {
		formatUserActivityMap.remove(UID);
	}

	public static void addResult(FormatView fv, Map<String, FormatChannleResult> channelResultMap,
			Map<Integer, FormatMediaResult> mediaResultMap, boolean b) {

		// update channel result
		if (!channelResultMap.containsKey(fv.channel)) {
			FormatChannleResult fcr = new FormatChannleResult(fv.channel, fv.UID, b);
			channelResultMap.put(fv.channel, fcr);
		} else {
			channelResultMap.get(fv.channel).addCount();
			channelResultMap.get(fv.channel).addIncreasingCnt();
			if (b)
				channelResultMap.get(fv.channel).addLinearUser(fv.UID);
			else
				channelResultMap.get(fv.channel).addNonLinearUser(fv.UID);
		}

		// update media result
		if (!mediaResultMap.containsKey(fv.MID)) {
			FormatMediaResult fmr = new FormatMediaResult(fv.MID, fv.mediaName, fv.channel, fv.UID, b);
			mediaResultMap.put(fv.MID, fmr);
		} else {
			mediaResultMap.get(fv.MID).addCount();
			mediaResultMap.get(fv.MID).addIncreasingCnt();
			if (b)
				mediaResultMap.get(fv.MID).addLinearUser(fv.UID);
			else
				mediaResultMap.get(fv.MID).addNonLinearUser(fv.UID);
		}
	}

	public static void deleteResult(FormatView fv, Map<String, FormatChannleResult> channelResultMap,
			Map<Integer, FormatMediaResult> mediaResultMap, boolean b) {
		if (channelResultMap.containsKey(fv.channel)) {
			if (b)
				channelResultMap.get(fv.channel).removeLinearUser(fv.UID);
			else
				channelResultMap.get(fv.channel).removeNonLinearUser(fv.UID);
			channelResultMap.get(fv.channel).deductCount();
		}
		if (mediaResultMap.containsKey(fv.MID)) {
			if (b)
				mediaResultMap.get(fv.MID).removeLinearUser(fv.UID);
			else
				mediaResultMap.get(fv.MID).removeNonLinearUser(fv.UID);
			mediaResultMap.get(fv.MID).deductCount();
		}
	}

	public static void main(String[] args) {
		

		String inputFile = "viewlogs.csv"; // Should be some file on your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// JavaRDD<String> data = sc.textFile(inputFile).cache();

		// format view logs
		JavaRDD<Record> records = sc.textFile(inputFile).map(new Function<String, Record>() {
			public Record call(String line) throws Exception {
				// Here you can use JSON
				// Gson gson = new Gson();
				// gson.fromJson(line, Record.class);
				String[] fields = line.split(",");
				// Record sd = new Record(Long.parseLong(fields[0]),
				// Long.parseLong(fields[1]), fields[2].trim(),
				// Long.parseLong(fields[3]));
				Record sd = new Record();
				sd.setUID(Integer.parseInt(fields[0]));
				sd.setMID(Integer.parseInt(fields[1]));
				sd.setType(fields[2]);

				// set time stamp
				String time = fields[3].trim();
				String modtime = "20" + time.substring(0, 2) + "-" + time.substring(2, 4) + "-" + time.substring(4, 6)
						+ " " + time.substring(6, 8) + ":" + time.substring(8, 10) + ":" + time.substring(10, 12);
				Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(modtime);
				sd.setTS(new Timestamp(date.getTime()));

				// set specific time
				String timeRecord = fields[3].trim();
				String specificTime = timeRecord.substring(timeRecord.length() - 6, timeRecord.length());
				sd.setTime(Long.parseLong(specificTime));
				return sd;
			}
		});

		SQLContext sqlContext = new SQLContext(sc);

		DataFrame schemaViewLogs = sqlContext.createDataFrame(records, Record.class);
		schemaViewLogs.registerTempTable("ViewLog");

		schemaViewLogs.show();

		// static tvshow
		// SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<TVShow> tv = sc.textFile("tvshow.csv").map(new Function<String, TVShow>() {
			public TVShow call(String line) throws Exception {
				String[] parts = line.split(",");
				TVShow tv = new TVShow();
				tv.setId(Integer.parseInt(parts[0].trim()));
				tv.setName(parts[1]);
				tv.setChannel(parts[2]);
				String times[] = parts[3].split("/");
				List<String> list = new ArrayList<>();
				for (String time : times) {
					// BroadcastTime bt = new BroadcastTime();
					// String var[] = time.split("-");
					// bt.start_time = Long.valueOf(var[0] + "00");
					// bt.end_time = Long.valueOf(var[1] + "00");
					// bt.broadcast_day = var[2];
					// System.out.println(bt.start_time + " "+ bt.end_time + "
					// "+bt.broadcast_day);
					// tv.setBroadcastTime(bt);
					list.add(time);
				}
				tv.setBroadcastTime(list);
				tv.setBroadcastDay(parts[4]);
				tv.setType(parts[5]);
				return tv;
			}
		});

		DataFrame schemaTV = sqlContext.createDataFrame(tv, TVShow.class);
		schemaTV.registerTempTable("TV");

		schemaTV.show();

		// find linear and non-linear
		DataFrame combinedDataFrame = sqlContext.sql(
				"SELECT UID, MID, TS, ViewLog.type, time, name, channel, broadcastTime, broadcastDay,TV.type FROM TV INNER JOIN ViewLog ON id = MID ORDER BY TS ASC");
		combinedDataFrame.show();
		combinedDataFrame.printSchema();

		List<FormatView> formatViews = combinedDataFrame.javaRDD().map(new Function<Row, FormatView>() {
			public FormatView call(Row row) {

				FormatView fv = new FormatView(row.getInt(0), row.getInt(1), row.getTimestamp(2), row.getString(3),
						row.getLong(4), row.getString(5), row.getString(6), row.getString(8), row.getString(9));

				List<String> list = row.getList(7);
				for (String time : list) {
					String var[] = time.split("-");

					BroadcastTime bt = new BroadcastTime(Long.valueOf(var[0] + "00"), Long.valueOf(var[1] + "00"),
							var[2]);
					fv.setTime(bt);
				}
				return fv;
			}
		}).collect();

		// tv broadcast
		List<TVBroadcast> tvBroadcast = combinedDataFrame.javaRDD().map(new Function<Row, TVBroadcast>() {
			public TVBroadcast call(Row row) {
				TVBroadcast tv = new TVBroadcast(row.getInt(1), row.getString(5), row.getString(6));

				List<String> list = row.getList(7);
				for (String time : list) {
					String var[] = time.split("-");
					BroadcastTime bt = new BroadcastTime(Long.parseLong(var[0] + "00"), Long.parseLong(var[1] + "00"),
							var[2]);
					tv.addBroadcast(bt);
				}
				return tv;
			}
		}).collect();

		// day of week -> media broadcast time
		Map<Integer, TVBroadcast> tvBroadcastMap = new HashMap<>();
		for (TVBroadcast tvB : tvBroadcast) {
			tvBroadcastMap.put(tvB.mediaID, tvB);
		}

		// linear tv programs tracker
		Set<Integer> linearMedia = new HashSet<>();

		// Channel -> FormatChannleResult
		Map<String, FormatChannleResult> channelResultMap = new HashMap<>();
		// MID -> FormatMediaResult
		Map<Integer, FormatMediaResult> mediaResultMap = new HashMap<>();
		// UID -> UserActivity
		Map<Integer, FormatUserAcitivity> formatUserActivityMap = new HashMap<>();

		for (FormatView fv : formatViews) {

			// find day of week
			Calendar cal = Calendar.getInstance();
			cal.setTime(fv.TS);
			int day = cal.get(Calendar.DAY_OF_WEEK);
			String day_week = "";
			switch (day) {
			case 1:
				day_week = "SUN";
				break;
			case 2:
				day_week = "M";
				break;
			case 3:
				day_week = "T";
				break;
			case 4:
				day_week = "W";
				break;
			case 5:
				day_week = "TH";
				break;
			case 6:
				day_week = "F";
				break;
			case 7:
				day_week = "SAT";
				break;
			}

			linearMedia.add(fv.MID);

			// Update Time for Media in real time
			updateTotalTime(day_week, fv, tvBroadcastMap, channelResultMap, mediaResultMap, linearMedia,
					formatUserActivityMap);

			// check linear or non-linear
			long start_time = fv.broadcastTime.get(day_week).start_time;
			long end_time = fv.broadcastTime.get(day_week).end_time;
						
			if (fv.specificTime > start_time && fv.specificTime < end_time) {
				// linear
				linearMedia.add(fv.MID);
				// start and record
				if (fv.eventType.equals("start") || fv.eventType.equals("record")) {
					if (!formatUserActivityMap.containsKey(fv.UID)) {
						addUserActivity(fv, formatUserActivityMap);
						addResult(fv, channelResultMap, mediaResultMap, true);
					} else {
						// check flipping
						Timestamp lastTime = formatUserActivityMap.get(fv.UID).startWatching;
						Timestamp curTime = fv.TS;
						long dif = Math.abs(lastTime.getTime() - curTime.getTime());
						if (formatUserActivityMap.get(fv.UID).lastMediaId != fv.MID && dif < 10) {
							// flipping deduct linear increasing counter
							deduct(formatUserActivityMap.get(fv.UID), channelResultMap, mediaResultMap);
						} else {
							addTime(formatUserActivityMap.get(fv.UID), fv, channelResultMap, mediaResultMap);
						}
						deleteResult(fv, channelResultMap, mediaResultMap, true);
						updateUserActivity(fv, formatUserActivityMap);
						addResult(fv, channelResultMap, mediaResultMap, true);
					}
				}
			} else {
				// non-linear
				if (fv.eventType.equals("start") || fv.eventType.equals("forward")) {
					if (!formatUserActivityMap.containsKey(fv.UID)) {
						addUserActivity(fv, formatUserActivityMap);
						addResult(fv, channelResultMap, mediaResultMap, false);
					} else {
						deleteResult(fv, channelResultMap, mediaResultMap, false);
						updateUserActivity(fv, formatUserActivityMap);
						addResult(fv, channelResultMap, mediaResultMap, false);
					}
				} else if (fv.eventType.equals("pause")) {
					if (formatUserActivityMap.containsKey(fv.UID)) {
						// check if the user exist to make sure we need to
						// deduct the event
						deleteResult(fv, channelResultMap, mediaResultMap, false);
					}

					/**
					 * delete the user activity for the user who stop IPTV
					 */
					delteUserActivity(fv.UID, formatUserActivityMap);
				}

			}
		}
		List<FormatChannleResult> channelList = new ArrayList<>();
		for (String channel : channelResultMap.keySet()) {
			channelList.add(channelResultMap.get(channel));

		}
		List<FormatMediaResult> mediaList = new ArrayList<>();

		for (Integer mid : mediaResultMap.keySet()) {
			mediaList.add(mediaResultMap.get(mid));
		}

		Collections.sort(channelList, new Comparator<FormatChannleResult>() {

			@Override
			public int compare(FormatChannleResult o1, FormatChannleResult o2) {
				return o2.resCount - o1.resCount;
			}

		});

		Collections.sort(mediaList, new Comparator<FormatMediaResult>() {

			@Override
			public int compare(FormatMediaResult o1, FormatMediaResult o2) {
				return o2.resCount - o1.resCount;
			}

		});

		System.out.println("==================== Channel Result ============================");
		for (FormatChannleResult channel : channelList) {
			System.out.println("Channle " + channel.channel + " View Times: " + channel.resCount
					+ " Linear increasing: " + channel.increasingCnt);

		}

		System.out.println("===================== Media Result ============================");
		for (FormatMediaResult media : mediaList) {
			System.out.println(
					"MediaID: " + media.mediaID + ", Median Name: " + media.mediaName + ", Channel: " + media.channel
							+ ", View Times: " + media.resCount + ", Linear increasing: " + media.increasingCnt);
		}

		// records.foreach(x -> {
		// // read tvshow.json & create dataframe base on that
		//
		// // to see if it's linear or not
		//
		// System.out.println(x.TS.substring(x.TS.length() - 4, x.TS.length()));
		//
		// });

		// DataFrame res = sqlContext.sql("select
		// department,designation,state,sum(costToCompany),count(*) from
		// record_table group by department,designation,state");

		// Function fct = new Function<String,String>() {
		// @Override
		// public String call(String line) throws Exception {
		// return convertLine(line);
		// }
		// };
		//
		// JavaRDD<String> outputRdd = data.map(fct);

		// long numAs = logData.filter(new Function<String, Boolean>() {
		// public Boolean call(String s) { return s.contains("a"); }
		// }).count();
		//
		// long numBs = logData.filter(new Function<String, Boolean>() {
		// public Boolean call(String s) { return s.contains("b"); }
		// }).count();
		//
		// System.out.println("Lines with a: " + numAs + ", lines with b: " +
		// numBs);
	}

	private static void updateTotalTime(String day_week, FormatView fv, Map<Integer, TVBroadcast> tvBroadcastMap,
			Map<String, FormatChannleResult> channelResultMap, Map<Integer, FormatMediaResult> mediaResultMap,
			Set<Integer> linearMedia, Map<Integer, FormatUserAcitivity> formatUserActivityMap) {

		for (Integer mediaId : tvBroadcastMap.keySet()) {
			BroadcastTime broadTime = tvBroadcastMap.get(mediaId).getBroadcast(day_week);
			long end_time = broadTime.end_time;

			if (fv.MID == mediaId && end_time < fv.specificTime && linearMedia.contains(mediaId)) {
				FormatMediaResult fmr = mediaResultMap.get(mediaId);
				if (fmr != null) {
					for (int userID : fmr.linearUserIdList) {
						FormatUserAcitivity userAct = formatUserActivityMap.get(userID);
						// calculate time
						long timeDif = end_time - (userAct.startWatching.getHours() * 10000
								+ userAct.startWatching.getMinutes() * 100 + userAct.startWatching.getSeconds());
						fmr.addTime(timeDif);

						FormatChannleResult fcr = channelResultMap.get(fmr.channel);
						fcr.addTime(timeDif);
					}

					// output result:
					String content = "Channel(" + fmr.channel + ") Media (" + fmr.mediaName + ", " + fmr.mediaID
							+ ") Total Time: " + fmr.totalTime + " Cnt: " + fmr.resCount + " Linear Increasing Cnt: "
							+ fmr.increasingCnt + " Channel total time: " + channelResultMap.get(fmr.channel).totalTime;
					try {
						PrintWriter writer = new PrintWriter(fmr.mediaID + ".txt", "UTF-8");
						writer.println(content);
						writer.close();
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					// update channel and media
					for (int userId : fmr.linearUserIdList) {

						fmr.deductCount();
						fmr.removeLinearUser(userId);

						// update corresponding channel
						FormatChannleResult fcr = channelResultMap.get(fmr.channel);
						fcr.deductCount();
						fcr.totalTime -= fmr.totalTime;
						fcr.removeLinearUser(userId);

						fmr.totalTime = 0;
						fmr.removeLinearUser(userId);
					}

					linearMedia.remove(mediaId);
				}
			}
		}
	}

	private static void addTime(FormatUserAcitivity formatUserAcitivity, FormatView fv,
			Map<String, FormatChannleResult> channelResultMap, Map<Integer, FormatMediaResult> mediaResultMap) {
		long time = Math.abs(formatUserAcitivity.startWatching.getTime() - fv.TS.getTime());
	
		if (channelResultMap.containsKey(formatUserAcitivity.lastChannel)) {
			channelResultMap.get(formatUserAcitivity.lastChannel).addTime(time);
		}
		if (mediaResultMap.containsKey(formatUserAcitivity.lastMediaId)) {
			mediaResultMap.get(formatUserAcitivity.lastMediaId).addTime(time);
		}
	}

	private static void deduct(FormatUserAcitivity formatUserAcitivity,
			Map<String, FormatChannleResult> channelResultMap, Map<Integer, FormatMediaResult> mediaResultMap) {
		if (channelResultMap.containsKey(formatUserAcitivity.lastChannel)) {
			channelResultMap.get(formatUserAcitivity.lastChannel).deductIncreasingCnt();
			channelResultMap.get(formatUserAcitivity.lastChannel).removeLinearUser(formatUserAcitivity.UID);
		}
		if (mediaResultMap.containsKey(formatUserAcitivity.lastMediaId)) {
			mediaResultMap.get(formatUserAcitivity.lastMediaId).deductIncreasingCnt();
			mediaResultMap.get(formatUserAcitivity.lastMediaId).removeLinearUser(formatUserAcitivity.UID);
		}
	}
}
