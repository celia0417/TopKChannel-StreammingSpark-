package org.apache.spark.spark_core_2;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Maps;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class Test {

	public static void main(String args[]) {

		SparkConf conf = new SparkConf().setAppName("Spark Top K").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10));
		jssc.remember(Durations.minutes(10));

		// SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<TVShow> tv = sc.textFile("hdfs://tdma4/user/weixin/tvshow.csv")
//		JavaRDD<TVShow> tv = sc.textFile("tvshow.csv")
				.map(new Function<String, TVShow>() {
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
							// System.out.println(bt.start_time + " "+
							// bt.end_time + "
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

		// spark streaming from kafka
		HashMap<String, String> kafkaParams = Maps.newHashMap();

		kafkaParams.put("metadata.broker.list", "tdma3:9092");
		kafkaParams.put("auto.offset.reset", "smallest");

		Set<String> topicsSet = new HashSet<>();
		topicsSet.add("viewlogs_2");

		// accumulator all the result

		// channel
		Accumulator<Map<String, FormatChannleResult>> accChannelResultMap = jssc.sparkContext().accumulator(

				new HashMap<String, FormatChannleResult>(), "channelResultMap", new MapAccumulatorChannel());

		// media
		Accumulator<Map<Integer, FormatMediaResult>> accMediaResultMap = jssc.sparkContext()

				.accumulator(new HashMap<>(), "mediaResultMap", new MapAccumulatorMedia());

		// user
		Accumulator<Map<Integer, FormatUserAcitivity>> accFormatUserActivityMap = jssc.sparkContext()

				.accumulator(new HashMap<>(), "formatUserActivityMap", new MapAccumulatorUser());

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		JavaDStream<String> lines = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				return tuple2._2();
			}
		});

		lines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
			
			@Override
			public Void call(JavaRDD<String> rdd, Time time) {

				// Get the singleton instance of SQLContext
				SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());

				JavaRDD<Record> records = rdd.map(new Function<String, Record>() {
					public Record call(String line) throws Exception {
						// System.out.println(line);
						// Here you can use JSON
						// Gson gson = new Gson();
						// gson.fromJson(line, Record.class);
						System.out.println("------------------" + line + "====================");
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
						String modtime = "20" + time.substring(0, 2) + "-" + time.substring(2, 4) + "-"
								+ time.substring(4, 6) + " " + time.substring(6, 8) + ":" + time.substring(8, 10) + ":"
								+ time.substring(10, 12);
						Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(modtime);
						sd.setTS(new Timestamp(date.getTime()));

						// set specific time
						String timeRecord = fields[3].trim();
						String specificTime = timeRecord.substring(timeRecord.length() - 6, timeRecord.length());
						sd.setTime(Long.parseLong(specificTime));
						return sd;
					}
				});

				// // Convert RDD[String] to RDD[case class] to DataFrame
				// JavaRDD<JavaRow> rowRDD = rdd.map(new Function<String,
				// JavaRow>() {
				// public JavaRow call(String word) {
				// JavaRow record = new JavaRow();
				// record.setWord(word);
				// return record;
				// }
				// });
				// DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD,
				// JavaRow.class);

				// Register as table
				// wordsDataFrame.registerTempTable("words");

				DataFrame schemaViewLogs = sqlContext.createDataFrame(records, Record.class);
				schemaViewLogs.registerTempTable("ViewLog");

				schemaViewLogs.show();

				DataFrame schemaTV = sqlContext.createDataFrame(tv, TVShow.class);
				schemaTV.registerTempTable("TV");

				schemaTV.show();

				// Do word count on table using SQL and print it
				// DataFrame wordCountsDataFrame =
				// sqlContext.sql("select word, count(*) as total from words
				// group by word");
				// wordCountsDataFrame.show();

				// find linear and non-linear
				DataFrame combinedDataFrame = sqlContext.sql(
						"SELECT UID, MID, TS, ViewLog.type, time, name, channel, broadcastTime, broadcastDay,TV.type FROM TV INNER JOIN ViewLog ON id = MID ORDER BY TS ASC");
				combinedDataFrame.show();
				combinedDataFrame.printSchema();

				List<FormatView> formatViews = combinedDataFrame.javaRDD().map(new Function<Row, FormatView>() {
					public FormatView call(Row row) {

						FormatView fv = new FormatView(row.getInt(0), row.getInt(1), row.getTimestamp(2),
								row.getString(3), row.getLong(4), row.getString(5), row.getString(6), row.getString(8),
								row.getString(9));

						List<String> list = row.getList(7);
						for (String time : list) {
							String var[] = time.split("-");

							BroadcastTime bt = new BroadcastTime(Long.valueOf(var[0] + "00"),
									Long.valueOf(var[1] + "00"), var[2]);
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
							BroadcastTime bt = new BroadcastTime(Long.parseLong(var[0] + "00"),
									Long.parseLong(var[1] + "00"), var[2]);
							tv.addBroadcast(bt);
						}
						return tv;
					}
				}).collect();

				Map<Integer, TVBroadcast> tvBroadcastMap = new HashMap<>();
				// day of week -> media broadcast time

				// tvBroadcast.foreachPartition(new
				// VoidFunction<Iterator<TVBroadcast>>() {
				// @Override
				// public void call(Iterator<TVBroadcast> it) throws Exception {
				// Map<Integer, TVBroadcast> tvBroadcastMap = new HashMap<>();
				//
				// while(it.hasNext()){
				//
				// }
				// }
				//
				// });

				for (TVBroadcast tv : tvBroadcast) {
					tvBroadcastMap.put(tv.mediaID, tv);
				}

				// linear tv programs tracker
				Set<Integer> linearMedia = new HashSet<>();

				// Channel -> FormatChannleResult
				Map<String, FormatChannleResult> channelResultMap = accChannelResultMap.value();

				// MID -> FormatMediaResult
				Map<Integer, FormatMediaResult> mediaResultMap = accMediaResultMap.value();
				// UID -> UserActivity
				Map<Integer, FormatUserAcitivity> formatUserActivityMap = accFormatUserActivityMap.value();

				for (FormatView fv : formatViews) {
//				formatViews.foreach((fv) -> {
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
								// check if the user exist to make sure we need
								// to
								// deduct the event
								deleteResult(fv, channelResultMap, mediaResultMap, false);
							}

							/**
							 * delete the user activity for the user who stop
							 * IPTV
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

				// output result:
				String content = "";
				for (FormatChannleResult channel : channelList) {
					content += channel.channel + "," + channel.resCount + "\n";

				}

//				try {
//					Path pt = new Path("hdfs://tdma4/user/weixin/ChannelResult.txt");
//					FileSystem fs;
//					fs = FileSystem.get(new Configuration());
//					BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
//					System.out.println(content);
//					br.write(content);
//					br.close();
//
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}

				// PrintWriter writer = new PrintWriter("ChannelResult.txt",
				// "UTF-8");
				// writer.println(content);
				// writer.close();

				System.out.println("===================== Media Result ============================");
				for (FormatMediaResult media : mediaList) {
					System.out.println("MediaID: " + media.mediaID + ", Median Name: " + media.mediaName + ", Channel: "
							+ media.channel + ", View Times: " + media.resCount + ", Linear increasing: "
							+ media.increasingCnt);
				}
				
				

				return null;
			}
		});

		jssc.start();
		jssc.awaitTermination();
	}

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

	private static void updateTotalTime(String day_week, FormatView fv, Map<Integer, TVBroadcast> tvBroadcastMap,
			Map<String, FormatChannleResult> channelResultMap, Map<Integer, FormatMediaResult> mediaResultMap,
			Set<Integer> linearMedia, Map<Integer, FormatUserAcitivity> formatUserActivityMap) {
		System.out.println("update===");
		for (Integer mediaId : tvBroadcastMap.keySet()) {
			System.out.println("update2===");
			BroadcastTime broadTime = tvBroadcastMap.get(mediaId).getBroadcast(day_week);
			long end_time = broadTime.end_time;
			System.out.println("end_time " + end_time);
			System.out.println("specificTime " + fv.specificTime);
			if (fv.MID == mediaId && end_time < fv.specificTime && linearMedia.contains(mediaId)) {
				System.out.println("update3===");

				FormatMediaResult fmr = mediaResultMap.get(mediaId);
				if (fmr != null) {
					for (int userID : fmr.linearUserIdList) {
						System.out.println("update4===");

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

//					try {
//						Path pt = new Path("hdfs://tdma4/user/weixin/" + fmr.mediaID + ".txt");
//						FileSystem fs;
//						fs = FileSystem.get(new Configuration());
//						BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
//						System.out.println(content);
//						br.write(content);
//						br.close();
//					} catch (IOException e1) {
//						// TODO Auto-generated catch block
//						e1.printStackTrace();
//					}

					// try {
					// PrintWriter writer = new PrintWriter(fmr.mediaID +
					// ".txt", "UTF-8");
					// writer.println(content);
					// writer.close();
					// } catch (FileNotFoundException e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// } catch (UnsupportedEncodingException e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// }

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

/** Lazily instantiated singleton instance of SQLContext */

class JavaSQLContextSingleton {
	private static transient SQLContext instance = null;

	public static SQLContext getInstance(SparkContext sparkContext) {
		if (instance == null) {
			instance = new SQLContext(sparkContext);
		}
		return instance;
	}
}
