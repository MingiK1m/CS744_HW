package com.group29.partc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Question1 {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<String, Integer>> stream = 
				env.addSource(DataSource.create())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer,Integer,Long,String>>() {

					@Override
					public long extractAscendingTimestamp(Tuple4<Integer, Integer, Long, String> arg0) {
						return arg0.f2;
					}
				})
				.flatMap(new FlatMapFunction<Tuple4<Integer,Integer,Long,String>, Tuple2<String, Integer>>(){

					@Override
					public void flatMap(Tuple4<Integer, Integer, Long, String> tuple, Collector<Tuple2<String, Integer>> out)
							throws Exception {
						out.collect(new Tuple2<String,Integer>(tuple.f3, 1));
					}

				})
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.sum(1);
		
		System.out.println("++ Print ----");
		stream.print();
		System.out.println("-- Print ----");
		
		env.execute();
	}

	/**
	streaming simulation part
	*/
	private static class DataSource extends RichSourceFunction<Tuple4<Integer, Integer, Long, String>> {

		private volatile boolean running = true;
		private final String filename = "/home/ubuntu/assignment2/partc/higgs-activity_time.txt";

		private DataSource() {

		}

		public static DataSource create() {
			return new DataSource();
		}

		@Override
		public void run(SourceContext<Tuple4<Integer, Integer, Long, String>> ctx) throws Exception {

			try{
				final File file = new File(filename);
				final BufferedReader br = new BufferedReader(new FileReader(file));

				String line = "";

				System.out.println("Start read data from \"" + filename + "\"");
				long count = 0L;
				while(running && (line = br.readLine()) != null) {
					if ((count++) % 10 == 0) {
						Thread.sleep(1);
					}
					ctx.collect(genTuple(line));
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		private Tuple4<Integer, Integer, Long, String> genTuple(String line) {
			String[] item = line.split(" ");
			Tuple4<Integer, Integer, Long, String> record = new Tuple4<>();

			record.setField(Integer.parseInt(item[0]), 0);
			record.setField(Integer.parseInt(item[1]), 1);
			record.setField(Long.parseLong(item[2]), 2);
			record.setField(item[3], 3);

			return record;
		}
	}
}
