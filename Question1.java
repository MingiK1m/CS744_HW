package com.group29.partc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Question1 {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(0);
		
		DataStream<Tuple4<Integer,Integer,Long,String>> stream = 
				env.addSource(DataSource.create());
		
		WindowedStream data = stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer,Integer,Long,String>>() {

					@Override
					public long extractAscendingTimestamp(Tuple4<Integer, Integer, Long, String> arg0) {
						return arg0.f2;
					}
				})
				.keyBy(3)
				.window(TumblingEventTimeWindows.of(Time.minutes(1)));
//				.apply(new WindowFunction<Tuple4<Integer,Integer,Long,String>, String, Tuple, TimeWindow>() {
//
//					@Override
//					public void apply(Tuple arg0, TimeWindow arg1,
//							Iterable<Tuple4<Integer, Integer, Long, String>> arg2, Collector<String> arg3)
//							throws Exception {
//						// TODO Auto-generated method stub
//						
//					}
//				});
		
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
