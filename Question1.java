package com.group29.partc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Question1 {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(0);
		
		DataStream<Tuple4<Integer,Integer,Long,String>> stream = 
				env.addSource(DataSource.create());
		
		
		DataStream<String> result = 
				stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer,Integer,Long,String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public long extractAscendingTimestamp(Tuple4<Integer, Integer, Long, String> arg0) {
						return arg0.f2;
					}
				})
				.keyBy(new KeySelector<Tuple4<Integer,Integer,Long,String>, String>() {

					@Override
					public String getKey(Tuple4<Integer, Integer, Long, String> arg0) throws Exception {
						return arg0.f3;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.apply(new WindowFunction<Tuple4<Integer,Integer,Long,String>, String, String, TimeWindow>() {

					@Override
					public void apply(String key, TimeWindow window,
							Iterable<Tuple4<Integer, Integer, Long, String>> input, Collector<String> output)
							throws Exception {
						int count = 0;
						for(Tuple4<Integer, Integer, Long, String> in : input){
							count++;
						}
						if(count > 100){
							String outputStr = "";
							outputStr += window + " Count: " + count + " Type: " + key;
							output.collect(outputStr);
						}
					}
				});
		
		result.print();
		
		env.execute();
	}

	/**
	streaming simulation part
	*/
	private static class DataSource extends RichSourceFunction<Tuple4<Integer, Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		
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
