package com.cloudwick.hadoop.LogCounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class CounterReducer extends Reducer<Text, Text, Text, LongWritable> {
	private Counter counterError;
	private Counter counterWarning;
	private Counter counterInfo;

	@Override
	protected void setup(Reducer<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		counterError = context.getCounter(DEBUG_COUNTER.ERROR);
		counterWarning = context.getCounter(DEBUG_COUNTER.WARNING);
		counterInfo = context.getCounter(DEBUG_COUNTER.INFO);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		switch (key.toString()) {
		case "ERROR":
			context.write(key, new LongWritable(counterError.getValue()));
			break;
		case "WARNING":
			context.write(key, new LongWritable(counterWarning.getValue()));
			break;
		default:
			context.write(key, new LongWritable(counterInfo.getValue()));
		}
	}
}