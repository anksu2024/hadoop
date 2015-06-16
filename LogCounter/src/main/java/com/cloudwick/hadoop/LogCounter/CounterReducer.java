package com.cloudwick.hadoop.LogCounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.log.Log;

public class CounterReducer extends Reducer<Text, Text, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {

		Log.info("ANKITHADOOPREDUCE " + key.toString());

		Counter counter;

		if (key.toString().equalsIgnoreCase("INFO")) {
			counter = context.getCounter(DEBUG_COUNTER.INFO);
			Log.info("INFO : " + counter);
			context.write(key, new LongWritable(counter.getValue()));
		} else if (key.toString().equalsIgnoreCase("WARNING")) {
			counter = context.getCounter(DEBUG_COUNTER.WARNING);
			Log.info("WARNING : " + counter);
			context.write(key, new LongWritable(counter.getValue()));
		} else if (key.toString().equalsIgnoreCase("ERROR")) {
			// For ERROR
			counter = context.getCounter(DEBUG_COUNTER.ERROR);
			Log.info("ERROR : " + counter);
			context.write(key, new LongWritable(counter.getValue()));
		}
	}
}