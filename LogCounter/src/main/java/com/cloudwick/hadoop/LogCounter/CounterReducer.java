package com.cloudwick.hadoop.LogCounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.log.Log;

public class CounterReducer extends Reducer<Text, Text, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {

		Log.info("ANKITHADOOPREDUCE " + key.toString());

		if (key.toString().equalsIgnoreCase("INFO")) {
			context.write(key, new LongWritable(Driver.counterInfo.getValue()));
		} else if (key.toString().equalsIgnoreCase("WARNING")) {
			context.write(key, new LongWritable(Driver.counterWarning.getValue()));
		} else if (key.toString().equalsIgnoreCase("ERROR")) {
			context.write(key, new LongWritable(Driver.counterInfo.getValue()));
		}
	}
}