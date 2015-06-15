package com.cloudwick.hadoop.LogCounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Counter counterError;
	private Counter counterWarning;
	private Counter counterInfo;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		counterError = context.getCounter(DEBUG_COUNTER.ERROR);
		counterWarning = context.getCounter(DEBUG_COUNTER.WARNING);
		counterInfo = context.getCounter(DEBUG_COUNTER.INFO);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (value.toString().startsWith("ERROR:")) {
			counterError.increment(1);
		} else if (value.toString().startsWith("WARNING:")) {
			counterWarning.increment(1);
		} else {
			// if(value.toString().startsWith("INFO:"))
			counterInfo.increment(1);
		}
	}
}