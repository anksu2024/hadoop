package com.cloudwick.hadoop.WordCount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> counts,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		int counter = 0;

		Iterator<IntWritable> iterator = counts.iterator();
		while (iterator.hasNext()) {
			counter += 1;
			iterator.next();
		}

		System.out.println(key + ":" + counter);
		context.write(key, new IntWritable(counter));
	}
}