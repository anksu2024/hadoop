package com.cloudwick.hadoop.TableJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableJoinReducer extends Reducer<IntWritable, Text, Text, Text> {
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,
			Reducer<IntWritable, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		
	}
}
