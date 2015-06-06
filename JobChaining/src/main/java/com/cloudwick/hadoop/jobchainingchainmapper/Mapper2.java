package com.cloudwick.hadoop.jobchainingchainmapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<IntWritable, Text, Text, Text> {
	@Override
	protected void map(IntWritable key, Text value,
			Mapper<IntWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int salary = key.get();
		String finalOutput = value.toString() + "," + key;
		if (salary >= 100000 && salary <= 130000) {
			context.write(new Text(finalOutput), new Text());
		}
	}
}