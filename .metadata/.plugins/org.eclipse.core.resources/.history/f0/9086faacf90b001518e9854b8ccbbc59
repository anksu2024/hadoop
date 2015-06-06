package com.cloudwick.hadoop.JobChaining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String parts[] = value.toString().split(",");

		if (parts[1].equals("CA")) {
			context.write(new IntWritable(Integer.parseInt(parts[3].trim())),
					new Text(parts[0].trim() + "," + parts[1].trim() + ","
							+ parts[2].trim()));
		}
	}
}

/*
 * 1,CA,seema,50000
 * 2,NY,rahul,70000
 * 3,TX,jitin,95000
 * 4,NJ,aditya,97000
 * 5,VI,arya,70000
 */