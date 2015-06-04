package com.cloudwick.hadoop.JobChaining;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobChainingMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	private Log LOG = LogFactory.getLog(JobChainingDriver.class);

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		LOG.info(value.toString());
		
		String parts[] = value.toString().split(",");

		int salary = Integer.parseInt(parts[3]);
		if (salary >= 100000 && salary <= 130000) {
			context.write(value, new Text(""));
		}
	}
}