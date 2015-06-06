package com.cloudwick.hadoop.SetUnion;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SetUnionMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	private Log LOG = LogFactory.getLog(SetUnionDriver.class);

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String playerDetails[] = value.toString().split(",");
		
		IntWritable id = new IntWritable(Integer.parseInt(playerDetails[0]
				.trim()));
		LOG.info("$$HADOOP$$2: " + id.get() + ":" + playerDetails[1]);
		context.write(id, new Text(playerDetails[1].trim()));
		
	}
}