package com.cloudwick.hadoop.WordFilterDynamic;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordFilterDynamicMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] parts = value.toString().split(",");

		if (parts.length != 3) {
			return;
		}

		String filter = context.getConfiguration().get("state");

		if (parts[1].equals(filter)) {
			context.write(new Text(parts[0] + "," + parts[1] + "," + parts[2]),
					new Text());
		}
	}
}