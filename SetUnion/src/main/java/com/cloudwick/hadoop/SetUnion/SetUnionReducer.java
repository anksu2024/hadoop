package com.cloudwick.hadoop.SetUnion;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SetUnionReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(key, new Text());
	}
}