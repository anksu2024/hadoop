package com.cloudwick.hadoop.DistributedCache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Map<Integer, String> deptMap;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.getLocal(conf);

		Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);

		// [0] because we added just one file.
		BufferedReader cacheReader = new BufferedReader(new InputStreamReader(
				fs.open(dataFile[0])));

		// Read the dept file and populate the deptMap
		deptMap = new HashMap<Integer, String>();
		String line;
		while ((line = cacheReader.readLine()) != null) {
			String[] parts = line.split(",");
			deptMap.put(Integer.parseInt(parts[0]), parts[1]);
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split(",");

		Text mapperKey = new Text("<" + parts[2] + ","
				+ deptMap.get(Integer.parseInt(parts[2])) + ">");
		Text mapperValue = new Text("<" + parts[0] + "," + parts[1] + ">");

		context.write(mapperKey, mapperValue);
	}
}