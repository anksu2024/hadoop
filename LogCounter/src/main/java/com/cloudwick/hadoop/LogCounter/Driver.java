package com.cloudwick.hadoop.LogCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
	public static Counter counterError;
	public static Counter counterWarning;
	public static Counter counterInfo;
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] "
					+ "<Log File Input Path> <Output Path>\n",
					getClass().getSimpleName());
			return -1;
		}

		// Configuration
		Configuration conf = new Configuration();
		
		// Job definition
		Job job = Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.setJobName("Sample Counter");

		// Mapper Details
		job.setMapperClass(CounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reducer Details
		job.setReducerClass(CounterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// Configuration of Output paths on HDFS
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String... args) throws Exception {
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}
}