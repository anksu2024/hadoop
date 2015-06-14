package com.cloudwick.hadoop.DistributedCache;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
	private Log LOG = LogFactory.getLog(Driver.class);

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: %s [generic options] "
					+ "<Emp Input Path> <Output Path> <Dept Directory>\n",
					getClass().getSimpleName());
			return -1;
		}

		Configuration conf = new Configuration();

		// Adding the Dept file in the DistributedCache
		DistributedCache.addCacheFile(new URI(args[2]), conf);

		Job job = new Job(conf);
		job.setJarByClass(Driver.class);
		job.setJobName("Table Join");
		job.setNumReduceTasks(0);

		// Mapper Details
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Configuration of Output paths on HDFS
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Set Number of Reducers = 0
		job.setNumReduceTasks(0);

		LOG.info("Reached Here");

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String... args) throws Exception {
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}
}