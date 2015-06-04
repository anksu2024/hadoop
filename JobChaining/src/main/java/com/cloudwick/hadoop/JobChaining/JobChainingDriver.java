package com.cloudwick.hadoop.JobChaining;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobChainingDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] "
					+ "<input dir> <output Directory>\n", getClass()
					.getSimpleName());
			return -1;
		}

		Job job;

		// == MAPPER 1 ==
		job = new Job(getConf());
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(
				"/cloudwick/jobchaining/temp"));

		job.setMapperClass(Mapper1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		if (job.waitForCompletion(true)) {
		}

		// == MAPPER 2 ==
		job = new Job(getConf());
		job.setJarByClass(Mapper2.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(
				"/cloudwick/jobchaining/temp"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Mapper2.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		if (job.waitForCompletion(true)) {
		}
		return 1;
	}

	public static void main(String... args) throws Exception {
		int exitCode = ToolRunner.run(new JobChainingDriver(), args);
		System.exit(exitCode);
	}
}