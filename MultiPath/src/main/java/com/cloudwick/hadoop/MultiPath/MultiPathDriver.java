package com.cloudwick.hadoop.MultiPath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiPathDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: %s [generic options] "
					+ "<input1 dir> <input2 dir> <output Directory>\n",
					getClass().getSimpleName());
			return -1;
		}

		Path firstPath = new Path(args[0]);
		Path secondPath = new Path(args[1]);
		Path thirdPath = new Path(args[2]);

		Configuration conf = new Configuration();
		Job job = new Job(conf);

		job.setJarByClass(MultiPathDriver.class);
		job.setJobName("Multipath Join");

		// Mapper Details
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Using Multipaths Mapper for processing input
		MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class,
				MultiPathMapper1.class);
		MultipleInputs.addInputPath(job, secondPath, TextInputFormat.class,
				MultiPathMapper2.class);

		FileOutputFormat.setOutputPath(job, thirdPath);

		job.setNumReduceTasks(0);

		// Wait for job completion
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String... args) throws Exception {
		int exitCode = ToolRunner.run(new MultiPathDriver(), args);
		System.exit(exitCode);
	}
}
