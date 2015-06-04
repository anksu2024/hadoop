package com.cloudwick.hadoop.TableJoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Text;

public class TableJoinDriver {
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] "
					+ "<input dir> <output Directory>\n", getClass()
					.getSimpleName());
			return -1;
		}

		Job job = new Job(getConf());

		// Mapper Details
		job.setMapperClass(TableJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reducer details
		job.setReducerClass(TableJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Configuration of Input Out paths on HDFS
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (job.waitForCompletion(true)) {
		}

		return 0;
	}

	public static void main(String... args) throws Exception {
		int exitCode = ToolRunner.run(new TableJoinDriver(), args);
		System.exit(exitCode);
	}
}