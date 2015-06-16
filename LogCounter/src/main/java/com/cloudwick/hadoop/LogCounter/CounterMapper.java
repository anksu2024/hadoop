package com.cloudwick.hadoop.LogCounter;

import java.io.IOException;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<LongWritable, Text, Text, Text> {
	// private final Log LOG = LogFactory.getLog(Driver.class);
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		if (value.toString().startsWith("INFO:")) {
			Driver.counterInfo.increment(1);
			context.write(new Text("INFO"), new Text(""));
		} else if (value.toString().startsWith("WARNING:")) {
			Driver.counterWarning.increment(1);
			context.write(new Text("WARNING"), new Text(""));
		} else if (value.toString().startsWith("ERROR:")) {
			Driver.counterError.increment(1);
			context.write(new Text("ERROR"), new Text(""));
		}
	}
}