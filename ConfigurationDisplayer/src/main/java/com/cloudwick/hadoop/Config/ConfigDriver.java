package com.cloudwick.hadoop.Config;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigDriver extends Configured implements Tool {
	public static void main(String ... args) throws Exception {
		int exitCode = ToolRunner.run(new ConfigDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		for(Entry<String, String> entry : conf) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		
		return 0;
	}
}