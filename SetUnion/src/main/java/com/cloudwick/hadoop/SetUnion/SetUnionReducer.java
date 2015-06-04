package com.cloudwick.hadoop.SetUnion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SetUnionReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	Set<String> playerNames;

	@Override
	protected void reduce(IntWritable id, Iterable<Text> names,
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		playerNames = new HashSet<String>();

		for (Text name : names) {
			playerNames.add(name.toString().trim());
		}

		StringBuilder combinedPlayers = new StringBuilder("");
		for (String playerName : playerNames) {
			combinedPlayers.append(playerName + ",");
		}

		// Remove Last ","
		combinedPlayers.deleteCharAt(combinedPlayers.length() - 1);

		context.write(id, new Text(combinedPlayers.toString()));
	}
}