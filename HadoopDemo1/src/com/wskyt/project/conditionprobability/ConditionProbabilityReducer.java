package com.wskyt.project.conditionprobability;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConditionProbabilityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		int count = 0;
		for (IntWritable intWritable : values) {
			count++;
		}
		System.out.println("key2="+key);
		System.out.println("count="+count);
		context.write(key,new IntWritable(count));
		System.out.println("Reducer done!");
	}
}
