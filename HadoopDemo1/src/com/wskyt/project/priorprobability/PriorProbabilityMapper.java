package com.wskyt.project.priorprobability;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PriorProbabilityMapper extends Mapper<Text,IntWritable,Text,IntWritable> {

	@Override
	public void map(Text key,IntWritable value,Context context) throws IOException,InterruptedException{
		System.out.println("key1="+key);
		System.out.println("value1="+value);
		context.write(key, value);
//		System.out.println("Mapper done!");
	}
}
