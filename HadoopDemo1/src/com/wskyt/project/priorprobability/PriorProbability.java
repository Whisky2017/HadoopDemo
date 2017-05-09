package com.wskyt.project.priorprobability;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PriorProbability {
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.171.129:9000");
		conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
		FileSystem fs = FileSystem.get(conf);
//		FileStatus[] files = fs.listStatus(new Path(args[0]));
//		for (FileStatus f:files) {
//			System.out.println(f.toString());
//		}
		
		Job job = Job.getInstance(conf,"priorprobability");
		job.setInputFormatClass(MyInputFormat.class);
		job.setJarByClass(PriorProbability.class);
		job.setMapperClass(PriorProbabilityMapper.class);
		job.setCombinerClass(PriorProbabilityReducer.class);
		job.setReducerClass(PriorProbabilityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
//		System.out.println("job done!");
//		FSDataInputStream in = fs.open(new Path("hdfs://192.168.171.129:9000/output/part-r-00000"));
//		IOUtils.copyBytes(in, System.out,4096,true);
	}
}
