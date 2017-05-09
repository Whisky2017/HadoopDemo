package com.wskyt.test;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MaxTemperature {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
//        configuration.set("mapred.jar", "./out/artifacts/maxtemperatrue/maxtemperature.jar"); //运行前程序要被打成JAR包
        configuration.set("fs.defaultFS", "hdfs://192.168.171.129:9000");
        FileSystem fs = FileSystem.get(configuration);
//        if (args.length != 2){
//            System.err.println("Usage:MaxTemperature <input path> <output path>");
//            System.exit(-1);
//        }
//        Job job = Job.getInstance(configuration,"max temperature");
//        job.setJarByClass(MaxTemperature.class);
//        job.setMapperClass(MaxTemperatureMapper.class);
//        job.setCombinerClass(MaxTemperatureReducer.class);
//        job.setReducerClass(MaxTemperatureReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        FSDataInputStream in = fs.open(new Path("hdfs://192.168.171.129:9000/output/part-r-00000"));
//        InputStream in = new URL(otherArgs[1]).openStream();
        IOUtils.copyBytes(in, System.out,4096,true);
//        System.exit(job.waitForCompletion(true)?0:1);
    }
}

