package com.wskyt.project.conditionprobability;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class MyRecordReader extends RecordReader<Text,IntWritable> {
	
	private Configuration conf;
	private FileSplit split;//待处理的分片
	private FSDataInputStream fileIn;
	
	private IntWritable value = null;
	private Text key = null;
	private Text keytmp = null;
	private String keyStr;
	
	//用于读每个文本并记录进度
	private long start;
	private long end;
	private long pos;
	private LineReader in;
	
	public MyRecordReader() throws IOException {	
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		split = (FileSplit)genericSplit;
		conf = context.getConfiguration();
		
//		start = split.getStart();
//		end = start+split.getLength();
//		context.getConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", true);
		Path path = split.getPath();

		
		FileSystem fs = path.getFileSystem(conf);
		fileIn = fs.open(path);
		in = new LineReader(fileIn,conf);
//		in.readLine(keytmp);
//		System.out.println("keytmp="+keytmp);
//		System.out.println("len="+keytmp.getLength());
//		pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(value == null){
			value = new IntWritable(1);
			System.out.println("value="+value);
		}
		if(key == null){
//			keyStr = split.getPath().getName()+","+keytmp.toString();
//			System.out.println("keystr="+keyStr);
			keytmp = new Text();
			key = new Text();
		}
		if(in.readLine(keytmp) == 0){
//			System.out.println("false");
			return false;
		}
//		keyStr = split.getPath().getParent().getName()+"_"+keytmp.toString();
		keyStr = keytmp.toString();
		System.out.println("keystr="+keyStr);
		key.set(keyStr);
//		pos++;
		return true;
}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	      if (in != null) {  
	        in.close();  
	      }  	    
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}
}
