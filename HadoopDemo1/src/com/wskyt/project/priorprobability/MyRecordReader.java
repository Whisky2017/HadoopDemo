package com.wskyt.project.priorprobability;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class MyRecordReader extends RecordReader<Text,IntWritable>{
	
	private Configuration configuration;
	private boolean processed;//当前文件是否已被读取
	private FileSplit split;//待处理的分片
	private Path filePath;
	
	private Text key = new Text();
	private IntWritable value = new IntWritable();
	
	public MyRecordReader(InputSplit genericSplit,TaskAttemptContext context) throws IOException {	
		this.processed = false;
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
		this.split = (FileSplit)genericSplit;
//		Configuration job = context.getConfiguration();
//		context.getConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", true);
		Path path = this.split.getPath();
		context.getConfiguration().set("map.input.file.name", path.getName());

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(!processed){
			this.key.set(split.getPath().getParent().getName());
			this.value.set(1);
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}
}
