package com.wskyt.project.prediction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyRecordReader extends RecordReader<Text,BytesWritable> {
	
	private Configuration conf;
	private FileSplit split;//待处理的分片
	
	private BytesWritable value = null;
	private Text key = null;
	
	//是否已读
	private boolean processed = false;
	
	public MyRecordReader() throws IOException {	
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
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
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(!processed){
			value = new BytesWritable();
			int len = (int) split.getLength();
			byte[] content = new byte[len];
			Path path = split.getPath();
			key = new Text(split.getPath().getParent().getName());
			FileSystem fs = path.getFileSystem(conf);
			FSDataInputStream in = null;
			try{
				in = fs.open(path);
				IOUtils.readFully(in, content, 0, len);
				value.set(content,0,len);
			}finally{
				if( in != null){
					in.close();
				}
			}
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
