package com.wskyt.project.prediction;

import com.wskyt.project.prediction.MyRecordReader;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  


public class MyInputFormat extends FileInputFormat<Text, BytesWritable>{
	
	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new MyRecordReader();
	}
	
	//�ж��Ƿ��Ƭ������ÿ���ļ�һƬ�����Բ���Ƭ
	@Override
	protected boolean isSplitable(JobContext context,Path filename){
		return false;
	}
}
