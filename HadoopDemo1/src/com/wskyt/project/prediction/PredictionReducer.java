package com.wskyt.project.prediction;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictionReducer extends Reducer<Text, Text, DoubleWritable, DoubleWritable> {
	//构造邻接表
	private double TP;
	private double FP;
	private double FN;
	private double TN;
	
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
		int countTP = 0,countFN = 0;
		for(Text value : values ){
			if(key.toString().equals(value.toString())){
				countTP++;
			}else{
				countFN++;
			}
		}
		TP = countTP;
		FN = countFN;
		context.write(new DoubleWritable(TP), new DoubleWritable(FN));
	}
}
