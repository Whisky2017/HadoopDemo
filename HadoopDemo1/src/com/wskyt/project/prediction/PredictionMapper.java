package com.wskyt.project.prediction;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredictionMapper extends Mapper<Text, BytesWritable, Text, Text> {
	@Override
	public void map(Text key,BytesWritable value,Context context) throws IOException,InterruptedException{
		Text valuenew = new Text();
		String content = new String(value.getBytes(), 0, value.getLength());
		double Pc1 = Prediction.conditionProbabilityForClass(content, "UK");
		double Pc2 = Prediction.conditionProbabilityForClass(content, "USA");
		if(Pc1 > Pc2){
			valuenew.set("UK");
		}else if(Pc1 < Pc2){
			valuenew.set("USA");
		}else if(Pc1 == Pc2){
			valuenew.set("WRONG!");
		}
		System.out.println("key="+key);
		System.out.println("value="+valuenew);
		context.write(key, valuenew);
		System.out.println("Mapper done!");
	}
}
