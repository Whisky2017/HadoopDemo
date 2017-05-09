package com.wskyt.project.prediction;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.wskyt.project.prediction.MyInputFormat;
import com.wskyt.project.prediction.PredictionMapper;
import com.wskyt.project.prediction.PredictionReducer;

public class Prediction {
	//先验概率
	public static double Nc1;
	public static double Nc2;
	public static double N;
	public static double Pc1;
	public static double Pc2;
	
	//条件概率
	public static double Tkc1;
	public static double Tkc2;
	public static double Tc1;//UK term总数
	public static double Tc2;//USA term总数
	public static double V;//c1、c2中不同单词个数
	
	//字典
	public static Hashtable<String, Double> table2 = new Hashtable<>();//UK(C1字典)
	public static Hashtable<String, Double> table3 = new Hashtable<>();//USA(C2字典)
	
	private static String path1 = "H:\\hadoopdata\\mp1";
	private static String path2 = "H:\\hadoopdata\\mp2";
	private static String path3 = "H:\\hadoopdata\\mp3";
		
	public static double conditionProbabilityForClass(String content,String className) throws FileNotFoundException{
		InputStream inputStream1 = new FileInputStream(new File(path1));
		InputStream inputStream2 = new FileInputStream(new File(path2));
		InputStream inputStream3 = new FileInputStream(new File(path3));
		Scanner scanner1 = new Scanner(inputStream1);
		Scanner scanner2 = new Scanner(inputStream2);
		Scanner scanner3 = new Scanner(inputStream3);
		String line1 =null;
		String line2 =null;
		String line3 =null;
		Hashtable<String, Double> table1 = new Hashtable<>();
		while(scanner1.hasNextLine()){
			line1 = scanner1.nextLine();
			String key_value1[] = line1.split("	");
//			System.out.println("key="+key_value[0]);
//			System.out.println("value="+key_value[1]);
			String key1 = key_value1[0];
			Double value1 = Double.parseDouble(key_value1[1]);
			table1.put(key1, value1);
		}
		scanner1.close();
//		System.out.println(table1);
		Nc1 = table1.get("UK");
		Nc2 = table1.get("USA");
		N = Nc1 + Nc2;
		Pc1 = Nc1/N;
		Pc2 = Nc2/N;
//		System.out.println("Pc1="+Pc1+" Pc2="+Pc2);
		
		int count = 0;
		double sumTc1 = 0;//UK term总数
		double sumTc2 = 0;//USA term总数
//		int counttmp1=0,counttmp2=0;
		while(scanner2.hasNextLine()){
			line2 = scanner2.nextLine();
			String key_value2[] = line2.split("	");
			String key_value3[] = key_value2[0].split("_");
			if(key_value3[0].equals("UK")){
				String key2 = key_value3[1];
				Double value2 = Double.parseDouble(key_value2[1]);
				table2.put(key2, value2);
				sumTc1 = sumTc1 + value2;
//				System.out.println("key2="+key2+",value2="+value2);
//				counttmp1++;
			}else if(key_value3[0].equals("USA")){
				String key3 = key_value3[1];
				Double value3 = Double.parseDouble(key_value2[1]);
				table3.put(key3, value3);
				sumTc2 = sumTc2 + value3;
//				System.out.println("key3="+key3+",value3="+value3);
//				counttmp2++;
			}
		}
		scanner2.close();
		Tc1 = sumTc1;
		Tc2 = sumTc2;
//		System.out.println("table2="+table2.size());
//		System.out.println("table3="+table3.size());
//		System.out.println(count);
		while(scanner3.hasNextLine()){
			line3 = scanner3.nextLine();
			count++;
		}
		scanner3.close();
		V = count;
//		System.out.println("V="+V);
		
		return 0;
	}
	
	public static void main(String args[])throws Exception,FileNotFoundException{
		String content = null;
		String className = null;
		Double p1 = conditionProbabilityForClass(content, className);
		
		//宏平均和微平均
		double macroAverage;
		double microAverage;
		
		double TPc1;
		double FPc1;
		double TPc2;
		double FPc2;
		
		String out;
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.171.129:9000");
		conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf,"prediction");
		job.setInputFormatClass(MyInputFormat.class);
		job.setJarByClass(Prediction.class);
		job.setMapperClass(PredictionMapper.class);
		job.setReducerClass(PredictionReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("job done!");
		FSDataInputStream in = fs.open(new Path("hdfs://192.168.171.129:9000/output/part-r-00000"));
		Scanner scanner = new Scanner(in);
		String line = null;
		line = scanner.nextLine();
		String num1[] = line.split("	");
		line = scanner.nextLine();
		String num2[] = line.split("	");
		System.out.println(num1);
		System.out.println(num2);
		
		
	}
}
