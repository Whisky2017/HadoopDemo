import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by 世康 on 2016/10/23.
 */

public class MaxTemperature {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("run.jar", "C:\\Users\\世康\\Desktop\\hadoop\\HadoopDemo\\out\\artifacts\\maxtemperature\\maxtemperature.jar"); //运行前程序要被打成JAR包
//        configuration.set("fs.defaultFS", "hdfs://192.168.171.129:9000");
        try {
            FileSystem fs = FileSystem.get(configuration);

//        if (args.length != 2){
//            System.err.println("Usage:MaxTemperature <input path> <output path>");
//            System.exit(-1);
//        }
            Job job = Job.getInstance(configuration, "max temperature");
            job.setJarByClass(MaxTemperature.class);
            job.setMapperClass(MaxTemperatureMapper.class);
            job.setCombinerClass(MaxTemperatureReducer.class);
            job.setReducerClass(MaxTemperatureReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path outpath = new Path(args[1]);
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job任务执行成功！");
            }
        }catch(Exception e){
            System.out.println("job任务执行失败！");
            e.printStackTrace();
        }
    }
}
