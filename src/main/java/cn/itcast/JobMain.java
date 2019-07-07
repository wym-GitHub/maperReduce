package cn.itcast;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
        //打包到集群上面运行的时候,必须要添加以下配置,指定程序的main函数
        job.setJarByClass(JobMain.class);
        //第一步:读取输入文件解析成key,value 对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/wordcount"));
        TextInputFormat.addInputPath(job, new Path("file:///D:\\input"));
        //第二步:设置我们的mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置我们的map阶段完成的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //shuffle阶段省略
        //设置我们reduce类
        job.setReducerClass(WordCountReducer.class);
        //设置我们reduce阶段完成之后的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //第八步:设置输出类以及输出路径//
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/wordcount_out"));
//        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\output"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;

    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Tool tool =new JobMain() ;
        int run = ToolRunner.run(configuration, tool, args);


        System.exit(run);
    }
}
