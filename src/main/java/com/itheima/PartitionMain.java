package com.itheima;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //创建job对象
        Job job = Job.getInstance(super.getConf(), PartitionMain.class.getSimpleName());
        //打包到集群上面运行的时候,必须要添加以下配置,指定程序的main函数
        job.setJarByClass(PartitionMain.class);
        //第一步:读取输入文件,指定输入文件路径
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/input"));
        TextInputFormat.addInputPath(job, new Path("file:///D:\\input"));
        //第二步:mapper阶段,指定mapper类,和输出数据的参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //shuffle阶段:
//         第三步:指定自定义分区类,以及reduceTask的个数
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);
        //shuffle第四五六步省略,使用默认
        //第七步:指定reduce类,以及输出数据类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //第八步:指定输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://node01:8020/output"));
        TextOutputFormat.setOutputPath(job, new Path("file:///D:\\output"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new PartitionMain(), args);
        System.exit(run);
    }
}
