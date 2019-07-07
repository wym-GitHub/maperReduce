package com.itheima;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        //分区接收的参数,<k2,v2>
        String s = text.toString().split("\t")[5];
        if (Integer.parseInt(s) >= 15) {
            return 1;
        } else {
            return 0;

        }
    }
}
