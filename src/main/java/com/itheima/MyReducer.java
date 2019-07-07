package com.itheima;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

    public static enum Counter{
        MY_REDUCE_INPUT_RECORDS,MY_REDUCE_INPUTBYTES
    }
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.MY_REDUCE_INPUT_RECORDS).increment(1L);
        context.write(key,NullWritable.get() );
    }
}
