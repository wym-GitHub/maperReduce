package cn.itcast;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        String line = value.toString();
        String[] split = line.split(",");
        for (String word : split) {
            //context.write(new Text(word), new LongWritable(1));
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable );
        }
    }
}
