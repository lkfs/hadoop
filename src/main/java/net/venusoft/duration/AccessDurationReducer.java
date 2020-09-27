package net.venusoft.duration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lk
 * @date 2020/9/21 16:50
 */
public class AccessDurationReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int sum= 0;
        for (IntWritable i : values) {
            count++;
            sum += i.get();
        }
        context.write(key, new Text(count+"__"+(count>0?(sum/count):0)));
        //context.write(key, new Text(Integer.toString(count)));
        //context.write(key, new IntWritable(count));
    }
}
