package net.venusoft.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author lk
 * @date 2020/9/27 15:09
 */
public class AccessCountReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for(IntWritable i:values){
            sum += i.get();
            count ++;
        }
        String result = count+"\\t"+sum+"\\t"+(1.0*sum/count);
        context.write(key, new Text(result));
    }
}
