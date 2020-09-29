package net.venusoft.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author lk
 * @date 2020/9/29 9:01
 */
public class AccessSortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text i:values){
            context.write(i, new DoubleWritable(-1*key.get()));
        }
    }
}
