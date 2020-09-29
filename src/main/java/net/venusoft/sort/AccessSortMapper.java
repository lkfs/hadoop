package net.venusoft.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lk
 * @date 2020/9/29 8:59
 */
public class AccessSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        if (splits.length >= 4) {
            String path = splits[0];
            int count = Integer.parseInt(splits[1]);
            int sum = Integer.parseInt(splits[2]);
            double avg = Double.parseDouble(splits[3]);
            AccessSortingBean bean = new AccessSortingBean(path, count, sum, avg);
            context.write(new DoubleWritable(-1*avg), new Text(path));
        }
    }
}
