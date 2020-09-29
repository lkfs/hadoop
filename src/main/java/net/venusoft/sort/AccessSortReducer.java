package net.venusoft.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author lk
 * @date 2020/9/29 9:01
 */
public class AccessSortReducer extends Reducer<AccessSortingBean, NullWritable, AccessSortingBean, NullWritable> {
    @Override
    protected void reduce(AccessSortingBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable n : values) {
            context.write(key, NullWritable.get());
        }
    }
}
