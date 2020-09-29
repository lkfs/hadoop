package net.venusoft.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lk
 * @date 2020/9/29 8:57
 */
public class AccessSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME","hadoop");

        String inputPath = "hdfs://hadoop01:9000/output1_4";
        String outputPath = "hdfs://hadoop01:9000/output2_1";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AccessSortDriver.class);

        job.setMapperClass(AccessSortMapper.class);
        job.setReducerClass(AccessSortReducer.class);

        job.setMapOutputKeyClass(AccessSortingBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(AccessSortingBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
