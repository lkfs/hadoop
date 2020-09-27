package net.venusoft.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lk
 * @date 2020/9/27 15:12
 */
public class AccessCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        String inputPath = "hdfs://hadoop01:9000/input";
        String outputPath = "hdfs://hadoop01:9000/output1_9";

        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(AccessCountDriver.class);

        job2.setMapperClass(AccessCountMapper.class);
        job2.setReducerClass(AccessCountReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        job2.waitForCompletion(true);
    }
}
