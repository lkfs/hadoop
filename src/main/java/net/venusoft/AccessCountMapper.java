package net.venusoft;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lk
 * @date 2020/9/18 11:59
 */
public class AccessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Pattern pattern = Pattern.compile("path:\\[(.*?)\\]");
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcher = pattern.matcher(line);
        if(matcher.find()){
            String path =  matcher.group(1);
            context.write(new Text(path), new IntWritable(1));
        }
    }

    /*public static void main(String[] args) {
        String t = "2020-09-15 10:21:11.057 [1600136471057] [http-nio-8089-exec-1] INFO  com.qrqy.ibd.config.AopConfig - class:[PhysicianMessageController],method:[getMessageList],path:[/messageList],request :[GetMessageListQO(pageSize=20, pageNo=0)]";
        Pattern pattern = Pattern.compile("path:\\[(.*?)\\]");
        Matcher matcher = pattern.matcher(t);
        if(matcher.find()){
            String path =  matcher.group(1);

            System.out.println(path);

        }
    }*/
}
