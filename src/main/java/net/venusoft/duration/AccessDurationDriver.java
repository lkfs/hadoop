package net.venusoft.duration;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lk
 * @date 2020/9/28 12:06
 */
@Slf4j
public class AccessDurationDriver {
    public static class AccessDurationMapper extends Mapper<LongWritable, Text, Text, AccessInfoBean>{
        private Map<String, AccessInfoBean> map = new HashMap<>();
        private Pattern requestPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.+?)\\],request :");
        private Pattern responsePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.+?)\\] took time:\\[(\\d*)ms\\],response :");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcherRequest = requestPattern.matcher(line);
            if(matcherRequest.find()){
                String startTime = matcherRequest.group(1);
                String logId = matcherRequest.group(2);
                String path = matcherRequest.group(3);

                AccessInfoBean infoBean = new AccessInfoBean(path, startTime);

                log.info("find request, logId = {}, info = {}", logId, infoBean);
                map.put(logId, infoBean);
            }
            else{
                Matcher matcherResponse = responsePattern.matcher(line);
                if(matcherResponse.find()){
                    String endTime = matcherResponse.group(1);
                    String logId = matcherResponse.group(2);
                    String path = matcherResponse.group(3);
                    String duration = matcherResponse.group(4);

                    AccessInfoBean infoBean = map.get(logId);
                    if(infoBean!=null){
                        infoBean.setEndTime(endTime);
                        try {
                            infoBean.setDuration(Integer.parseInt(duration));
                        } catch (NumberFormatException e) {
                            log.error("duration is null, line = {}", line);
                        }

                        log.info("find response, logId = {}, info = {}", logId, infoBean);
                        context.write(new Text(path), infoBean);
                    }
                }
            }
        }
    }

    public static class AccessDurationReducer extends Reducer<Text, AccessInfoBean, Text, AccessResultBean>{
        @Override
        protected void reduce(Text key, Iterable<AccessInfoBean> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for(AccessInfoBean i:values){
                sum += i.getDuration();
                count ++;
            }
            context.write(key, new AccessResultBean(count, sum, (1.0*sum/count)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputPath = "hdfs://hadoop01:9000/input/";
        String outputPath = "hdfs://hadoop01:9000/output1_2";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AccessDurationDriver.class);

        job.setMapperClass(AccessDurationMapper.class);
        job.setReducerClass(AccessDurationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessInfoBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AccessResultBean.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }

    /*public static void main(String[] args) {
        String line ="2020-09-12 09:06:42.350 [1599872801818] [http-nio-8089-exec-3] INFO  com.qrqy.ibd.config.AopConfig - class:[CommonController],method:[getUnionId],path:[/common/getUnionId] took time:[532ms],response :{\"data\":\"\",\"code\":200,\"message\":\"success\",\"result\":{\"unionId\":\"oYjYhwB_nf6o9YuwTyF5Mf9rYUJU\",\"openid\":\"o5zsG5iZ_v4k8fPsBKEPDxjUOyxs\"}}";
         Pattern responsePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s*\\[(\\d*)\\].*path:\\[(.+?)\\] took time:\\[\\d*ms\\],response :(\\{.*\\})");
        Matcher matcherResponse = responsePattern.matcher(line);
        if(matcherResponse.find()){
            log.info("success");
        }
        else {
            log.info("fail");
        }
    }*/
}
