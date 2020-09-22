package net.venusoft;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author lk
 * @date 2020/9/18 11:08
 */
@Slf4j
public class HdfsUploadHandler {

    private FileSystem fs;

    public HdfsUploadHandler() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        String url = "hdfs://hadoop01:9000";
        fs = FileSystem.get(new URI(url), conf, "hadoop");
    }

    public void close() throws IOException {
        if(fs!=null) fs.close();
    }

    public void upload(String p) throws IOException {
        File path = new File(p);
        if(path.exists()){
            File[] files = path.listFiles();
            for (File file:files) {
                if(file.isDirectory()){
                    upload(file.getAbsolutePath());
                }
                else{
                    FileInputStream inputStream = new FileInputStream(file);
                    String target = "/input2/"
                            +file.getName().replaceFirst("\\.[a-z0-9A-Z]*?$", "")
                            +"_"
                            +p.replaceFirst(".*\\\\", "")
                            +"."
                            +file.getName().replaceFirst("^.*\\.", "");

                    FSDataOutputStream outputStream = fs.create(new Path(target));
                    IOUtils.copyBytes(inputStream, outputStream, 4096);
                    log.info("upload success, source = {}, target = {}", file.getAbsolutePath(), target);
                }
            }
        }
    }

}
