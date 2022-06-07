package com.ruiyuan.upspark.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.HashMap;


@Slf4j
public class ReadImage {

    /**
     * 从hdfs上读取sequenceFile
     *
     * @param path        hdfs文件名
     * @param key         key值
     * @param valuelength 文件长度
     * @return pictureUrl
     * @throws IOException
     */
    public  String readHDFSSequenceFile(String path, String pictkey) throws IOException {
        YamlUtil yamlUtil = new YamlUtil();
        HashMap hashMap = yamlUtil.yamlRead();
        String hdfsDir = (String) hashMap.get("hdfsDir");
        String HDFS_URL = (String) hashMap.get("HDFS_URL");


        //hdfs的配置
        Configuration conf = new Configuration();
        Path newpath = new Path(hdfsDir + path);
        //System.out.println(newpath);

        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option option = SequenceFile.Reader.file(newpath);
        reader = new SequenceFile.Reader(conf, option);

        Text keys = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        BytesWritable values = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        Text key = new Text(pictkey.getBytes());

        String value = null;
        while (reader.next(keys, values)) {
            //上传文件名与读取的文件名
            if (key.equals(keys)) {
                //文件指针
                long position = reader.getPosition();

                int valuelength = values.getLength();
                // offset
                long offset = position - valuelength;
                String http = "http://" + HDFS_URL + "/webhdfs/v1" + hdfsDir + path + "?op=OPEN&user.name=root&offset=";
                String length = "&length=";
                value = http + offset + length + valuelength;
            }
        }
        reader.close();
        return value;
    }
}
