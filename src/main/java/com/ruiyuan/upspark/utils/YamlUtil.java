package com.ruiyuan.upspark.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class YamlUtil {
    //解析yml配置文件为map
    public HashMap<String, String> yamlRead() {
        Yaml yaml = new Yaml();
        InputStream inputStream = null;
        HashMap<String, String> obj = null;
        try {
            inputStream = this.getClass()
                    .getClassLoader()
                    .getResourceAsStream("application.yml");
            return yaml.load(inputStream);
            //System.out.println(obj);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }




}