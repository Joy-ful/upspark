package com.ruiyuan.upspark.controller;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

@Slf4j
@RestController
public class SparkUp {

    @GetMapping(value = "/submitSparkJob")
    public String submitSparkJob() throws IOException {
        HashMap env = new HashMap();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

        //hadoop、spark环境变量读取
        env.put("HADOOP_CONF_DIR", "/etc/hadoop/conf");
        env.put("YARN_CONF_DIR", "/etc/hadoop/conf");
        env.put("SPARK_CONF_DIR", "/etc/spark/conf");
        env.put("JAVA_HOME", "/opt/module/java");

        //创建spark启动对象，并设置监听，spark启动的各参数
        SparkAppHandle handler = new SparkLauncher(env)
                .setSparkHome("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
                .setAppResource("/software/queue_stream.py")
                .setMainClass("queue_stream")
                .setAppName("python_pi" + " " + df.format(new Date()))
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.cores", "3")
                .setVerbose(true)
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        System.out.println("**********  state  changed  **********");
                    }

                    @Override
                    public void infoChanged(SparkAppHandle handle) {
                        //log.info("提交任务，任务状态：" + handle.getState());
                    }
                });

        while (!"FINISHED".equalsIgnoreCase(handler.getState().toString()) && !"FAILED".equalsIgnoreCase(handler.getState().toString()))
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        return handler.getAppId();
    }

    /*@GetMapping(value = "/sparkKill")
    public void stopTast() {
        handler.kill();
    }*/

    /**
     * 停止spark任务
     * <p>
     * yarn资源管理器地址， 例如：master:8032，查看yarn集群获取具体地址
     *
     * @param appIdStr 需要取消的任务id
     */
    @GetMapping(value = "/killSparkJob/{appIdStr}")
    public void killSparkJob(@PathVariable("appIdStr") String appIdStr) {
        log.info("取消spark任务,任务id：" + appIdStr);

        // 初始化 yarn的配置
        Configuration cf = new Configuration();

        boolean cross_platform = false;
        String os = System.getProperty("os.name");
        if (os.contains("Windows")) {
            cross_platform = true;
        }

        // 配置使用跨平台提交任务
        cf.setBoolean("mapreduce.app-submission.cross-platform", cross_platform);

        // 设置yarn资源，不然会使用localhost:8032
        cf.set("yarn.resourcemanager.address", "10.10.13.180:8032");

        // 创建yarn的客户端，此类中有杀死任务的方法
        YarnClient yarnClient = YarnClient.createYarnClient();

        //push  commit

        // 初始化yarn的客户端
        yarnClient.init(cf);

        // yarn客户端启动
        yarnClient.start();

        try {
            // 根据应用id，杀死应用
            //yarnClient.killApplication(getAppId(appIdStr));
            yarnClient.killApplication(ApplicationId.fromString(appIdStr));
        } catch (Exception e) {
            log.error("取消spark任务失败", e);
        }

        // 关闭yarn客户端
        yarnClient.stop();
    }

    private static ApplicationId getAppId(String appIdStr) {
        return ConverterUtils.toApplicationId(appIdStr);
    }

}

