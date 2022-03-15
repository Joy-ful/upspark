package com.ruiyuan.upspark.service.impl;

import com.ruiyuan.upspark.service.TaskManagementService;
import com.ruiyuan.upspark.utils.EsUtils;
import com.ruiyuan.upspark.utils.MySQLUtils;
import com.ruiyuan.upspark.utils.ReadImage;
import com.ruiyuan.upspark.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class TaskManagementServiceImpl extends TaskManagementService {

    /**
     * 启动spark任务
     */
    @Override
    public String startTask(Map<String, String> param) {

        HashMap env = new HashMap();
        String uvaid = param.get("UVAID");
        String taskid = param.get("TaskID");
        String url = param.get("url");

        //设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //hadoop、spark环境变量读取
        env.put("HADOOP_CONF_DIR", "/etc/hadoop/conf");
        env.put("YARN_CONF_DIR", "/etc/hadoop/conf");
        env.put("SPARK_CONF_DIR", "/etc/spark/conf");
        env.put("JAVA_HOME", "/opt/module/java");

        //创建spark启动对象，并设置监听，spark启动的各参数
        SparkAppHandle handler = null;

        try {
            handler = new SparkLauncher(env)
                    .setSparkHome("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
                    .setAppResource("/software/UVVideo/UAVideo/target/VStreamAIService-1.0-SNAPSHOT.jar")
                    .setMainClass("com.ruiyuan.jobs.UAVVideoStreamingTask")
                    .setAppName("UAVVideoStreamingTask" + " " + df.format(new Date()))
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .setConf("spark.executor.instances", "1")
                    //.setConf("spark.cores.max", "1")
                    .setConf("spark.executor.cores", "1")
                    .setConf("spark.executor.memory", "2G")
                    //.setConf("spark.task.cpus","1")
                    .setConf("spark.default.parallelism", "1")
                    //.setConf("spark.yarn.executor.memoryOverhead","8G")

                    .addAppArgs(uvaid, taskid, url)
                    .setVerbose(true)
                    .startApplication(new SparkAppHandle.Listener() {
                        @Override
                        public void stateChanged(SparkAppHandle handle) {
                            System.out.println("**********  stateChanged  changed  **********");
                            System.out.println("--   uvaid" + uvaid + "--  url" + url);
                        }

                        @Override
                        public void infoChanged(SparkAppHandle handle) {
                            System.out.println("**********  infoChanged  changed  **********");
                        }
                    });

            String appId = null;
            //jobID
            while (!"FINISHED".equalsIgnoreCase(handler.getState().toString()) && !"FAILED".equalsIgnoreCase(handler.getState().toString())) {
                try {
                    if ("SUBMITTED".equalsIgnoreCase(handler.getState().toString())) {
                        appId = handler.getAppId();
                        return appId;
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 停止spark任务
     */
    @Override
    public String stopTask(Map<String, String> param) {

        String appIdStr = param.get("appIdStr");
        String UAVid = param.get("UVAID");
        String Taskid = param.get("TaskID");

        log.info("取消spark任务,任务id：" + appIdStr);

        // 初始化 yarn的配置
        Configuration conf = new Configuration();

        boolean cross_platform = false;
        String os = System.getProperty("os.name");
        if (os.contains("Windows")) {
            cross_platform = true;
        }

        // 配置使用跨平台提交任务
        conf.setBoolean("mapreduce.app-submission.cross-platform", cross_platform);

        // 设置yarn资源，不然会使用localhost:8032
        conf.set("yarn.resourcemanager.address", "10.10.13.180:8032");

        // 创建yarn的客户端，此类中有杀死任务的方法
        YarnClient yarnClient = YarnClient.createYarnClient();

        // 初始化yarn的客户端
        yarnClient.init(conf);

        // yarn客户端启动
        yarnClient.start();

        try {
            // 根据应用id，杀死应用
            //yarnClient.killApplication(getAppId(appIdStr));
            yarnClient.killApplication(ApplicationId.fromString(appIdStr));
        } catch (Exception e) {
            log.error("取消spark任务失败", e);
            return "id Null";
        }

        // 关闭yarn客户端
        yarnClient.stop();
        ZkUtils zkUtils = new ZkUtils();
        MySQLUtils mySQLUtils = new MySQLUtils();
        ReadImage readImage = new ReadImage();
        EsUtils esUtils = new EsUtils();


        int mysqlCount = zkUtils.getMysqlCount(UAVid, Taskid);
        mySQLUtils.addNewsPaper(Taskid, mysqlCount);
        for (int i = 0; i <= mysqlCount; i++) {
            try {
                String MiopictureURL = Taskid + "_" + i + ".jpg";
                String SeqpictureURL = readImage.readHDFSSequenceFile(UAVid + Taskid, MiopictureURL);
                boolean b = esUtils.updateByQuery("MiopictureURL", SeqpictureURL);
                System.out.println(b);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return "success: " + appIdStr;
    }

    /*public String stopTask(@PathVariable("appIdStr") String jobID) {


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

        // 初始化yarn的客户端
        yarnClient.init(cf);

        // yarn客户端启动
        yarnClient.start();

        try {
            // 根据应用id，杀死应用
            yarnClient.killApplication(getAppId(jobID));
            //yarnClient.killApplication(ApplicationId.fromString(jobID));
        } catch (Exception e) {
            //log.error("取消spark任务失败", e);
        }

        // 关闭yarn客户端
        yarnClient.stop();

        return jobID;
    }*/

    @Override
    protected String getTaskStatus() {
        return null;
    }


    private static ApplicationId getAppId(String appIdStr) {
        return ConverterUtils.toApplicationId(appIdStr);
    }

}
