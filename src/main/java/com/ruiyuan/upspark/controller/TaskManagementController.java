package com.ruiyuan.upspark.controller;


import com.ruiyuan.upspark.service.impl.TaskManagementServiceImpl;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Map;


@RestController
public class TaskManagementController {

    @Resource
    TaskManagementServiceImpl taskManagementService;
    //SparkUp sparkUp;

    //String UVAID, String url
    @RequestMapping(value = "/submitSparkJob", method = RequestMethod.POST)
    private String start(@RequestParam Map<String, String> param)  {
        return taskManagementService.startTask(param);
    }

    @RequestMapping(value="/killSparkJob", method= RequestMethod.POST)
    private String stop(@RequestBody String jobID) {
        return taskManagementService.stopTask(jobID);
    }
}
