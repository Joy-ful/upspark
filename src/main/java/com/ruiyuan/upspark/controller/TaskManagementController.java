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
    private String start(@RequestBody Map<String, Object> param)  {
        return taskManagementService.startTask(param);
    }

    @RequestMapping(value="/killSparkJob", method= RequestMethod.POST)
    private String stop(@RequestBody Map<String, Object> param) {
        return taskManagementService.stopTask(param);
    }
}
