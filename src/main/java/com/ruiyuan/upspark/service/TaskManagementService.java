package com.ruiyuan.upspark.service;

import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

public abstract class TaskManagementService {
    //String UVAID, String url

<<<<<<< HEAD
    protected abstract String startTask(Map<String, Object> param);

    protected abstract String stopTask(Map<String, Object> param);
=======
    protected abstract String startTask(Map<String, String> param);

    protected abstract String stopTask(String jobID);
>>>>>>> origin/master

    protected abstract String getTaskStatus();


}
