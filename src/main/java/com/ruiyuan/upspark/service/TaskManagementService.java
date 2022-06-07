package com.ruiyuan.upspark.service;

import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

public abstract class TaskManagementService {
    //String UVAID, String url

    protected abstract String startTask(Map<String, Object> param);

    protected abstract String stopTask(Map<String, Object> param);

    protected abstract String getTaskStatus();


}
