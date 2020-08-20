package com.atguigu.realtime.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@ResponseBody
public class LoggerController {

    @PostMapping("/log")
    public String dbLog(@RequestParam("log") String log) {
        // 1. 给日志添加时间戳
        log = addTs(log);
        // 2. 把数据罗盘, 给离线需求使用
        saveToDisk(log);
        // 3. 把数据发送到kafka
        sendToKafka(log);


        return "ok";
    }

    @Autowired
    KafkaTemplate<String, String> kafka;

    private void sendToKafka(String log) {
        // 不同的日志写入到不同的topic
        if (log.contains("startup")) {
            System.out.println("come here1");
            kafka.send(Constant.STARTUP_LOG_TOPIC, log);
        } else {
            System.out.println("come here1");
            kafka.send(Constant.EVENT_LOG_TOPIC, log);
        }
    }

    /**
     * 把日志信息写入到磁盘
     *
     * @param log
     */

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    private void saveToDisk(String log) {
        logger.info(log);
    }

    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
        return obj.toJSONString();
    }

}
