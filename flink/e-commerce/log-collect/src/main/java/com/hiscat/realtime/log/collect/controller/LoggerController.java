package com.hiscat.realtime.log.collect.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author hiscat
 */
@Slf4j
@RestController
@AllArgsConstructor
public class LoggerController {

    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/applog")
    public String applog(@RequestBody String data) {
        log.info(data);
        JSONObject json = JSON.parseObject(data);
        if (json.getJSONObject("start") != null) {
            kafkaTemplate.send("start-topic", data);
        } else {
            kafkaTemplate.send("event-topic", data);
        }
        return "success";
    }
}
