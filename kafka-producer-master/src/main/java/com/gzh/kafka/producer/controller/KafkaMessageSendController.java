package com.gzh.kafka.producer.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.gzh.kafka.producer.service.KafkaMessageSendService;

@RestController
// @RequestMapping(value = "send", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
@RequestMapping(value = "send")
public class KafkaMessageSendController {

    @Autowired
    private KafkaMessageSendService kafkaMessageSendService;

    @RequestMapping(value = "/sendMessage")
    public String send(@RequestParam(required = true) String message) {
        System.out.println("*****************" + message);
        try {
            kafkaMessageSendService.send(message);
        } catch (Exception e) {
            return "send failed.";
        }
        return message;
    }

    @RequestMapping(value = "/sendReceive")
    public String sendReceive(@RequestParam(required = true) String message)
            throws InterruptedException, ExecutionException {
        String retMsg = kafkaMessageSendService.sendReceive(message);
        return retMsg;
    }

}