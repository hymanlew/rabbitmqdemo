package com.hyman.rabbitmqdemo.rabbitDemo.rpc;

import com.hyman.rabbitmqdemo.util.Logutil;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.HashMap;

@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.rpc")
public class ClientApplication {

    public static void main(String[] args) {
        // 获取注解的 bean 的上下文配置信息
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ClientApplication.class);

        RabbitTemplate template = context.getBean(RabbitTemplate.class);

        MessageProperties properties = new MessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        properties.setContentType("utf-8");
        Message message = new Message("RPC 请求订单信息".getBytes(),properties);

        // 如果超时未返回，则messageReturn为null。可以通过 setReplyTimeout(10000);设置超时时间
        template.setReplyTimeout(5000);
        Object messageReturn = template.sendAndReceive("hymanexchange","test",message,new CorrelationData("2018"));
        Logutil.logger.info(messageReturn != null ? messageReturn.toString():"null");
    }
}
