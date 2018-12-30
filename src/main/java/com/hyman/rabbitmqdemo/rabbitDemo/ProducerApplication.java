package com.hyman.rabbitmqdemo.rabbitDemo;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.HashMap;

/**
 * 生产者启动类：
 *
 * RabbitAdmin: 用于声明交换机、队列、绑定等。
 * RabbitTemplate: 用于 RabbitMQ 消息的发送和接收。
 * MessageListenerContainer: 监听容器，为消息入队提供异步处理。
 */

//@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.manual")
//@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.auto")
@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.messageAdapter")
public class ProducerApplication {

    public static void main(String[] args) {
        // 获取注解的 bean 的上下文配置信息
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ProducerApplication.class);

        RabbitAdmin rabbitAdmin = context.getBean(RabbitAdmin.class);
        RabbitTemplate template = context.getBean(RabbitTemplate.class);

        // 绑定交换机
        rabbitAdmin.declareExchange(new TopicExchange("hymanexchange",true,false,new HashMap<>()));

        // 消息属性，持久化，编码
        MessageProperties messageConfig = new MessageProperties();
        messageConfig.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        messageConfig.setContentType("UTF-8");
        Message me = new Message("spring-消息测试".getBytes(),messageConfig);
        //Message me1 = new Message("spring-自动声明消息测试".getBytes(),messageConfig);

        /**
         * exchange, routingKey, Message, CorrelationData（消息唯一 id，非必要参数）。
         * 发布消息还可以使用rabbitTemplate.convertAndSend(); 其支持消息后置处理。
         */
        template.send("hymanexchange","test.me",me,new CorrelationData("1"));
        //template.send("hymanexchange","test.me",me1);
    }
}
