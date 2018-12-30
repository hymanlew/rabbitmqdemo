package com.hyman.rabbitmqdemo.rabbitDemo.messageListenerContainer;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.HashMap;

//@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.messageListenerContainer")
@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.messageReliability")
public class ProducerApplication {

    public static void sendString(RabbitTemplate template, String s,String id){
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        properties.setContentType("utf-8");
        Message message = new Message(s.getBytes(),properties);
        template.send("hymanexchange","test.me",message,new CorrelationData(id));
        //template.send("hymanexchange","test1.me",message,new CorrelationData(id));
    }


    public static void main(String[] args) {
        // 获取注解的 bean 的上下文配置信息
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ProducerApplication.class);

        RabbitAdmin rabbitAdmin = context.getBean(RabbitAdmin.class);
        RabbitTemplate template = context.getBean(RabbitTemplate.class);

        // 绑定交换机
        rabbitAdmin.declareExchange(new TopicExchange("hymanexchange",true,false,new HashMap<>()));

        for (int i=0; i<3; i++) {
            if(i == 0){
                sendString(template,"测试异常","id1");
                continue;
            }else if(i == 1){
                sendString(template,"真实异常","id2");
                continue;
            }else {
                sendString(template,"正常数据","id3");
            }
        }
    }
}
