package com.hyman.rabbitmqdemo.rabbitDemo.manual;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.HashMap;

@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.manual")
public class ConsumerApplication {

    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ConsumerApplication.class);

        RabbitAdmin rabbitAdmin = context.getBean(RabbitAdmin.class);
        MessageListenerContainer messagelistener = context.getBean(MessageListenerContainer.class);

        rabbitAdmin.declareExchange(new TopicExchange("hymanexchange",true,false,new HashMap<>()));
        // 声明队列 (队列名, 是否持久化, 是否排他, 是否自动删除, 队列属性);
        rabbitAdmin.declareQueue(new Queue("testqueue",true,false,false,new HashMap<>()));

        // 将队列 Binding 到交换机上
        rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue("testqueue")).to(new DirectExchange("hymanexchange")).with("test.#"));

        // 开始监听队列
        messagelistener.start();
    }
}
