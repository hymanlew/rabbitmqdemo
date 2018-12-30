package com.hyman.rabbitmqdemo.rabbitmqSpring.messageAE;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring整合RabbitMQ：
 * Spring AMQP 是对 AMQP 协议的抽象和封装，从官方网站上得知它是由两个项目组成的(spring-amqp和spring-rabbit)。在使用Spring
 * 整合 RabbitMQ 时我们主要关注三个核心接口：
 *
 * RabbitAdmin: 用于声明交换机、队列、绑定等。
 * RabbitTemplate: 用于 RabbitMQ 消息的发送和接收。
 * MessageListenerContainer: 监听容器，为消息入队提供异步处理。
 */

// 创建生产者配置类，将RabbitAdmin、RabbitTemplate纳入Spring管理
@Configuration
public class SpringAMQPProducerConfig {

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory){
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory){
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMandatory(true);
        return template;
    }
}
