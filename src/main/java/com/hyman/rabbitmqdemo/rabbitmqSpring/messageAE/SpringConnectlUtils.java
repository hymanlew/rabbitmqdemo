package com.hyman.rabbitmqdemo.rabbitmqSpring.messageAE;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring整合RabbitMQ：
 * Spring AMQP 是对 AMQP 协议的抽象和封装，从官方网站上得知它是由两个项目组成的(spring-amqp和spring-rabbit)。在使用Spring
 * 整合 RabbitMQ 时我们主要关注三个核心接口：
 *
 * RabbitAdmin: 用于声明交换机、队列、绑定等。
 * RabbitTemplate: 用于 RabbitMQ 消息的发送和接收。
 * MessageListenerContainer: 监听容器，为消息入队提供异步处理。
 */

// 创建通道连接配置类，不能声明为静态方法
@Configuration
public class SpringConnectlUtils {

    // 将 rabbitmq 客户端的连接工厂对象交由 springrabbitmq 进行管理，所以这里导入不同的工厂。
    @Bean
    public ConnectionFactory getConnectFactory(){
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();

        // 配置连接信息
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setVirtualHost("hymanhost");
        factory.setUsername("hyman");
        factory.setPassword("hyman");

        // 网络异常自动连接恢复
        factory.setAutomaticRecoveryEnabled(true);
        // 每10秒尝试重试连接一次
        factory.setNetworkRecoveryInterval(10000);

        // 设置ConnectionFactory属性信息
        Map<String,Object> factoryPropertiesMap = new HashMap<>();
        factoryPropertiesMap.put("name","hyman");
        factoryPropertiesMap.put("ip","127.0.0.1");
        factory.setClientProperties(factoryPropertiesMap);

        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(factory);
        return cachingConnectionFactory;
    }
}