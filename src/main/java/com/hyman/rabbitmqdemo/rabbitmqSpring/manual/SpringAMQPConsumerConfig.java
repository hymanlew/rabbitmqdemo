package com.hyman.rabbitmqdemo.rabbitmqSpring.manual;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * RabbitAdmin: 用于声明交换机、队列、绑定等。
 * RabbitTemplate: 用于 RabbitMQ 消息的发送和接收。
 * MessageListenerContainer: 消息监听容器，为消息入队提供异步处理。
 */
@Configuration
public class SpringAMQPConsumerConfig {

    // 由于配置类都是在一个包内，所以只声明一次即可
    //@Bean
    //public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory){
    //    return new RabbitAdmin(connectionFactory);
    //}

    @Bean
    public MessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory){
        // 声明消息监听容器
        SimpleMessageListenerContainer messagelistener = new SimpleMessageListenerContainer();
        messagelistener.setConnectionFactory(connectionFactory);
        messagelistener.setQueueNames("testqueue");

        // 设置消费者线程数
        messagelistener.setConcurrentConsumers(5);
        // 设置最大消费者线程数
        messagelistener.setMaxConcurrentConsumers(10);
        // 设置消费者属性信息
        Map<String,Object> argumentMap = new HashMap<>();
        messagelistener.setConsumerArguments(argumentMap);

        // 设置消费者标签
        messagelistener.setConsumerTagStrategy(new ConsumerTagStrategy() {
            @Override
            public String createConsumerTag(String s) {
                return "spring-消息者测试-1";
            }
        });

        // 使用 setAutoStartup 方法可以手动设置消息消费时机
        messagelistener.setAutoStartup(false);
        // 增加消息后置处理器
        //messagelistener.setAfterReceivePostProcessors();

        // 声明消息监听器
        messagelistener.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("1 ==== "+new String(message.getBody(),"utf-8"));
                    System.out.println("1 ==== "+message.getMessageProperties());
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        });

        return messagelistener;
    }
}
