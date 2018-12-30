package com.hyman.rabbitmqdemo.rabbitmqBoot;

import com.hyman.rabbitmqdemo.util.Logutil;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
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
 *
 * 在 manual Demo 中我们是手动使用 RabbitAdmin 对交换机、队列和绑定进行声明的，Spring AMQP还提供了自动声明方式交换机、队列
 * 和绑定。我们可以直接把要自动声明的组件纳入 Spring 容器中管理即可。
 * 自动声明发生在RabbitMQ第一次连接创建的时候，自动声明支持单个和批量自动声明。使用自动声明需要符合如下条件：
 *
 * 1，需要有连接产生。
 * 2，RabbitAdmin 必须交由 Spring管理，且autoStartup必须为true(默认)
 * 3，如果ConnectionFactory使用的是CachingConnectionFactory，则cacheMode必须要为CacheMode.CHANNEL
 * 4，所有要声明的组件的 shouldDeclare必须为true
 * 5，要声明的Queue名称不能以amq.开头
 *
 * 上诉规则定义在RabbitAdmin的afterPropertiesSet方法中，有兴趣的同学可以自行阅读RabbitAdmin源码
 *
 */
@Configuration
public class BootConsumerConfigAuto {

    @Bean
    public Exchange exchange(){
        return new TopicExchange("bootexchange",true,false,new HashMap<>());
    }

    @Bean
    public Queue queue(){
        // 声明队列 (队列名, 是否持久化, 是否排他, 是否自动删除, 队列属性);
        return new Queue("bootqueue",true,false,false,new HashMap<>());
    }

    @Bean
    public Binding binding(){
        // 要绑定到的目标，目标类型，交换器名，routing key，参数map
        return new Binding("bootqueue",Binding.DestinationType.QUEUE,"bootexchange","add.#",new HashMap<>());
    }

    @Bean
    public MessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory){
        // 声明消息监听容器
        SimpleMessageListenerContainer messagelistener = new SimpleMessageListenerContainer();
        messagelistener.setConnectionFactory(connectionFactory);
        messagelistener.setQueueNames("bootqueue");

        messagelistener.setConcurrentConsumers(5);
        messagelistener.setMaxConcurrentConsumers(10);

        Map<String,Object> argumentMap = new HashMap<>();
        messagelistener.setConsumerArguments(argumentMap);

        // 设置消费者标签
        messagelistener.setConsumerTagStrategy(new ConsumerTagStrategy() {
            @Override
            public String createConsumerTag(String s) {
                return "boot-消息者";
            }
        });

        // 使用 setAutoStartup 方法设置自动消息消费时机
        //messagelistener.setAutoStartup(true);
        // 增加消息后置处理器
        //messagelistener.setAfterReceivePostProcessors();

        // 声明消息监听器
        messagelistener.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    Logutil.logger.info("==== "+new String(message.getBody(),"utf-8"));
                    Logutil.logger.info("==== "+message.getMessageProperties());
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        });

        return messagelistener;
    }
}
