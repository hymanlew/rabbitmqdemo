package com.hyman.rabbitmqdemo.rabbitmqSpring.messageAdapter;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
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
public class SpringAMQPConsumerConfigAuto {

    // 自动声明交换机，如果要一次性声明多个则使用 public List<Exchange> listExchange() 即可
    @Bean
    public Exchange exchange(){
        return new TopicExchange("hymanexchange",true,false,new HashMap<>());
    }

    // 自动声明队列，如果要一次性声明多个则使用 public List<Queue> listQueue() 即可
    @Bean
    public Queue queue(){
        // 声明队列 (队列名, 是否持久化, 是否排他, 是否自动删除, 队列属性);
        return new Queue("testqueue",true,false,false,new HashMap<>());
    }

    // 自动声明绑定，如果要一次性声明多个则使用 public List<Binding> listBinding() 即可
    @Bean
    public Binding binding(){
        // 要绑定到的目标，目标类型，交换器名，routing key，参数map
        return new Binding("testqueue",Binding.DestinationType.QUEUE,"hymanexchange","test.#",new HashMap<>());
    }

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
                return "spring-消息者测试-自定义消息处理器";
            }
        });

        messagelistener.setAutoStartup(true);

        // 新建消息处理器适配器
        MessageListenerAdapter messageAdapter = new MessageListenerAdapter(new MessageHandle());
        // 设置默认处理消息方法
        messageAdapter.setDefaultListenerMethod("handleMessage");
        // 声明消息处理器适配器属性，并使用 add方法进行处理
       Map<String,String> adapterConfig = new HashMap<>();
       adapterConfig.put("testqueue","add");
       messageAdapter.setQueueOrTagToMethodName(adapterConfig);

       messagelistener.setMessageListener(messageAdapter);

        return messagelistener;
    }
}
