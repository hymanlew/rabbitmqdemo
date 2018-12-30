package com.hyman.rabbitmqdemo.rabbitmqSpring.rpc;

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

@Configuration
public class SpringAMQPServerConfig {

    @Bean
    public Exchange exchange(){
        return new TopicExchange("hymanexchange",true,false,new HashMap<>());
    }

    @Bean
    public Queue queue(){
        return new Queue("testqueue",true,false,false,new HashMap<>());
    }

    @Bean
    public Binding binding(){
        return new Binding("testqueue",Binding.DestinationType.QUEUE,"hymanexchange","test.#",new HashMap<>());
    }

    @Bean
    public MessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory){
        SimpleMessageListenerContainer messagelistener = new SimpleMessageListenerContainer();
        messagelistener.setConnectionFactory(connectionFactory);
        messagelistener.setQueueNames("testqueue");

        messagelistener.setConcurrentConsumers(5);
        messagelistener.setMaxConcurrentConsumers(10);

        Map<String,Object> argumentMap = new HashMap<>();
        messagelistener.setConsumerArguments(argumentMap);

        messagelistener.setConsumerTagStrategy(new ConsumerTagStrategy() {
            @Override
            public String createConsumerTag(String s) {
                return "spring-RPC-server";
            }
        });

       RPCMethod rpcMethod = new RPCMethod();
        MessageListenerAdapter listenerAdapter = new MessageListenerAdapter(rpcMethod);
        listenerAdapter.setDefaultListenerMethod("addOrder");

        // 声明消息监听器
        messagelistener.setMessageListener(listenerAdapter);
        return messagelistener;
    }
}
