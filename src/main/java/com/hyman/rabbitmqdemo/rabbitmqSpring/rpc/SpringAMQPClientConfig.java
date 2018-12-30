package com.hyman.rabbitmqdemo.rabbitmqSpring.rpc;

import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

/**
 * 使用 Spring AMQP进行异步RPC十分简单，只需在客户端发送消息请求的时候使用 sendAndReceive()就会同步等待服务端返回结果信息（即
 * 服务端消息处理结果的返回值），如果超时服务端未响应信息，则返回结果信息为NULL，可以通过 rabbitTemplate.setReplyTimeout(10000)
 * 设置超时时间。
 *
 */

@Configuration
public class SpringAMQPClientConfig {

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

    @Bean
    public Exchange exchange(){
        return new TopicExchange("hymanexchange",true,false,new HashMap<>());
    }
}
