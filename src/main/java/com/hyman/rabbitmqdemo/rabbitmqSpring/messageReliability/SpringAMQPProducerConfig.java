package com.hyman.rabbitmqdemo.rabbitmqSpring.messageReliability;

import com.hyman.rabbitmqdemo.util.Logutil;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.UnsupportedEncodingException;

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
        /**
         * 启用受托，确认回调机制（只有路由失败时，即匹配失败时，才会执行回调业务）。
         * 如果消息没有被正确路由，则 ACK 的值仍为true，Publisher Confirm只能保证消息到达消息中间件。如果要使得ACK的值为false
         * 可以通过在发送消息还没被确认时，停止RabbitMQ服务进行测试。
         *
         * 同时Spring AMQP对Publisher Confirm进行了封装，我们可以在发送消息时传递CorrelationData，当调用消息确认回调方法时我
         * 们可以获取到发送消息时传递的CorrelationData，该功能为我们业务处理提供了极大便利，我们不再需要花成本去维护Delivery Tag，
         * 可以直接使用 CorrelationData 的 getId() 方法获取业务主键。
         */
        template.setMandatory(true);
        template.setReturnCallback(new RabbitTemplate.ReturnCallback(){
            @Override
            public void returnedMessage(Message message, int i, String s, String s1, String s2) {
                try {
                    Logutil.logger.info("replyCode："+ i);
                    Logutil.logger.info("replyText："+ s);
                    Logutil.logger.info("exchange："+ s1);
                    Logutil.logger.info("routingKey："+ s2);
                    Logutil.logger.info("properties："+ message.getMessageProperties());
                    Logutil.logger.info("消息体："+ new String(message.getBody(),"utf-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        });
        template.setConfirmCallback(new RabbitTemplate.ConfirmCallback(){
            @Override
            public void confirm(CorrelationData correlationData, boolean b, String s) {
                Logutil.logger.info("ACK："+ b);
                Logutil.logger.info("cause："+ s);
                Logutil.logger.info("message ID："+ correlationData.getId());
            }
        });
        return template;
    }
}
