package com.hyman.rabbitmqdemo.rabbitmqSpring.messageConverter;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.ClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
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

    /**
     * 自定义 json 对象转换器，以实现在不同的项目系统中传输类对象数据。发送消息的类和接受消息的类必须是一样的，不仅是要里面的字
     * 段一样，而且类名也要一样，然后指定一个消费端该类的包路径。
     *
     * 但是在本例中，上面的方法都抛出异常（在重写的 toClass 方法中），应该是此方法已经过时了。因为现在可以在 setIdClassMapping
     * 中设置一个 json 对象类（class）的路径。
     * @return
     */
    //@Bean
    //public Jackson2JsonMessageConverter customJsonConverter() {
    //    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    //    converter.setClassMapper(new ClassMapper() {
    //        @Override
    //        public Class<?> toClass(MessageProperties properties) {
    //            //throw new UnsupportedOperationException("this mapper is only for outbound, do not use for receive message");
    //            Object object = properties.getHeaders().get("__TypeId__");
    //            return object.getClass();
    //        }
    //        @Override
    //        public void fromClass(Class<?> clazz, MessageProperties properties) {
    //            properties.setHeader("__TypeId__", "hyman.springbootdemo.rabbitmqSpring.messageConverter.Order");
    //        }
    //    });
    //    return converter;
    //}
}
