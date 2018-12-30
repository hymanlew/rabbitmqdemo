package com.hyman.rabbitmqdemo.rabbitmqSpring.messageListenerContainer;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 在之前的例子中，我们往MessageListenerContainer设置了MessageListener进行消息的消费，本篇将介绍一种更为简单的消息消费方式，
 * 使用 @RabbitListener 注解方式。使用 RabbitListener进行消息的消费步骤如下：
 *
 * 1，在启动类上添加@EnableRabbit注解
 * 2，在Spring容器中托管一个RabbitListenerContainerFactory，默认实现类SimpleRabbitListenerContainerFactory
 * 3，编写一个消息处理器类托管到Spring容器中，并使用 @RabbitListener注解标注该类为RabbitMQ的消息处理类
 * 4，使用 @RabbitHandler注解标注在方法上，表示当有收到消息的时候，就交给带有 @RabbitHandler的方法处理，具体找哪个方法需要根
 *    据 MessageConverter 转换后的对象类型决定。
 */

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
