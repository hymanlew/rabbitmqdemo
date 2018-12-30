package com.hyman.rabbitmqdemo.rabbitmqSpring.messageListenerContainer;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringAMQPConsumerConfig {

    // 注意返回的一个泛型，并且方法名是固定的，不可以更改。否则找不到该 bean
    @Bean
    public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory(ConnectionFactory connectionFactory){
        SimpleRabbitListenerContainerFactory simpleFactory = new SimpleRabbitListenerContainerFactory();
        simpleFactory.setConnectionFactory(connectionFactory);
        simpleFactory.setConcurrentConsumers(5);
        simpleFactory.setMaxConcurrentConsumers(10);
        simpleFactory.setConsumerTagStrategy(new ConsumerTagStrategy() {
            @Override
            public String createConsumerTag(String s) {
                return "自定义消息监听容器";
            }
        });

        // 指定 rabbit 异常处理器
        simpleFactory.setErrorHandler(new CustomlRejectingErrorHandler());
        return simpleFactory;
    }
}
