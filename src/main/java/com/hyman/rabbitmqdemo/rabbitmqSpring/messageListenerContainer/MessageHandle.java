package com.hyman.rabbitmqdemo.rabbitmqSpring.messageListenerContainer;

import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * 自定义消费者消息处理器类：
 * 在消息处理器类中使用 @RabbitListener 注解声明该类为RabbitMQ消息处理器类，并在bindings属性中声明了队列和交换机已经它们之间
 * 的绑定关系，使用 @RabbitHandler 注解声明具体消息处理方法。
 */
@Component
@RabbitListener(bindings = {
        @QueueBinding(value = @Queue(value = "testqueue",durable = "true",autoDelete = "false",exclusive = "false"),
                      exchange = @Exchange(name = "hymanexchange"))})
public class MessageHandle {

    @RabbitHandler
    public void add(byte[] body) {
        System.out.println("----------byte[]方法进行处理----------");

        try {
            String data = new String(body,"utf-8");
            if(data.contains("测试")){
                // 这里要注意，由于要重复请求的，但是它会一直抛出异常。所以会出现死循环的状态，是正常的结果
                throw new RuntimeException("测试异常 ==== 重新请求 ====");
            }else if(data.contains("真实")){
                throw new RuntimeException("真实异常 ==== 不再请求 ====");
            }else {
                System.out.println(data);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
