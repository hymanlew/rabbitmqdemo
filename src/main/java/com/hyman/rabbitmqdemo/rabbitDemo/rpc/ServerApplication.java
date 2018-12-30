package com.hyman.rabbitmqdemo.rabbitDemo.rpc;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * 必须先启动消费者，再启动生产者，否则消费者接收不到数据（因为本 demo 设置的是自动路由接收信息，当 routingkey 与 bindingkey
 * 不匹配或是消费者连接失败时，消息会被自动丢弃）。
 */

@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.rpc")
public class ServerApplication {

    public static void main(String[] args) {
        new AnnotationConfigApplicationContext(ServerApplication.class);
    }
}
