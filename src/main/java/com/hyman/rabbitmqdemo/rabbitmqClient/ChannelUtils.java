package com.hyman.rabbitmqdemo.rabbitmqClient;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ异常处理：
 * 使用JAVA客户端整合RabbitMQ进行的许多操作都会抛出异常，我们可以自定义异常处理器进行处理，比如我们希望在RabbitMQ消费消息失败
 * 时记录一条日志，又或者在消息消费失败时发送一则通知等操作。
 */

// 创建连接工具类 并设置异常处理器
public class ChannelUtils {

    private static Connection connect = null;

    // socket 与 rabbitmq 的连接工厂
    public static ConnectionFactory getConnectFactory(){
        ConnectionFactory factory = new ConnectionFactory();

        // 配置连接信息
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setVirtualHost("hymanhost");
        factory.setUsername("hyman");
        factory.setPassword("hyman");

        // 网络异常自动连接恢复
        factory.setAutomaticRecoveryEnabled(true);
        // 每10秒尝试重试连接一次
        factory.setNetworkRecoveryInterval(10000);

        // 设置ConnectionFactory属性信息
        Map<String,Object> factoryPropertiesMap = new HashMap<>();
        factoryPropertiesMap.put("name","hyman");
        factoryPropertiesMap.put("ip","127.0.0.1");
        factory.setClientProperties(factoryPropertiesMap);

        // 设置自定义异常处理器，但是这个一定要确认好，否则消息失败的消息不会再重新放入队列，而会直接丢弃。
        //factory.setExceptionHandler(new DefaultExceptionHandler(){
        //    @Override
        //    public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
        //        Logutil.logger.error("----------消息消费异常处理----------");
        //        Logutil.logger.error("异常日志："+exception.getMessage());
        //        super.handleConsumerException(channel, exception, consumer, consumerTag, methodName);
        //    }
        //});
        return factory;
    }

    // 创建连接通道
    public static Channel getchannel(String connectionmsg){
        try {
            ConnectionFactory factory = getConnectFactory();
            connect = factory.newConnection(connectionmsg);
            return connect.createChannel();
        }catch (Exception e){
            throw new RuntimeException("获取 channel 连接失败！");
        }
    }

    // 关闭连接通道
    public static void closeConnect(){
        try {
            if(connect != null){
                connect.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
