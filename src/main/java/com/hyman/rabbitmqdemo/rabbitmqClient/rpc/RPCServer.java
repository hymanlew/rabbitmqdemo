package com.hyman.rabbitmqdemo.rabbitmqClient.rpc;

import com.hyman.rabbitmqdemo.rabbitmqClient.ChannelUtils;
import com.hyman.rabbitmqdemo.util.Logutil;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;

// 创建RPC服务端
public class RPCServer {

    public static void main(String[] args) throws Exception{
        Channel channel = ChannelUtils.getchannel("RPC 系统 server 端");

        channel.queueDeclare("testqueue",true,false,false,new HashMap<>());
        channel.exchangeDeclare("hymanexchange", BuiltinExchangeType.TOPIC,true,false,new HashMap<>());

        // 消费者订阅消息，监听队列 (队列名, 是否自动应答(与消息可靠有关), 消费者标签, 消费者)，验证生产者的输出
        channel.basicConsume("testqueue",true,"consumer-1",new DefaultConsumer(channel){
            // delivery：调度信息的传递
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String replyTo = properties.getReplyTo();
                String correlationId = properties.getCorrelationId();

                Logutil.logger.info("-------- 收到 RPC 调用请求 --------");
                Logutil.logger.info("tag == "+consumerTag);
                Logutil.logger.info("消息属性：" + properties.toString());
                Logutil.logger.info("消息内容：" + new String(body));

                try {
                    String orderId = RPCMethod.addOrder(new String(body));
                    AMQP.BasicProperties repleyProperties = new AMQP.BasicProperties().builder().deliveryMode(2).contentType("utf-8")
                            .correlationId(correlationId).build();
                    channel.basicPublish("",replyTo,repleyProperties,orderId.getBytes());
                    Logutil.logger.info("-------- RPC调用成功 结果已返回 --------");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
