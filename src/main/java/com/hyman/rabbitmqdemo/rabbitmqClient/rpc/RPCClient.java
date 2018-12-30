package com.hyman.rabbitmqdemo.rabbitmqClient.rpc;

import com.hyman.rabbitmqdemo.rabbitmqClient.ChannelUtils;
import com.hyman.rabbitmqdemo.util.Logutil;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

// 创建 RPC 客户端
public class RPCClient {

    public static void main(String[] args) throws Exception{
        Channel channel = ChannelUtils.getchannel("RPC 系统 client 端");

        channel.queueDeclare("testqueue",true,false,false,new HashMap<>());
        channel.exchangeDeclare("hymanexchange", BuiltinExchangeType.TOPIC,true,false,false,new HashMap<>());
        channel.queueBind("testqueue","hymanexchange","test.#",new HashMap<>());

        String replyTo = "order.replay";
        AMQP.Queue.DeclareOk queue = channel.queueDeclare(replyTo,true,false,false,new HashMap<>());

        String correlationId = UUID.randomUUID().toString();
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder().deliveryMode(2).contentType("utf-8")
                .correlationId(correlationId).replyTo(replyTo).build();
        channel.basicPublish("hymanexchange","test",true,basicProperties,"订单信息".getBytes());

        channel.basicConsume(queue.getQueue(),true,"RPC 系统 client 端",new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                Logutil.logger.info("-------- RPC调用结果 --------");
                Logutil.logger.info(consumerTag);
                Logutil.logger.info("消息属性：" +properties.toString());
                Logutil.logger.info("消息内容："+new String(body));
            }
        });
    }
}
