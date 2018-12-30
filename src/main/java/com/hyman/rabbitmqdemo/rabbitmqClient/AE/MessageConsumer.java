package com.hyman.rabbitmqdemo.rabbitmqClient.AE;

import com.hyman.rabbitmqdemo.rabbitmqClient.ChannelUtils;
import com.hyman.rabbitmqdemo.util.Logutil;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageConsumer {

    private final static String QUEUE_NAME = "testqueue";

    public static void main(String[] args) throws Exception{
        Channel channel = ChannelUtils.getchannel("test-channel-consumer");

        // 声明队列 (名, 持久化, 排他, 自动删除, 队列属性);
        Map<String,Object> queueProperties = new HashMap<>();
        // 设置队列中的消息将在5秒后过期
        //queueProperties.put("x-message-ttl",5000);
        // 指定队列中可存放的最大消息数量
        queueProperties.put("x-max-length", 10);
        // 设置队列可存放消息内容的最大字节长度为20
        queueProperties.put("x-max-length-bytes", 20);
        // 创建队列时设置队列为最大优先级
        queueProperties.put("x-max-priority", 10);
        // 设置队列关联到死信队列交换机，如果设置它，则就不用将死信（或 AE）交换机关联到普通交换机，否则会冲突。
        // 并且消息队列的属性也不能修改，否则会出异常
        queueProperties.put("x-dead-letter-exchange","hymanAE");
        AMQP.Queue.DeclareOk queue = channel.queueDeclare(QUEUE_NAME,true,false,false,queueProperties);

        // 声明 AE 或者 dead exchange 类型为Fanout
        channel.exchangeDeclare("hymanAE",BuiltinExchangeType.FANOUT,true,false,false,new HashMap<>());

        // 设置 AE 交换机属性到普通交换机 (交换机名, 交换机类型, 是否持久化, 是否自动删除, 是否是内部交换机, 交换机属性);
        // 如果设置了死信队列则，就不能将死信（或 AE）交换机关联到普通交换机，否则会冲突
        //Map<String,Object> exchangep = new HashMap<>();
        //exchangep.put("alternate-exchange","hymanAE");

        // 注意这里只能使用 DIRECT 类型的直连交换器（把消息路由到那些binding key与routing key完全匹配的Queue中）。使用 topic 类型不可以。
        //channel.exchangeDeclare("hymanDrect", BuiltinExchangeType.DIRECT,true,false,false,exchangep);
        //channel.queueBind(queue.getQueue(),"hymanDrect","test.#",new HashMap<>());
        //channel.exchangeDeclare("hymanFanout", BuiltinExchangeType.FANOUT,true,false,false,exchangep);

        channel.exchangeDeclare("hymanDrect", BuiltinExchangeType.DIRECT,true,false,false,new HashMap<>());
        channel.queueBind(queue.getQueue(),"hymanDrect","test.#",new HashMap<>());

        // 将 queue2 队列绑定到 hymanAE 交换机上，无需指定 routing key
        AMQP.Queue.DeclareOk queue2 = channel.queueDeclare("testqueue2",true,false,false,new HashMap<>());
        channel.queueBind(queue2.getQueue(),"hymanAE","",new HashMap<>());

        // 消费 queue 队列消息(队列名, 是否自动应答(与消息可靠有关), 消费者标签, 消费者)
        channel.basicConsume(queue.getQueue(),false,"consumer1",new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    Logutil.logger.info("正常队列 === "+queue.getQueue());
                    Logutil.logger.info("消息内容："+new String(body));
                    // 返回消息属性
                    //channel.basicAck(envelope.getDeliveryTag(),false);
                    // 测试死信队列，必须手动设置为死信
                    channel.basicNack(envelope.getDeliveryTag(),false,false);
                } catch (IOException e) {
                    channel.basicNack(envelope.getDeliveryTag(),false,true);
                }
            }
        });

        channel.basicConsume(queue2.getQueue(),false,"consumer-AE",new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    Logutil.logger.info("AE 队列 === "+queue.getQueue());
                    Logutil.logger.info("消息内容："+new String(body));
                    channel.basicAck(envelope.getDeliveryTag(),false);
                } catch (IOException e) {
                    Logutil.logger.info("AE1 队列 === "+queue.getQueue());
                    channel.basicNack(envelope.getDeliveryTag(),false,true);
                }
            }
        });
    }

}
