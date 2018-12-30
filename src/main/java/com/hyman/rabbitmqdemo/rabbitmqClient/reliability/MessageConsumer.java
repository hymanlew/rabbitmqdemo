package com.hyman.rabbitmqdemo.rabbitmqClient.reliability;

import com.hyman.rabbitmqdemo.rabbitmqClient.ChannelUtils;
import com.hyman.rabbitmqdemo.util.Logutil;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;

/**
 * 在使用消息返回和消息确认机制时，能保证消息能够到达消息中间件并被正确路由到队列，但是在消费者消费消息时我们无法得到反馈信息，
 * 我们无法得知消息是否已经被消费成功。为了实现该功能RabbitMQ提供了Consumer Acknowledgements机制，使用它能在消费者消费消息后
 * 给Broker进行反馈，Broker根据反馈对消息进行处理。
 */
public class MessageConsumer {

    private final static String QUEUE_NAME = "testqueue";

    public static void main(String[] args) throws Exception{
        Channel channel = ChannelUtils.getchannel("test-channel-consumer");

        // 声明队列 (队列名, 是否持久化, 是否排他, 是否自动删除, 队列属性);
        AMQP.Queue.DeclareOk queue = channel.queueDeclare(QUEUE_NAME,true,false,false,new HashMap<>());

        // 声明交换机 (交换机名, 交换机类型, 是否持久化, 是否自动删除, 是否是内部交换机, 交换机属性);
        channel.exchangeDeclare("hymanexchange", BuiltinExchangeType.TOPIC,true,false,false,new HashMap<>());

        // 将队列 Binding 到交换机上 (队列名, 交换机名, binding key, 绑定属性);
        channel.queueBind(QUEUE_NAME,"hymanexchange","test.#",new HashMap<>());

        // 消费者订阅消息，监听如上声明的队列 (队列名, 是否自动应答(与消息可靠有关), 消费者标签, 消费者)，验证生产者的输出
        //channel.basicConsume(queue.getQueue(),true,"consumer1",new DefaultConsumer(channel)

        // 验证消费者的输出，不自动应答，而是交由 Broker（rabbitmq 服务器）进行处理。它会继续放入队列中让消费者消费，所以会一直循环。
        channel.basicConsume(queue.getQueue(),false,"consumer1",new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    Logutil.logger.info(consumerTag);
                    Logutil.logger.info(envelope.toString());
                    Logutil.logger.info(properties.toString());
                    Logutil.logger.info("消息内容："+new String(body));
                    if(new String(body).contains("消费者")){
                        throw new RuntimeException();
                    }else {
                        // 消费者成功消费信息
                        channel.basicAck(envelope.getDeliveryTag(),false);
                    }
                } catch (Exception e) {
                    Logutil.logger.info("消费者消费信息失败 =====");
                    channel.basicNack(envelope.getDeliveryTag(),false,true);
                }
            }
        });
    }
}
