package com.hyman.rabbitmqdemo.rabbitmqClient;

import com.hyman.rabbitmqdemo.util.Logutil;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;

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

        // 消费者订阅消息，监听如上声明的队列 (队列名, 是否自动应答(与消息可靠有关), 消费者标签, 消费者)
        channel.basicConsume(queue.getQueue(),true,"consumer1",new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                Logutil.logger.info(consumerTag);
                Logutil.logger.info(envelope.toString());
                Logutil.logger.info(properties.toString());
                Logutil.logger.info("消息内容："+new String(body));

                /**
                 * 手动抛出一个异常：
                 * 当消费消息出现异常时，会进入我们自定义异常处理的逻辑。需要注意的是默认 RabbitMQ Java Client 在发生异常时会
                 * 将Channel/Connection关闭，进而使程序未能按照预期的方向执行，所以我们在软件设计的时候应当考虑周全。
                 */
                throw new RuntimeException("消息消费出现异常");
            }
        });


        try {
            // 上述代码。
        } catch (Exception e) {

            // 尽量不要自己处理掉异常，而是返回给上层调用。
            e.printStackTrace();
            System.out.println("接受消息失败！");
        }
    }

}
