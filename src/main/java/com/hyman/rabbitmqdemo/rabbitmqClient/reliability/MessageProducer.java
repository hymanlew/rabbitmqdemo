package com.hyman.rabbitmqdemo.rabbitmqClient.reliability;

import com.hyman.rabbitmqdemo.rabbitmqClient.ChannelUtils;
import com.hyman.rabbitmqdemo.util.Logutil;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;

/**
 * 消息可靠性（保证消息能够到达消息中间件并被正确路由到队列）：
 * 在项目中使用RabbitMQ时，我们可能会遇到这样的问题：如一个订单系统当用户付款成功时我们往消息中间件添加一条记录期望消息消费者
 * 修改订单状态，但是最终实际订单状态并没有被修改成功。遇到这种问题我们排查的思路如下:
 * 1.消息是否已经成功发送到消息中间件
 * 2.消息是否有丢失的情况 消息是否已经被消费成功
 *
 * 在生产环境下是不容许出现消息投递/消费错误的情况的，因为这可能会对企业产生巨大的损失。RabbitMQ如何保证消息的可靠性(生产者保
 * 证消息可靠投递，消费者保证消息可靠消费，RabbitMQ持久化)：
 *
 *
 * 生产者保证消息可靠投递（为了保证消息被正确投递到消息中间件，RabbitMQ提供了如下两个配置来保证消息投递的可靠性）：
 *
 * 1，在发送消息的时候我们可以设置 Mandatory 属性。如果设置了Mandatory属性则当消息不能被正确路由到队列中去时将会触发Return Method，
 * 这样我们可以在Return Method中进行相关业务处理，如果Mandatory没有设置则当消息不能正确路由到队列中去的时候，Broker将会丢弃该消息。
 *
 * 2，RabbitMQ还提供了消息确认机制(Publisher Confirm)。生产者将Channel设置成Confirm模式，当设置Confirm模式后所有在该信道上
 * 面发布的消息都会被指派一个唯一的ID(从1开始，ID在同个Channel范围是唯一的)，一旦消息被投递到所有匹配的队列之后Broker就会发送
 * 一个确认给生产者(包含消息的唯一ID)，这就使得生产者知道消息已经正确到达目的队列了。
 *
 * 如果消息和队列是可持久化的，那么确认消息会将消息写入磁盘之后出，Broker回传给生产者的确认消息中DeliverTag域包含了确认消息的
 * 序列号，此外Broker也可以设置basic.ack的multiple域，表示到这个序列号之前的所有消息都已经得到了处理（multiple如果为true则表
 * 示小于等于deliveryTag的消息都被投递成功，如果为false则表示只有等于deliveryTag的消息已经被投递成功）。
 *
 * 除了使用Publisher Confirm方式，RabbitMQ还提供了事务机制保证消息投递，但是使用事务会大大降低系统的吞吐量，就失去了消息中间
 * 件存在的意义，所以在这里不进行探讨。
 *
 * Publisher Confirm模式最大的好处在于他是异步的，一旦发布一条消息生产者应用程序就可以在等信道返回确认的同时继续发送下一条消息，
 * 当消息最终得到确认之后，生产者应用可以通过回调ACK方法来处理该确认消息，如果RabbitMQ因为自身内部错误导致消息丢失，生产者应用
 * 可以通过回调NACK方法来处理该确认消息。
 *
 * Publisher Confirm机制在性能上要比事务优越很多，但是Publisher Confirm机制无法进行回滚，一旦服务器崩溃生产者无法得到Confirm
 * 信息，生产者其实本身也不知道该消息是否已经被持久化，只有继续重发来保证消息不丢失，但是如果原先已经持久化的消息并不会被回滚，
 * 这样队列中就会存在两条相同的消息，系统需要支持去重。
 *
 */
public class MessageProducer {

    public static void main(String[] args) throws Exception{
        Channel channel = ChannelUtils.getchannel("test-channel-producer");

        // 声明交换机 (交换机名, 交换机类型, 是否持久化, 是否自动删除, 是否是内部交换机, 交换机属性);
        channel.exchangeDeclare("hymanexchange", BuiltinExchangeType.TOPIC,true,false,false,new HashMap<>());

        // 信道将交换器与交换器绑定
        //channel.exchangeBind("destination-目标","source-来源","routingkey");

        // 设置消息属性，deliveryMode（2）消息持久化，消息编码格式;
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder().deliveryMode(2).contentType("utf-8").build();

        // 当消息没有被正确路由时回调ReturnListener
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int i, String s, String s1, String s2, AMQP.BasicProperties basicProperties, byte[] bytes) throws IOException {
                Logutil.logger.info("replyCode："+ i);
                Logutil.logger.info("replyText："+ s);
                Logutil.logger.info("exchange："+ s1);
                Logutil.logger.info("routingKey："+ s2);
                Logutil.logger.info("properties："+ basicProperties);
                Logutil.logger.info("消息体："+ new String(bytes,"utf-8"));
            }
        });

        // 开启消息确认
        channel.confirmSelect();
        channel.addConfirmListener(new ConfirmListener() {
            // 当消息最终得到确认之后
            @Override
            public void handleAck(long l, boolean b) throws IOException {
                Logutil.logger.info("==== ACK 消息得到确认 ====");
                Logutil.logger.info("deliveryTag 消息传递标签："+ l);
                Logutil.logger.info("multiple 是否多个："+ b);
            }

            // 当 RabbitMQ 因为自身内部错误导致消息丢失时
            @Override
            public void handleNack(long l, boolean b) throws IOException {
                Logutil.logger.info("==== NACK 消息没有得到确认（由于内部异常） ====");
                Logutil.logger.info("deliveryTag 消息传递标签："+ l);
                Logutil.logger.info("multiple 是否多个："+ b);
            }
        });

        // 生产者发布消息 (交换机名, 路由关键字 Routing key, 可靠消息, 消息属性, 消息体);
        channel.basicPublish("hymanexchange","test",true,basicProperties,"测试信息可靠性".getBytes());
        channel.basicPublish("hymanexchange","test123",true,basicProperties,"测试失败信息".getBytes());
        channel.basicPublish("hymanexchange","test",false,basicProperties,"消费者消费消息测试".getBytes());

        Logutil.logger.info("send msg to [hymanexchange] exchange !");
        // 正常通信时不能关闭通道，否则消费者无法得到信息
        //channel.close();
        //ChannelUtils.closeConnect();

        /**
         * 根据输出结果说明了，当消息不能被确认路由时调用了ReturnListener 的 handleReturn方法。
         * 同时如果消息没有被正确路由仍然走的是ACK方法，Publisher Confirm只能保证消息到达消息中间件。如果要测试NACK方法可以
         * 通过在发送消息还没被确认时，停止RabbitMQ服务进行测试。
         */
    }
}
