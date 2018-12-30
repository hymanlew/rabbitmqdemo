package com.hyman.rabbitmqdemo.rabbitmqClient;

import com.hyman.rabbitmqdemo.util.Logutil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.util.HashMap;

public class MessageProducer {

    public static void main(String[] args) throws Exception{
        Channel channel = ChannelUtils.getchannel("test-channel-producer");

        // 声明交换机 (交换机名, 交换机类型, 是否持久化, 是否自动删除, 是否是内部交换机, 交换机属性);
        channel.exchangeDeclare("hymanexchange", BuiltinExchangeType.TOPIC,true,false,false,new HashMap<>());

        // 信道将交换器与交换器绑定
        //channel.exchangeBind("destination-目标","source-来源","routingkey");

        // 设置消息属性，deliveryMode（2）消息持久化，消息编码格式;
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder().deliveryMode(2).contentType("utf-8").build();

        // 生产者发布消息 (交换机名, 路由关键字 Routing key, 可靠消息相关属性, 消息属性, 消息体);
        String msg = "测试发送信息";
        channel.basicPublish("hymanexchange","test",false,basicProperties,msg.getBytes());

        Logutil.logger.info("send msg "+ msg + " to [hymanexchange] exchange !");

        // 正常通信时不能关闭通道，否则消费者无法得到信息
        //channel.close();
        //ChannelUtils.closeConnect();


        try {
            // 上述代码。
        } catch (Exception e) {

            // 尽量不要自己处理掉异常，而是返回给上层调用。
            e.printStackTrace();
            System.out.println("发送消息失败！");
        }
    }
}
