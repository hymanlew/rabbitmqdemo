package com.hyman.rabbitmqdemo.rabbitmqSpring.rpc;

import com.hyman.rabbitmqdemo.util.Logutil;

/**
 * RPC：
 * MQ 消息中间件本身是基于异步的消息处理，示例中所有的生产者（P）将消息发送到RabbitMQ后不会知道消费者（C）处理成功或者失败（甚
 * 至连有没有消费者来处理这条消息都不知道）。但实际的应用场景中，我们很可能需要一些同步处理，需要同步等待服务端将我的消息处理完
 * 成后再进行下一步处理。这相当于RPC（Remote Procedure Call，远程过程调用）。在RabbitMQ中也支持RPC。
 *
 * RabbitMQ 中实现RPC 的机制是：
 * 1，客户端发送请求（消息）时，在消息的属性中（MessageProperties，在AMQP 协议中定义了14中properties，这些属性会随着消息一起
 *    发送）设置两个值replyTo （一个Queue 名称，用于告诉服务器处理完成后将通知我的消息发送到这个Queue 中）和 correlationId
 *    （此次请求的标识号，服务器处理完成后需要将此属性返还，客户端将根据这个id了解哪条请求被成功执行了或执行失败）。
 *
 * 2，服务器端收到消息并处理
 * 3，服务器端处理完消息后，将生成一条应答消息到replyTo 指定的Queue ，同时带上correlationId 属性
 * 4，客户端之前已订阅replyTo 指定的Queue，从中收到服务器的应答消息后，根据其中的 correlationId 属性分析哪条请求被执行了，根
 *    据执行结果进行后续业务处理。
 *
 *
 * 服务端步骤:
 1.服务端监听一个队列，监听客户端发送过来的消息
 2.收到消息之后调用RPC服务得到调用结果
 3.从消息属性中获取reply_to，correlation_id属性，把调用结果发送给reply_to指定的队列，发送的消息属性要带上correlation_id
 *
 * 客户端步骤:
 1.监听reply_to队列
 2.发送消息，消息属性需要带上reply_to,correlation_id
 3.服务端处理完之后reply_to对应的队列就会收到异步处理结果消息
 4.收到消息之后进行处理，根据消息的correlation_id找到对应的请求
 */

// 创建模拟 RPC 服务调用方法
public class RPCMethod {

    // 参数只能是 byte 数组类型
    public static String addOrder(byte[] orderInfo){
        try {
            Logutil.logger.info(new String(orderInfo,"utf-8"));
            Logutil.logger.info("orderInfo已添加到数据库");
            return "订单ID";
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}
