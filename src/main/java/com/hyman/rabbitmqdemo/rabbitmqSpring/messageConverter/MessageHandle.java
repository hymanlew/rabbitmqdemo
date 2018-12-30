package com.hyman.rabbitmqdemo.rabbitmqSpring.messageConverter;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * MessageListenerAdapte（消息监听处理适配器）：
 * Spring AMQP提供了消息处理器适配器的功能，它可以把一个纯POJO类适配成一个可以处理消息的处理器，默认处理消息的方法为 handleMessage，
 * 可以通过 setDefaultListenerMethod 方法进行修改。
 *
 * 创建的消费者消息处理器类，它可是是纯POJO类。并且其自定义处理器方法的参数默认情况下为byte[]类型，这是由Spring AMQP默认消息转
 * 换器(SimpleMessageConverter)决定的。
 *
 * Spring AMQP消息转换器：
 * 在上诉例子中我们定义的add(byte[] message)方法的参数是一个字节数组，但是有时候我们往 RabbitMQ 中发送的是一个JSON对象，我们希
 * 望在处理消息的时候它已经自动帮我们转为JAVA对象；又或者我们往RabbitMQ中发送的是一张图片或其他格式的文件，我们希望在处理消息的
 * 时候它已经自动帮我们转成文件格式，我们可以手动设置 MessageConverter 来实现如上需求，如果未设置 MessageConverter 则使用
 * Spring AMQP默认提供的SimpleMessageConverter。
 *
 *
 * 本例子就是使用 MessageConverter实现了当生产者往RabbitMQ发送不同类型的数据的时候，使用 MessageHandle不同的方法进行处理，需要
 * 注意的是当生产者在发送 JSON数据的时候，需要制定这个 JSON 是哪个对象，用于Spring AMQP转换，规则如下：
 *
 * 1，当发送普通对象的JSON数据时，需要在消息的header中增加一个__TypeId__的属性告知消费者是哪个对象。
 * 2，当发送List集合对象的JSON数据时，需要在消息的header中将__TypeId__指定为java.util.List，并且需要额外指定__ContentTypeId__
 *    属性用于告知消费者List集合中的对象类型
 * 3，当发送Map集合对象的JSON数据时，需要在消息的header中将__TypeId__指定为java.util.Map，并且需要额外指定__KeyTypeId__属性用
 *    于告知客户端Map中key的类型，__ContentTypeId__用于告知客户端Map中Value的类型
 *
 */
public class MessageHandle {

    public void add(byte[] body) {
        System.out.println("----------byte[]方法进行处理----------");
        System.out.println("body");
    }

    public void add(String message) {
        System.out.println("----------String方法进行处理----------");
        System.out.println(message);
    }

    public void add(File file) {
        System.out.println("----------File方法进行处理----------");
        System.out.println(file.length());
        System.out.println(file.getName());
        System.out.println(file.getAbsolutePath());
    }

    public void add(Order order) {
        System.out.println("----------Order方法进行处理----------");
        System.out.println(order.getOrderId() + "---" + order.getOrderAmount());
    }

    public void add(List<Order> orderList) {
        System.out.println("----------List<Order>方法进行处理----------");
        System.out.println(orderList.size());
        for (Order order : orderList) {
            System.out.println(order.getOrderId() + "---" + order.getOrderAmount());
        }
    }

    public void add(Map<String, Order> orderMap) {
        System.out.println("----------Map<String, Order>方法进行处理----------");
        for (Map.Entry<String, Order> entry : orderMap.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue().getOrderId() + "---" + entry.getValue().getOrderAmount());
        }
    }
}
