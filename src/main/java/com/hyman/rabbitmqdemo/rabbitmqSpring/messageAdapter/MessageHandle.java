package com.hyman.rabbitmqdemo.rabbitmqSpring.messageAdapter;

import java.io.UnsupportedEncodingException;

/**
 * MessageListenerAdapte（消息监听处理适配器）：
 * 在 manual，auto 两个 Demo 中，在声明消息消费处理逻辑时往 MessageListenerContainer 中传递了 MessageListener，但是我们有时
 * 候已经写好了消费逻辑对应的类，我们不希望它去扩展 MessageListener/ChannelAwareMessageListener，因为这么做的话意味着我们需
 * 要改变现有代码。
 *
 * Spring AMQP提供了消息处理器适配器的功能，它可以把一个纯POJO类适配成一个可以处理消息的处理器，默认处理消息的方法为 handleMessage，
 * 可以通过 setDefaultListenerMethod 方法进行修改。
 *
 * 创建消费者消息处理器类，它可是是纯POJO类：
 */
public class MessageHandle {

    /**
     * 自定义处理器方法的参数默认情况下为byte[]类型，这是由Spring AMQP默认消息转换器(SimpleMessageConverter)决定的。
     * @param message
     */
    public void add(byte[] message){
        try {
            System.out.println("自定义消息处理器 === "+new String(message,"utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
