package com.hyman.rabbitmqdemo.rabbitmqSpring.messageConverter;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

import java.io.UnsupportedEncodingException;

// 自定义字符串消息转换器
public class StringMessageConverter implements MessageConverter{

    @Override
    public Message toMessage(Object o, MessageProperties messageProperties) throws MessageConversionException {
        return null;
    }

    @Override
    public Object fromMessage(Message message) throws MessageConversionException {
        try {
            return new String(message.getBody(),"utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new MessageConversionException("StringMessageConverter转换失败", e);
        }
    }
}
