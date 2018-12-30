package com.hyman.rabbitmqdemo.rabbitDemo.messageConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyman.rabbitmqdemo.rabbitmqSpring.messageConverter.Order;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * 生产者启动类：
 *
 * RabbitAdmin: 用于声明交换机、队列、绑定等。
 * RabbitTemplate: 用于 RabbitMQ 消息的发送和接收。
 * MessageListenerContainer: 监听容器，为消息入队提供异步处理。
 */

@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.messageConverter")
public class ProducerApplication {

    public static void sendString(RabbitTemplate template, String s){
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        properties.setContentType("text/plain");
        Message message = new Message(s.getBytes(),properties);
        template.send("hymanexchange","test.me",message);
    }

    public static void sendObject(RabbitTemplate template,Order order){
        /**
         * 该类是Jackson库的主要类。它将转换Java对象到匹配的JSON结构，反之亦然。使用JsonParser和JsonGenerator的实例实现
         * JSON实际的读/写。
         */
        ObjectMapper objectMapper = new ObjectMapper();

        MessageProperties properties = new MessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        /**
         * 当发送普通对象的JSON数据时，需要在消息的header中增加一个__TypeId__的属性告知消费者是哪个对象。
         */
        properties.getHeaders().put("__TypeId__", "Order");
        properties.setContentType("application/json");

        try {
            Message message = new Message(objectMapper.writeValueAsString(order).getBytes(),properties);
            template.send("hymanexchange","test.me",message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public static void sendObjList(RabbitTemplate template){
        Order o1 = new Order("002",new BigDecimal(150));
        Order o2 = new Order("003",new BigDecimal(200));

        // 数组转 list 集合的简化方法，如同：
        // int[] a = new int[3];
        // Arrays.asList(a);
        List<Order> orders = Arrays.asList(o1,o2);
        ObjectMapper objectMapper = new ObjectMapper();

        MessageProperties properties = new MessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        properties.setContentType("application/json");
        /**
         * 当发送List集合对象的JSON数据时，需要在消息的header中将__TypeId__指定为java.util.List，并且需要额外指定__ContentTypeId__
         * 属性用于告知消费者List集合中的对象类型
         */
        properties.getHeaders().put("__TypeId__", "java.util.List");
        properties.getHeaders().put("__ContentTypeId__", "Order");

        try {
            Message message = new Message(objectMapper.writeValueAsString(orders).getBytes(),properties);
            template.send("hymanexchange","test.me",message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public static void sendMap(RabbitTemplate template){
        Order o1 = new Order("004",new BigDecimal(250));
        Order o2 = new Order("005",new BigDecimal(300));
        Map<String,Order> map = new HashMap<>();
        map.put(o1.getOrderId(),o1);
        map.put(o2.getOrderId(),o2);

        ObjectMapper objectMapper = new ObjectMapper();
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        properties.setContentType("application/json");
        /**
         * 当发送Map集合对象的JSON数据时，需要在消息的header中将__TypeId__指定为java.util.Map，并且需要额外指定__KeyTypeId__
         * 属性用于告知客户端Map中key的类型，__ContentTypeId__用于告知客户端Map中Value的类型
         */
        properties.getHeaders().put("__TypeId__", "java.util.Map");
        properties.getHeaders().put("__KeyTypeId__", "java.lang.String");
        properties.getHeaders().put("__ContentTypeId__", "Order");

        try {
            Message message = new Message(objectMapper.writeValueAsString(map).getBytes(),properties);
            template.send("hymanexchange","test.me",message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public static void sendImage(RabbitTemplate template){
        try {
            File file = new File("E:"+File.separator+"dog.jpg");
            FileInputStream inputStream = new FileInputStream(file);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);

            int lenth;
            byte[] bytes = new byte[1024*10];
            while ((lenth = inputStream.read(bytes)) != -1){
                outputStream.write(bytes,0,lenth);
            }
            byte[] datas = outputStream.toByteArray();
            inputStream.close();
            outputStream.close();

            MessageProperties properties = new MessageProperties();
            properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            properties.setContentType("image/jpg");
            properties.getHeaders().put("fileType", "jpg");

            Message message = new Message(datas,properties);
            template.send("hymanexchange","test.me",message);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        // 获取注解的 bean 的上下文配置信息
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ProducerApplication.class);

        RabbitAdmin rabbitAdmin = context.getBean(RabbitAdmin.class);
        RabbitTemplate template = context.getBean(RabbitTemplate.class);

        // 绑定交换机
        rabbitAdmin.declareExchange(new TopicExchange("hymanexchange",true,false,new HashMap<>()));

       sendString(template,"字符串");
       sendObject(template,new Order("001",new BigDecimal(100)));
       sendObjList(template);
       sendMap(template);
       sendImage(template);
    }
}
