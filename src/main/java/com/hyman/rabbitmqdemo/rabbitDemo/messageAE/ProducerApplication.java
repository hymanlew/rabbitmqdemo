package com.hyman.rabbitmqdemo.rabbitDemo.messageAE;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import javax.swing.table.TableRowSorter;
import java.util.HashMap;
import java.util.Map;

@ComponentScan(basePackages = "hyman.springbootdemo.rabbitmqSpring.messageAE")
public class ProducerApplication {

    public static void sendString(RabbitTemplate template, String s,String id){
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        properties.setContentType("utf-8");

        // 如果消费者设置了队列优先级，则发送消息时使用 setPriority() 方法指定消息优先级。
        properties.setPriority(5);
        // 设置消息将在三秒后过期
        properties.setExpiration("3000");
        Message message = new Message(s.getBytes(),properties);

        // 测试 AE
        //template.send("hymanDrect","test123",message,new CorrelationData(id));
        // 测试死信
        template.send("hymanDrect","test.#",message,new CorrelationData(id));
    }


    public static void main(String[] args) {
        // 获取注解的 bean 的上下文配置信息
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ProducerApplication.class);

        RabbitAdmin rabbitAdmin = context.getBean(RabbitAdmin.class);
        RabbitTemplate template = context.getBean(RabbitTemplate.class);

        // 声明AE 类型为Fanout
        rabbitAdmin.declareExchange(new FanoutExchange("hymanAE",true,false,new HashMap<>()));

        // 绑定 AE 到工作交换机，测试死信时先注释掉 AE 交换机的关联
        //Map<String,Object> eproperties = new HashMap<>();
        //eproperties.put("alternate-exchange","hymanAE");
        //rabbitAdmin.declareExchange(new DirectExchange("hymanDrect",true,false,eproperties));

        // 测试死信
        rabbitAdmin.declareExchange(new DirectExchange("hymanDrect",true,false,new HashMap<>()));
        sendString(template,"测试异常","id1");
        sendString(template,"真实异常","id2");
    }
}
