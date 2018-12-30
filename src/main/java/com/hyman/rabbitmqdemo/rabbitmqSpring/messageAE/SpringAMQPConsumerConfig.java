package com.hyman.rabbitmqdemo.rabbitmqSpring.messageAE;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Alternate Exchange简称AE，当消息不能被正确路由时，如果交换机设置了AE则消息会被投递到AE中，如果存在AE链则会按此继续投递，直
 * 到消息被正确路由或AE链结束消息被丢弃。通常建议AE的交换机类型为 Fanout（广播式路由，即路由到所有与它绑定的Queue中）防止出现
 * 路由失败，如果一个交换机指定了AE，那么意为着该交换机和AE链都无法被正确路由时才会触发消息返回。
 *
 * alternate-exchange 是 RabbitMQ自己扩展的功能，不是AMQP协议定义的。在创建Exchange时指定该 x-arguments 为 alternate-exchange属性，
 * 则发送消息的时候根据route key没有找到可以投递的队列，这就会将此消息路由到 Alternate Exchange 属性指定的 Exchange (就是一个普通的
 * exchange)上了。
 * 并且它只支持附加到 DIRECT，FANOUT，header 类型的交换器上，不支持 TOPIC。
 *
 *
 * 消息数量限制 Length Limit：
 * 我们可以为队列设置 x-max-length 来指定队列中可存放的最大消息数量，或者设置 max-length-bytes来指定队列中存放消息内容的最大
 * 字节长度，当超过这个长度后部分消息将成为死信。
 *
 *
 * 死信队列 Dead Letter Exchange：
 * 如果队列中设置了Dead Letter Exchange属性，当消息变成死信后它能重写被投递到另一个交换机。消息变成死信一般有如下几种情况：
 *
 * 1.消息过期而被删除
 * 2.消息数量超过队列最大限制而被删除
 * 3.消息总大小超过队列最大限制而被删除
 * 4.消息被拒绝(channel.basicReject()/channel.basicNack())，且 requeue 为false（即自动重入队列的条件在异常中设定，或是设置
 *   消息自动应答为 false）。
 *
 * 同时也可以指定一个可选的x-dead-letter-routing-key表示默认的routing-key，如果没有指定则使用消息的routing-key。
 *
 *
 * 优先级队列 Priority Queue：
 * 1，创建优先级队列需增加 x-max-priority 参数（指定一个数字，数值越大则优先级越高）
 * 2，发送消息的时候需要设置 priority 属性，最好不要超过上面指定的 x-max-priority，如果超过了x-max-priority 则为 x-max-priority，
 *    需要注意如果生产者发送消息较慢，消费者消费速度较快则可能不会严格的按照优先级队列进行消费。
 *
 */
@Configuration
public class SpringAMQPConsumerConfig {

    // 自动声明交换机，如果要一次性声明多个则使用 public List<Exchange> listExchange() 即可
    @Bean
    public List<Exchange> exchanges(){
        FanoutExchange hymanAE = new FanoutExchange("hymanAE",true,false,new HashMap<>());

        // 在测试死信队列时，要注释掉
        //Map<String,Object> eproperties = new HashMap<>();
        //eproperties.put("alternate-exchange", "hymanAE");
        //DirectExchange directExchange = new DirectExchange("hymanDrect",true,false,eproperties);

        DirectExchange directExchange = new DirectExchange("hymanDrect",true,false,new HashMap<>());
        return Arrays.asList(hymanAE,directExchange);
    }

    // 自动声明队列，如果要一次性声明多个则使用 public List<Queue> listQueue() 即可
    @Bean
    public List<Queue> queues(){
        // 声明队列 (队列名, 是否持久化, 是否排他, 是否自动删除, 队列属性);
        Map<String,Object> queueProperties = new HashMap<>();
        // 设置队列中的消息将在5秒后过期
        //queueProperties.put("x-message-ttl",5000);
        // 指定队列中可存放的最大消息数量
        //queueProperties.put("x-max-length", 2);
        // 设置队列可存放消息内容的最大字节长度为20
        //queueProperties.put("x-max-length-bytes", 20);
        // 创建队列时设置队列优先级
        queueProperties.put("x-max-priority", 10);
        // 死信队列，声明时要特别注意不能重复定义同一个队列。
        queueProperties.put("x-dead-letter-exchange","hymanAE");
        Queue queue = new Queue("testqueue",true,false,false,queueProperties);

        Queue queue2 = new Queue("testqueue2",true,false,false,new HashMap<>());
        return Arrays.asList(queue,queue2);
    }

    // 自动声明绑定，如果要一次性声明多个则使用 public List<Binding> listBinding() 即可
    @Bean
    public List<Binding> bindings(){
        // 要绑定到的目标，目标类型，交换器名，routing key，参数map
        //Binding bind = new Binding("testqueue",Binding.DestinationType.QUEUE,"hymanDrect","test.#",new HashMap<>());
        Binding bind2 = new Binding("testqueue2",Binding.DestinationType.QUEUE,"hymanAE","",new HashMap<>());

        // 最好使用这种方式声明
        Binding bind = BindingBuilder.bind(new Queue("testqueue")).to(new DirectExchange("hymanDrect")).with("test.#");
        return Arrays.asList(bind,bind2);
    }

    @Bean
    public MessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory){
        // 声明消息监听容器
        SimpleMessageListenerContainer messagelistener = new SimpleMessageListenerContainer();
        messagelistener.setConnectionFactory(connectionFactory);
        messagelistener.setQueueNames("testqueue");

        // 设置消费者线程数
        messagelistener.setConcurrentConsumers(5);
        // 设置最大消费者线程数
        messagelistener.setMaxConcurrentConsumers(10);
        // 设置消费者属性信息
        Map<String,Object> argumentMap = new HashMap<>();
        messagelistener.setConsumerArguments(argumentMap);

        // 设置消费者标签
        messagelistener.setConsumerTagStrategy(new ConsumerTagStrategy() {
            @Override
            public String createConsumerTag(String s) {
                return "rabbitmq exchange 测试";
            }
        });

        // 声明消息监听器，测试 AE
        //messagelistener.setMessageListener(new MessageListener() {
        //    @Override
        //    public void onMessage(Message message) {
        //        try {
        //            System.out.println("==== "+new String(message.getBody(),"utf-8"));
        //            System.out.println("==== "+message.getMessageProperties());
        //        } catch (UnsupportedEncodingException e) {
        //            e.printStackTrace();
        //        }
        //    }
        //});

        // 声明消息监听器，测试死信
        messagelistener.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        messagelistener.setMessageListener(new ChannelAwareMessageListener() {
            @Override
            public void onMessage(Message message, Channel channel) throws Exception{
                System.out.println("死信 == "+new String(message.getBody(),"utf-8"));
                System.out.println("死信 == "+message.getMessageProperties());
                // 测试死信队列，必须手动设置为死信
                channel.basicNack(message.getMessageProperties().getDeliveryTag(),false,false);
            }
        });
        return messagelistener;
    }

    @Bean
    public MessageListenerContainer messageListenerContainer2(ConnectionFactory connectionFactory){
        // 声明消息监听容器
        SimpleMessageListenerContainer messagelistener = new SimpleMessageListenerContainer();
        messagelistener.setConnectionFactory(connectionFactory);
        messagelistener.setQueueNames("testqueue2");

        messagelistener.setConcurrentConsumers(5);
        messagelistener.setMaxConcurrentConsumers(10);

        Map<String,Object> argumentMap = new HashMap<>();
        messagelistener.setConsumerArguments(argumentMap);

        // 设置消费者标签
        messagelistener.setConsumerTagStrategy(new ConsumerTagStrategy() {
            @Override
            public String createConsumerTag(String s) {
                return "rabbitmq AE exchange 测试";
            }
        });

        // 声明消息监听器
        messagelistener.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("AE ==== "+new String(message.getBody(),"utf-8"));
                    System.out.println("AE ==== "+message.getMessageProperties());
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        });
        return messagelistener;
    }
}
