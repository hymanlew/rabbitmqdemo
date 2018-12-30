package com.hyman.rabbitmqdemo.rabbitmqSpring.messageReliability;

import com.hyman.rabbitmqdemo.util.Logutil;
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
import java.util.HashMap;
import java.util.Map;

/**
 * 在使用消息返回和消息确认机制时，能保证消息能够到达消息中间件并被正确路由到队列，但是在消费者消费消息时我们无法得到反馈信息，
 * 我们无法得知消息是否已经被消费成功。为了实现该功能RabbitMQ提供了Consumer Acknowledgements机制，使用它能在消费者消费消息后
 * 给Broker进行反馈，Broker根据反馈对消息进行处理。
 *
 * 这里保证了生产者和消费者消息的可靠性，但是假设在运行过程中RabbitMQ服务端宕机了，若此前没有进行持久化操作则消息就会丢失。所以
 * 使用RabbitMQ，通常建议开启持久化功能。
 * 交换机持久化：在声明时指定durable为true。
 * 队列持久化：在声明时指定durable为true。
 * 消息持久化：在声明时指定delivery_mode为2。
 *
 * 这样才能保证 RabbitMQ 消息的可靠性。
 */

@Configuration
public class SpringAMQPConsumerConfig {

    // 自动声明交换机，如果要一次性声明多个则使用 public List<Exchange> listExchange() 即可
    @Bean
    public Exchange exchange(){
        return new TopicExchange("hymanexchange",true,false,new HashMap<>());
    }

    // 自动声明队列，如果要一次性声明多个则使用 public List<Queue> listQueue() 即可
    @Bean
    public Queue queue(){
        // 声明队列 (队列名, 是否持久化, 是否排他, 是否自动删除, 队列属性);
        return new Queue("testqueue",true,false,false,new HashMap<>());
    }

    // 自动声明绑定，如果要一次性声明多个则使用 public List<Binding> listBinding() 即可
    @Bean
    public Binding binding(){
        // 要绑定到的目标，目标类型，交换器名，routing key，参数map
        return new Binding("testqueue",Binding.DestinationType.QUEUE,"hymanexchange","test.#",new HashMap<>());
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
                return "消息测试-可靠性";
            }
        });

        // 使用 setAutoStartup 方法设置自动消息消费时机，自动对 broker（rabbitmq 服务器）进行处理响应。
        //messagelistener.setAutoStartup(true);

        /**
         * 设置消息确认模式为手动模式（MANUAL）。
         *
         * AcknowledgeMode 有三个可选值分别是：NONE，MANUAL，AUTO：
         *
         * NONE，为自动确认等效于 autoAck=true。
         * MANUAL，手动确认等效于autoAck=false。
         * AUTO，是根据方法的执行情况来决定是确认还是拒绝，如果消息被成功消费了则自动确认，如果在消费消息时抛出
         *       AmqpRejectAndDontRequeueException 则消息会被拒绝并且不会重新入队列，如果抛出 ImmediateAcknowledgeAmqpException
         *       则消息会被确认，如果抛出其他异常则消息会被拒绝，并且重新入队列。
         *
         * 更多详细信息可查阅 SimpleMessageListenerContainer 的 doReceiveAndExecute() 方法进行获取。
         */
        messagelistener.setAcknowledgeMode(AcknowledgeMode.MANUAL);

        // 声明消息监听器
        messagelistener.setMessageListener(new ChannelAwareMessageListener() {
            @Override
            public void onMessage(Message message, Channel channel) throws IOException {
                try {
                    String data = new String(message.getBody(),"utf-8");
                    Logutil.logger.info(" ==== "+data);
                    Logutil.logger.info(" ==== "+message.getMessageProperties());
                    if(data.contains("真实")){
                        throw new RuntimeException();
                    }else {
                        // 消费者成功消费信息，进行确认
                        channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
                    }
                } catch (Exception e) {
                    Logutil.logger.info("消费信息失败，拒绝消费 =====");
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(),false,true);
                }
            }
        });

        return messagelistener;
    }
}
