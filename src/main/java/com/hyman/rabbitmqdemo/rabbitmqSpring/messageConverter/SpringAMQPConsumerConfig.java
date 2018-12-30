package com.hyman.rabbitmqdemo.rabbitmqSpring.messageConverter;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.ClassMapper;
import org.springframework.amqp.support.converter.ContentTypeDelegatingMessageConverter;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.GsonHttpMessageConverter;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * RabbitAdmin: 用于声明交换机、队列、绑定等。
 * RabbitTemplate: 用于 RabbitMQ 消息的发送和接收。
 * MessageListenerContainer: 消息监听容器，为消息入队提供异步处理。
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

    // 不可用，因为自定义 json 转换器抛异常
    //@Resource
    //private Jackson2JsonMessageConverter jsonMessageConverter;

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
                return "spring-自定义消息转换器";
            }
        });
        messagelistener.setAutoStartup(true);

        // 新建消息处理器适配器
        MessageListenerAdapter messageAdapter = new MessageListenerAdapter(new MessageHandle());
        messageAdapter.setDefaultListenerMethod("handleMessage");
        // 声明消息处理器适配器属性，并使用 add方法进行处理
        Map<String,String> adapterConfig = new HashMap<>();
        adapterConfig.put("testqueue","add");
        messageAdapter.setQueueOrTagToMethodName(adapterConfig);
        messagelistener.setMessageListener(messageAdapter);

        // 设置消息转换器
        /**
         * ContentTypeDelegatingMessageConverter，它能根据不同的MessageProperties属性(contentType)决定来委托给具体的哪一个
         * MessageConverter。
         * 消息数据类型委派转换器，其本身不做消息转换的具体动作，而是将消息转换委托给具体的MessageConverter。我们可以设置 COntentType
         * 和 MessageConverter 的映射关系。
         *
         * 在经过消息转化器后，Spring AMQP会根据最后转换结果的类型找到对应的消息处理方法。
         */
        ContentTypeDelegatingMessageConverter converter = new ContentTypeDelegatingMessageConverter();
        StringMessageConverter stringConverter = new StringMessageConverter();
        FileMessageConverter fileMessageConverter = new FileMessageConverter();

        /**
         * spring-rabbitmq 包中的 json 消息转换工具类，也包括 GsonHttpMessageConverter。
         * 使用Jackson2JsonMessageConverter处理器，生产端发送JSON类型数据，但是没有指定消息的contentType类型，那么它就会将消
         * 息转换成byte[]类型的消息进行消费。
         * 如果指定了contentType为application/json，那么消费端就会将消息转换成Map类型的消息进行消费。
         * 如果指定为 application/json，并且生产端是List类型的JSON格式，那么消费端就会将消息转换成List类型的消息进行消费。
         *
         *
         * 不过要注意，在不同的项目系统中通信使用 JsonMessageConverter 时有一个小问题，在不对它进行任何改造的前提下，发送消息
         * 的类和接受消息的类必须是一样的，不仅是要里面的字段一样，类名一样，连类的包路径都要一样。
         *
         * 所以当系统 A 使用 JsonMessageConverter 发送消息类A1给系统 B 时，系统 B 可以有如下几种方式来接收：
         * 1，依赖系统 A 的jar包，直接使用类 A1 来接收。
         * 2，不依赖系统 A 的jar包，自己建一个和 A1 一模一样的类，连名称，包路径都一样。
         * 3，负责监听 queue 的类实现 MessageListener 接口，直接接收 Message 类，再自己转换。
         *
         * 上面三个方法都不是很好，按照正常的想法，我们肯定是期望系统 B 直接使用自己的类来接收就可以了，只要与 A1 类的字段名一
         * 样即可。要实现此功能，就要先看看 JsonMessageConverter 是如何将 Message 进行反序列化的，在 JsonMessageConverter 的
         * fromMessage 方法中有这么一段:
         *
         * 在默认情况下，JsonMessageConverter 使用的 ClassMapper 是 DefaultJackson2JavaTypeMapper，在转换时通过 Message 的
         * Properties 来获取要转换的目标类的类型。通过 Debug 可以发现，目标类的类型是存储在Message Proterties 的一个 headers
         * Map 中，Key 叫“__TypeId__”。所以只要想办法在传输消息时更改__TypeId__的值即可。
         *
         * 所以解决办法就是，在消息的生产者端自定义一个 json 转换器，类型是 JsonMessageConverter，设置一个自定义的 ClassMapper，
         * 重写 fromClass 方法，将 __TypeId__ 的值设为消费端用来接收的类的路径+名称。
         * 当然了，也可以在消费者端重写toClass方法，直接返回想要转换的目标类的类类型。两种选一种就可以。
         *
         *
         * 但是在本例中，上面的方法都抛出异常（在重写的 toClass 方法中），应该是此方法已经过时了。因为现在可以在 setIdClassMapping
         * 中设置一个 json 对象类（class）的路径。
         * 并且在 map 中 key 的值，必须是消费者与生产者统一的（即 __ContentTypeId__ 的值）。
         */
        Jackson2JsonMessageConverter jsonMessageConverter = new Jackson2JsonMessageConverter();
        DefaultJackson2JavaTypeMapper javaTypeMapper = new DefaultJackson2JavaTypeMapper();
        Map<String,Class<?>> classMap = new HashMap<>();
        classMap.put("Order",Order.class);
        javaTypeMapper.setIdClassMapping(classMap);
        jsonMessageConverter.setJavaTypeMapper(javaTypeMapper);


        // 添加委派（即实际）的消息转换器
        converter.addDelegate("text/html",stringConverter);
        converter.addDelegate("text/plain",stringConverter);
        converter.addDelegate("application/json",jsonMessageConverter);
        converter.addDelegate("image/jpg",fileMessageConverter);
        converter.addDelegate("image/png",fileMessageConverter);

        messageAdapter.setMessageConverter(converter);
        return messagelistener;
    }
}
