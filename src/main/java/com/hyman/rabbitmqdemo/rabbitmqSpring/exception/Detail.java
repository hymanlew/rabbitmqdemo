package com.hyman.rabbitmqdemo.rabbitmqSpring.exception;

public class Detail {
    /**
     * RabbitMQ异常处理，Spring AMQP配置方式：
     * 在消费 RabbitMq 中的 Message 时，常常会出现异常，可能是 Message 本身格式不对，或者由于某些原因无法被处理。我一般都是
     * catch 异常然后抛个 AmqpRejectAndDontRequeueException (以下简称 ARADRE )，也没出啥问题。
     *
     * 根据官方文档，当 listener 在消费消息时抛出一个异常的时候，该异常会被包装在 ListenerExecutionFailedException 中抛出，
     * 并根据 listenerContainer 中 defaultRequeueRejected 设定的值来决定是否将该消息重新加入队列，默认是会重新加入队列（true），
     * false 则将把这个消息直接丢弃。
     * 但需要注意的是，如果抛出的异常是 ARADRE 或其他被 RabbitMq 认为是致命错误的异常，即便 defaultRequeueRejected 的值为 true，
     * 该消息也不会重新加入队列，而是会被直接丢弃或加入 dead-letter-exchange 中(如果有配置 dead-letter-exchange)。
     *
     * 在 1.6.3.RELEASE 中被 RabbitMq 认为是致命错误的异常有以下 6 种：
     * o.s.amqp…MessageConversionException
     * o.s.messaging…MessageConversionException
     * o.s.messaging…MethodArgumentNotValidException
     * o.s.messaging…MethodArgumentTypeMismatchException
     * java.lang.NoSuchMethodException
     * java.lang.ClassCastException
     *
     * 也就是说，当抛出以上异常及 ARADRE 时，该消息一定不会重新入队，即便 defaultRequeueRejected 的值为 true。
     *
     *
     * Spring-RabbitMq 是如何实现的：
     * 在源码中，异常在 AbstractMessageListenerContainer 中被包装在 ListenerExecutionFailedException 中之后还会经由 ErrorHandler
     * 的 handleError 方法处理， 默认的 ErrorHandler 是 ConditionalRejectingErrorHandler。
     * 我们也可以实现自己的 ErrorHandler 来控制需要丢弃消息的异常，只要实现 org.springframework.util.ErrorHandler 接口，然后将
     * listenerContainer 中的 errorHandler 参数指定我们自定义的 handler 即可。
     *
     * ConditionalRejectingErrorHandler 中配置有 FatalExceptionStrategy，会调用 FatalExceptionStrategy 中的 isFatal 方法来判断异
     * 常是不是属于致命异常。
     *
     *
     * 队列异常：
     * 1，reply-code=406, reply-text=PRECONDITION_FAILED，是因为 RabbitMQ不允许使用不同的参数重新定义一个队列，所以已经存在的队列，
     *    我们无法修改其属性。如重复定义就可能遇到这个异常错误。解决方案：清空队列或者修改队列名：
     *
     *    查看所有队列信息， rabbitmqctl list_queues
     *    关闭应用，rabbitmqctl stop_app
     *    清除所有队列，rabbitmqctl reset（相当于恢复出厂设置，所以一定要小心）
     *    启动应用，和上述关闭命令配合使用，达到清空队列的目的，rabbitmqctl start_app
     */
}
