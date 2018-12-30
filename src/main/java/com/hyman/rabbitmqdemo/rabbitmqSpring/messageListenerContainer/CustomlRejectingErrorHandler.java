package com.hyman.rabbitmqdemo.rabbitmqSpring.messageListenerContainer;

import com.hyman.rabbitmqdemo.util.Logutil;
import org.slf4j.Logger;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.listener.FatalExceptionStrategy;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentTypeMismatchException;
import org.springframework.util.ErrorHandler;

// 自定义异常处理器 ConditionalRejectingErrorHandler
public class CustomlRejectingErrorHandler implements ErrorHandler{

    private final FatalExceptionStrategy exceptionStrategy = new CustomExceptionStrategy();
    private final Logger logger = Logutil.logger;

    @Override
    public void handleError(Throwable t) {

        if (this.logger.isWarnEnabled()) {
            this.logger.warn("Execution of Rabbit message listener failed.", t);
        }
        // 如果是致命异常，则转为 AmqpRejectAndDontRequeueException 抛出
        if (!this.causeChainContainsARADRE(t) && this.exceptionStrategy.isFatal(t)) {
            throw new AmqpRejectAndDontRequeueException("Error Handler converted exception to fatal", t);
        }
    }

    /**
     * @return true if the cause chain already contains an
     * {@link AmqpRejectAndDontRequeueException}.
     */
    private boolean causeChainContainsARADRE(Throwable t) {
        Throwable cause = t.getCause();
        while (cause != null) {
            if (cause instanceof AmqpRejectAndDontRequeueException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    // Strategy：策略，implements FatalExceptionStrategy，extends ConditionalRejectingErrorHandler.DefaultExceptionStrategy
    public class CustomExceptionStrategy implements FatalExceptionStrategy{
        // 判断传入参数是不是致命异常
        @Override
        public boolean isFatal(Throwable t) {
            if (t instanceof ListenerExecutionFailedException
                    && isCauseFatal(t.getCause())) {
                if (CustomlRejectingErrorHandler.this.logger.isWarnEnabled()) {
                    CustomlRejectingErrorHandler.this.logger.warn(
                            "Fatal message conversion error; message rejected; "
                                    + "it will be dropped or routed to a dead letter exchange, if so configured: "
                                    + ((ListenerExecutionFailedException) t).getFailedMessage());
                }
                return true;
            }
            return false;
        }

        private boolean isCauseFatal(Throwable cause) {
            return cause instanceof MessageConversionException
                    || cause instanceof org.springframework.messaging.converter.MessageConversionException
                    || cause instanceof MethodArgumentNotValidException
                    || cause instanceof MethodArgumentTypeMismatchException
                    || cause instanceof NoSuchMethodException
                    || cause instanceof ClassCastException
                    || isUserCauseFatal(cause);
        }

        /**
         * 通过重写该方法来添加自定义的异常：
         * 如果想要把自定义的异常加入到 fatalException，一个简单的办法就是提供新的 FatalExceptionStrategy，只要继承
         * ConditionalRejectingErrorHandler.DefaultExceptionStrategy 并重写 isUserCauseFatal(Throwable cause) 方法，在方
         * 法里对于需要丢弃消息的异常返回 true即可。
         *
         * @param cause the cause
         * @return true if the cause is fatal.
         */
        //@Override
        protected boolean isUserCauseFatal(Throwable cause) {
            if(cause.getMessage().contains("真实")){
                return true;
            }
            return false;
        }
    }
}
