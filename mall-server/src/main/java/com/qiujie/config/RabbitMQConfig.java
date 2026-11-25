package com.qiujie.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecovererWithConfirms;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 配置 — 秒杀订单队列
 *
 * @author qiujie
 */
@Configuration
public class RabbitMQConfig {

    public static final String SECKILL_QUEUE = "seckill.order.queue";
    public static final String SECKILL_EXCHANGE = "seckill.order.exchange";
    public static final String SECKILL_ROUTING_KEY = "seckill.order";

    // 订单超时取消
    public static final String ORDER_DELAY_QUEUE = "order.delay.queue";
    public static final String ORDER_TIMEOUT_EXCHANGE = "order.timeout.exchange";
    public static final String ORDER_TIMEOUT_QUEUE = "order.timeout.queue";
    public static final String ORDER_TIMEOUT_ROUTING_KEY = "order.timeout";
    public static final String ORDER_TIMEOUT_FAILED_QUEUE = "order.timeout.failed.queue";
    public static final String SECKILL_FAILED_QUEUE = "seckill.order.failed.queue";
    private static final int ORDER_TIMEOUT_MS = 10 * 60 * 1000; // 10 分钟

    // ======================== 秒杀队列 ========================

    public static final String SECKILL_DLX_EXCHANGE = "seckill.order.dlx.exchange";
    public static final String SECKILL_DLX_ROUTING_KEY = "seckill.order.dlx";
    public static final String SECKILL_RETRY_QUEUE = "seckill.retry.delay.queue";
    private static final int SECKILL_RETRY_TTL = 5 * 1000; // 5 秒

    @Bean
    public DirectExchange seckillExchange() {
        return new DirectExchange(SECKILL_EXCHANGE);
    }

    @Bean
    public DirectExchange seckillDlxExchange() {
        return new DirectExchange(SECKILL_DLX_EXCHANGE);
    }

    @Bean
    public Queue seckillQueue() {
        return QueueBuilder.durable(SECKILL_QUEUE)
                .deadLetterExchange(SECKILL_DLX_EXCHANGE)
                .deadLetterRoutingKey(SECKILL_DLX_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding seckillBinding() {
        return BindingBuilder.bind(seckillQueue()).to(seckillExchange()).with(SECKILL_ROUTING_KEY);
    }

    @Bean
    public Queue seckillRetryQueue() {
        return QueueBuilder.durable(SECKILL_RETRY_QUEUE)
                .ttl(SECKILL_RETRY_TTL)
                .deadLetterExchange(SECKILL_EXCHANGE)
                .deadLetterRoutingKey(SECKILL_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding seckillRetryBinding() {
        return BindingBuilder.bind(seckillRetryQueue())
                .to(seckillDlxExchange()).with(SECKILL_DLX_ROUTING_KEY);
    }

    // ======================== 订单超时取消 ========================

    @Bean
    public DirectExchange orderTimeoutExchange() {
        return new DirectExchange(ORDER_TIMEOUT_EXCHANGE);
    }

    /**
     * 延时队列 — 消息 TTL 过期后自动路由到死信交换机
     */
    @Bean
    public Queue orderDelayQueue() {
        return QueueBuilder.durable(ORDER_DELAY_QUEUE)
                .ttl(ORDER_TIMEOUT_MS)
                .deadLetterExchange(ORDER_TIMEOUT_EXCHANGE)
                .deadLetterRoutingKey(ORDER_TIMEOUT_ROUTING_KEY)
                .build();
    }

    @Bean
    public Queue orderTimeoutQueue() {
        return QueueBuilder.durable(ORDER_TIMEOUT_QUEUE).build();
    }

    @Bean
    public Binding orderTimeoutBinding() {
        return BindingBuilder.bind(orderTimeoutQueue())
                .to(orderTimeoutExchange())
                .with(ORDER_TIMEOUT_ROUTING_KEY);
    }

    @Bean
    public Queue orderTimeoutFailedQueue() {
        return QueueBuilder.durable(ORDER_TIMEOUT_FAILED_QUEUE).build();
    }

    @Bean
    public Queue seckillFailedQueue() {
        return QueueBuilder.durable(SECKILL_FAILED_QUEUE).build();
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        converter.setCreateMessageIds(true);
        return converter;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter,
            SimpleRabbitListenerContainerFactoryConfigurer configurer) {
        return listenerFactory(connectionFactory, jsonMessageConverter, configurer, AcknowledgeMode.MANUAL);
    }

    @Bean
    public SimpleRabbitListenerContainerFactory orderTimeoutListenerContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter, RabbitTemplate rabbit,
            SimpleRabbitListenerContainerFactoryConfigurer configurer) {
        SimpleRabbitListenerContainerFactory factory =
                listenerFactory(connectionFactory, jsonMessageConverter, configurer, AcknowledgeMode.AUTO);
        RepublishMessageRecovererWithConfirms recoverer = new RepublishMessageRecovererWithConfirms(
                rabbit, "", ORDER_TIMEOUT_FAILED_QUEUE, CachingConnectionFactory.ConfirmType.CORRELATED);
        factory.setAdviceChain(RetryInterceptorBuilder.stateless().maxAttempts(3)
                .backOffOptions(200, 2, 800).recoverer(recoverer).build());
        return factory;
    }

    private SimpleRabbitListenerContainerFactory listenerFactory(
            ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter,
            SimpleRabbitListenerContainerFactoryConfigurer configurer, AcknowledgeMode ackMode) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setMessageConverter(jsonMessageConverter);
        factory.setAcknowledgeMode(ackMode);
        return factory;
    }
}
