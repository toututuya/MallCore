package com.qiujie.mq;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class ConfirmedPublisher {
    private final RabbitTemplate rabbit;

    public ConfirmedPublisher(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    public void send(String exchange, String routingKey, Object message, MessagePostProcessor options) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        rabbit.convertAndSend(exchange, routingKey, message, options, correlation);
        try {
            CorrelationData.Confirm confirm = correlation.getFuture().get(5, TimeUnit.SECONDS);
            if (!confirm.isAck() || correlation.getReturned() != null) {
                throw new AmqpException("RabbitMQ did not route the publish to " + routingKey);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AmqpException("RabbitMQ confirm wait interrupted", error);
        } catch (TimeoutException | ExecutionException error) {
            throw new AmqpException("RabbitMQ publish outcome unknown for " + routingKey, error);
        }
    }
}
