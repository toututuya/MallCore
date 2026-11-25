package com.qiujie.mq;

import com.qiujie.config.RabbitMQConfig;
import com.qiujie.service.OrderCancellationService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class OrderTimeoutListener {
    private final OrderCancellationService cancellations;

    public OrderTimeoutListener(OrderCancellationService cancellations) {
        this.cancellations = cancellations;
    }

    @RabbitListener(queues = RabbitMQConfig.ORDER_TIMEOUT_QUEUE,
            containerFactory = "orderTimeoutListenerContainerFactory")
    public void handleTimeout(OrderTimeoutMessage message) {
        cancellations.cancel(Objects.requireNonNull(message.getOrderId(), "Missing order ID"));
    }
}
