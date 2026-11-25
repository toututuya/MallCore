package com.qiujie.mq;

import cn.hutool.core.util.IdUtil;
import com.qiujie.cache.SeckillReservationStore;
import com.qiujie.config.RabbitMQConfig;
import com.qiujie.entity.Order;
import com.qiujie.entity.OrderItem;
import com.qiujie.entity.Product;
import com.qiujie.entity.ProductImg;
import com.qiujie.enums.BusinessStatusEnum;
import com.qiujie.enums.OrderStatusEnum;
import com.qiujie.enums.PayMethodEnum;
import com.qiujie.exception.BusinessException;
import com.qiujie.mapper.OrderItemMapper;
import com.qiujie.mapper.OrderMapper;
import com.qiujie.mapper.ProductImgMapper;
import com.qiujie.mapper.ProductMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 秒杀订单消息消费者。
 *
 * @author qiujie
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SeckillMessageListener {
    private static final int MAX_RETRIES = 3;
    private static final String RETRY_HEADER = "x-retry-count";
    private static final DateTimeFormatter ORDER_SN_PREFIX = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private final OrderMapper orderMapper;
    private final OrderItemMapper orderItemMapper;
    private final ProductMapper productMapper;
    private final ProductImgMapper productImgMapper;
    private final SeckillReservationStore reservations;
    private final ConfirmedPublisher publisher;
    private final TransactionTemplate transactions;

    @RabbitListener(queues = RabbitMQConfig.SECKILL_QUEUE)
    public void handleSeckillOrder(SeckillMessage message, Channel channel,
                                   @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                                   @Header(value = RETRY_HEADER, required = false) Integer retryCount) throws IOException {
        try {
            if (Boolean.TRUE.equals(transactions.execute(status -> createSeckillOrder(message)))) {
                recordSuccess(message);
            }
        } catch (RuntimeException failure) {
            try {
                handleFailure(message, retryCount == null ? 0 : retryCount, failure);
            } catch (RuntimeException retryPublishFailure) {
                log.error("Could not hand off seckill message for session {}, user {}",
                        message.getSessionId(), message.getUserId(), retryPublishFailure);
                channel.basicNack(deliveryTag, false, true);
                return;
            }
        }
        channel.basicAck(deliveryTag, false);
    }

    private boolean createSeckillOrder(SeckillMessage message) {
        Integer productId = message.getProductId();
        // Serialize duplicate deliveries and terminal compensation on the same product row.
        if (productMapper.lockForOrder(productId) == null) {
            throw new IllegalStateException("Missing product " + productId);
        }
        if (!reservations.claim(message.getSessionId(), message.getUserId(), message.getReservationId())) {
            return false;
        }
        if (hasOrder(message)) {
            return true;
        }
        if (productMapper.decrementStock(productId, 1) != 1) {
            throw new BusinessException(BusinessStatusEnum.CONFLICT, "商品库存不足");
        }
        Product product = productMapper.selectById(productId);

        Order order = new Order();
        order.setUserId(message.getUserId());
        order.setSeckillSessionId(message.getSessionId());
        order.setOrderSn(LocalDateTime.now().format(ORDER_SN_PREFIX)
                + IdUtil.getSnowflake(1, 1).nextIdStr().substring(11));
        order.setTotalAmount(message.getSeckillPrice());
        order.setPayMethod(PayMethodEnum.UNKNOWN);
        order.setStatus(OrderStatusEnum.PENDING_PAY);
        orderMapper.insert(order);

        OrderItem item = new OrderItem();
        item.setOrderId(order.getId());
        item.setProductId(productId);
        item.setProductName(product.getName());
        item.setProductPrice(message.getSeckillPrice());
        item.setAmount(1);
        ProductImg firstImg = productImgMapper.selectFirstByProductId(productId);
        if (firstImg != null) {
            item.setProductImg(firstImg.getUrl());
        }
        orderItemMapper.insert(item);

        OrderTimeoutMessage timeout = new OrderTimeoutMessage();
        timeout.setOrderId(order.getId());
        publisher.send("", RabbitMQConfig.ORDER_DELAY_QUEUE, timeout, outbound -> outbound);
        return true;
    }

    private void handleFailure(SeckillMessage message, int retryCount, RuntimeException failure) {
        if (retryCount < MAX_RETRIES) {
            publisher.send("", RabbitMQConfig.SECKILL_RETRY_QUEUE, message, outbound -> {
                outbound.getMessageProperties().setHeader(RETRY_HEADER, retryCount + 1);
                return outbound;
            });
            return;
        }

        log.warn("Seckill retries exhausted for session {}, user {}",
                message.getSessionId(), message.getUserId(), failure);
        try {
            boolean committed = Boolean.TRUE.equals(transactions.execute(status -> {
                productMapper.lockForOrder(message.getProductId());
                if (hasOrder(message)) {
                    return true;
                }
                reservations.failReservation(message.getSessionId(), message.getUserId(), message.getReservationId());
                return false;
            }));
            if (committed) {
                recordSuccess(message);
            }
        } catch (RuntimeException compensationFailure) {
            log.error("Seckill outcome requires review for session {}, user {}",
                    message.getSessionId(), message.getUserId(), compensationFailure);
            publisher.send("", RabbitMQConfig.SECKILL_FAILED_QUEUE, message, outbound -> outbound);
        }
    }

    private boolean hasOrder(SeckillMessage message) {
        return orderMapper.findSeckillOrderId(message.getUserId(), message.getSessionId()) != null;
    }

    private void recordSuccess(SeckillMessage message) {
        reservations.recordSuccess(message.getSessionId(), message.getUserId(), message.getReservationId());
    }
}
