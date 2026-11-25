package com.qiujie.service;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.qiujie.cache.SeckillReservationStore;
import com.qiujie.entity.Order;
import com.qiujie.entity.OrderItem;
import com.qiujie.enums.OrderStatusEnum;
import com.qiujie.mapper.OrderItemMapper;
import com.qiujie.mapper.OrderMapper;
import com.qiujie.mapper.ProductMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

@Service
public class OrderCancellationService {

    private final OrderMapper orders;
    private final OrderItemMapper items;
    private final ProductMapper products;
    private final TransactionTemplate transactions;
    private final SeckillReservationStore reservations;

    public OrderCancellationService(OrderMapper orders, OrderItemMapper items, ProductMapper products,
                                    TransactionTemplate transactions, SeckillReservationStore reservations) {
        this.orders = orders;
        this.items = items;
        this.products = products;
        this.transactions = transactions;
        this.reservations = reservations;
    }

    public OrderStatusEnum cancel(Integer orderId) {
        OrderStatusEnum status = transactions.execute(transaction -> cancelInDatabase(orderId));
        if (status == null || status == OrderStatusEnum.CANCELLED) {
            Integer sessionId = orders.pendingStockRelease(orderId);
            if (sessionId != null) {
                reservations.restoreCancelledStock(sessionId, orderId);
                orders.finishStockRelease(orderId);
                reservations.expireCancelledStockMarker(orderId);
            }
        }
        return status;
    }

    private OrderStatusEnum cancelInDatabase(Integer orderId) {
        boolean cancelled = orders.cancelPending(orderId) == 1;
        Order order = orders.selectById(orderId);
        if (!cancelled) {
            return order == null ? null : order.getStatus();
        }

        List<OrderItem> orderItems = items.selectList(Wrappers.<OrderItem>lambdaQuery()
                .eq(OrderItem::getOrderId, orderId).eq(OrderItem::getDeleted, 0));
        if (orderItems.isEmpty()) {
            throw new IllegalStateException("Order " + orderId + " has no item to release");
        }
        for (OrderItem item : orderItems) {
            if (products.incrementStock(item.getProductId(), item.getAmount()) != 1) {
                throw new IllegalStateException("Unable to release stock for order " + orderId);
            }
        }
        if (order.getSeckillSessionId() != null) {
            orders.recordStockRelease(orderId, order.getSeckillSessionId());
        }
        return OrderStatusEnum.CANCELLED;
    }
}
