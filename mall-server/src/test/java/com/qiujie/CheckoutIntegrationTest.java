package com.qiujie;

import cn.dev33.satoken.stp.StpUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.qiujie.cache.SeckillReservationStore;
import com.qiujie.config.RabbitMQConfig;
import com.qiujie.entity.Order;
import com.qiujie.entity.OrderItem;
import com.qiujie.entity.Product;
import com.qiujie.entity.SeckillSession;
import com.qiujie.enums.OrderStatusEnum;
import com.qiujie.exception.BusinessException;
import com.qiujie.mapper.OrderItemMapper;
import com.qiujie.mapper.OrderMapper;
import com.qiujie.mapper.ProductMapper;
import com.qiujie.mapper.SeckillSessionMapper;
import com.qiujie.mq.ConfirmedPublisher;
import com.qiujie.mq.OrderTimeoutMessage;
import com.qiujie.mq.SeckillMessage;
import com.qiujie.service.OrderCancellationService;
import com.qiujie.service.OrderService;
import com.qiujie.service.SeckillService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("dev")
@EnabledIfEnvironmentVariable(named = "MALLCORE_INTEGRATION", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.devtools.restart.enabled=false")
class CheckoutIntegrationTest {
    @Autowired private TestRestTemplate http;
    @Autowired private ProductMapper products;
    @Autowired private OrderMapper orders;
    @Autowired private OrderItemMapper items;
    @Autowired private SeckillSessionMapper sessions;
    @Autowired private OrderCancellationService cancellations;
    @Autowired private OrderService orderService;
    @Autowired private SeckillService seckillService;
    @Autowired private SeckillReservationStore reservations;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private StringRedisTemplate redis;
    @Autowired private ConfirmedPublisher publisher;
    @Autowired private RabbitTemplate rabbit;
    @Autowired private AmqpAdmin broker;
    @Autowired @Qualifier("seckillBinding") private Binding seckillBinding;

    @Test
    void browsesProductCreatesCartOrderAndPaysThroughHttp() {
        int productId = product(7);
        HttpHeaders headers = new HttpHeaders();
        headers.set("mall", StpUtil.getStpLogic().createLoginSession(9001));

        assertThat(request(HttpMethod.GET, "/portal/product/detail/" + productId, headers, null)
                .path("id").asInt()).isEqualTo(productId);
        request(HttpMethod.POST, "/portal/cart/add", headers, Map.of("productId", productId, "amount", 2));
        JsonNode cart = request(HttpMethod.GET, "/portal/cart/list", headers, null);
        assertThat(cart).hasSize(1);
        int cartId = cart.get(0).path("id").asInt();
        int orderId = request(HttpMethod.POST, "/portal/order/create", headers,
                Map.of("addressId", 1, "cartIds", List.of(cartId))).path("id").asInt();
        assertThat(orderId).isPositive();
        request(HttpMethod.PUT, "/portal/order/pay/" + orderId, headers, Map.of("payMethod", 1));

        assertThat(orders.selectById(orderId).getStatus()).isEqualTo(OrderStatusEnum.PAID);
        assertThat(stock(productId)).isEqualTo(5);
        assertThat(request(HttpMethod.GET, "/portal/cart/list", headers, null)).isEmpty();
    }

    @Test
    void brokerExpiresAnOrderAndDuplicateCancellationDoesNotRestockTwice() {
        int productId = product(3);
        int sessionId = session(productId, 1);
        int orderId = pendingOrder(9002, sessionId, productId);
        redis.opsForValue().set(stockKey(sessionId), "0");

        // The isolated Compose stack may already contain 10-minute delays from another test.
        broker.purgeQueue(RabbitMQConfig.ORDER_DELAY_QUEUE);
        publisher.send("", RabbitMQConfig.ORDER_DELAY_QUEUE, timeout(orderId), outbound -> {
            outbound.getMessageProperties().setExpiration("150");
            return outbound;
        });
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(orders.selectById(orderId).getStatus()).isEqualTo(OrderStatusEnum.CANCELLED);
            assertThat(orders.pendingStockRelease(orderId)).isNull();
            assertThat(stock(productId)).isEqualTo(4);
            assertThat(redis.opsForValue().get(stockKey(sessionId))).isEqualTo("1");
        });

        publisher.send("", RabbitMQConfig.ORDER_TIMEOUT_QUEUE, timeout(orderId), outbound -> outbound);
        assertThat(cancellations.cancel(orderId)).isEqualTo(OrderStatusEnum.CANCELLED);
        await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(stock(productId)).isEqualTo(4);
            assertThat(redis.opsForValue().get(stockKey(sessionId))).isEqualTo("1");
        });
    }

    @Test
    void failedTimeoutRollsBackAndCanBeRetrievedFromParkingQueue() {
        int orderId = pendingOrder(9003, null, null);
        publisher.send("", RabbitMQConfig.ORDER_TIMEOUT_QUEUE, timeout(orderId), outbound -> outbound);

        Object parked = rabbit.receiveAndConvert(RabbitMQConfig.ORDER_TIMEOUT_FAILED_QUEUE, 10_000);
        assertThat(parked).isInstanceOf(OrderTimeoutMessage.class);
        assertThat(((OrderTimeoutMessage) parked).getOrderId()).isEqualTo(orderId);
        assertThat(orders.selectById(orderId).getStatus()).isEqualTo(OrderStatusEnum.PENDING_PAY);
    }

    @Test
    void concurrentPaymentAndCancellationLeaveExactlyOneOutcome() throws Exception {
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 12; i++) {
                int productId = product(3);
                int orderId = pendingOrder(9004, null, productId);
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> payment = threads.submit(() -> {
                    start.await();
                    try {
                        orderService.pay(9004, orderId, 1);
                        return true;
                    } catch (BusinessException rejected) {
                        return false;
                    }
                });
                Future<OrderStatusEnum> cancellation = threads.submit(() -> {
                    start.await();
                    return cancellations.cancel(orderId);
                });
                start.countDown();

                boolean paid = payment.get(10, TimeUnit.SECONDS);
                OrderStatusEnum expected = paid ? OrderStatusEnum.PAID : OrderStatusEnum.CANCELLED;
                assertThat(cancellation.get(10, TimeUnit.SECONDS)).isEqualTo(expected);
                assertThat(orders.selectById(orderId).getStatus()).isEqualTo(expected);
                assertThat(stock(productId)).isEqualTo(paid ? 3 : 4);
            }
        } finally {
            threads.shutdownNow();
        }
    }

    @Test
    void oneHundredConcurrentSeckillSubmissionsCreateTwentyDistinctOrders() throws Exception {
        int productId = product(20);
        int sessionId = session(productId, 20);
        ExecutorService threads = Executors.newFixedThreadPool(20);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> requests = new ArrayList<>();
        List<Integer> buyers = new ArrayList<>();
        try {
            for (int i = 0; i < 100; i++) {
                int userId = 10_000 + i % 50;
                requests.add(threads.submit(() -> {
                    start.await();
                    try {
                        seckillService.execute(sessionId, userId);
                        return userId;
                    } catch (BusinessException rejected) {
                        assertThat(rejected.getMessage()).isIn("您已参与过此秒杀活动", "已抢光");
                        return null;
                    }
                }));
            }
            start.countDown();
            for (Future<Integer> request : requests) {
                Integer buyer = request.get(30, TimeUnit.SECONDS);
                if (buyer != null) {
                    buyers.add(buyer);
                }
            }
        } finally {
            threads.shutdownNow();
        }

        assertThat(buyers).hasSize(20).doesNotHaveDuplicates();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(orderCount(sessionId)).isEqualTo(20);
            assertThat(stock(productId)).isZero();
            assertThat(redis.opsForValue().get(stockKey(sessionId))).isEqualTo("0");
            for (Integer buyer : buyers) {
                assertThat(seckillService.getResult(sessionId, buyer)).containsEntry("status", 1);
            }
        });

        int userId = buyers.get(0);
        String mark = redis.opsForValue().get("seckill:order:" + sessionId + ":" + userId);
        assertThat(mark).startsWith("processing:");
        sendSeckill(sessionId, userId, productId, mark.substring("processing:".length()));
        await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(orderCount(sessionId)).isEqualTo(20);
            assertThat(stock(productId)).isZero();
            assertThat(redis.opsForValue().get(stockKey(sessionId))).isEqualTo("0");
        });
    }

    @Test
    void unroutablePublishReturnsReservationAndAllowsAnotherAttempt() {
        int productId = product(1);
        int sessionId = session(productId, 1);
        int userId = 9005;
        broker.removeBinding(seckillBinding);
        try {
            assertThatThrownBy(() -> seckillService.execute(sessionId, userId))
                    .isInstanceOf(BusinessException.class).hasMessage("秒杀消息发送失败");
            assertThat(redis.opsForValue().get(stockKey(sessionId))).isEqualTo("1");
            assertThat(seckillService.getResult(sessionId, userId)).containsEntry("status", -1);
            assertThat(orderCount(sessionId)).isZero();
        } finally {
            broker.declareBinding(seckillBinding);
        }

        seckillService.execute(sessionId, userId);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(orderCount(sessionId)).isEqualTo(1);
            assertThat(seckillService.getResult(sessionId, userId)).containsEntry("status", 1);
        });
    }

    @Test
    void staleDeliveryCannotUseNewReservation() {
        int productId = product(1);
        int sessionId = session(productId, 1);
        int userId = 9006;
        assertThat(reservations.reserve(sessionId, userId, 1, "old", Duration.ofMinutes(2))).isEqualTo(1);
        assertThat(reservations.releaseUnclaimed(sessionId, userId, "old")).isTrue();
        assertThat(reservations.releaseUnclaimed(sessionId, userId, "old")).isFalse();
        assertThat(reservations.reserve(sessionId, userId, 1, "new", Duration.ofMinutes(2))).isEqualTo(1);

        sendSeckill(sessionId, userId, productId, "old");
        await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(orderCount(sessionId)).isZero());

        sendSeckill(sessionId, userId, productId, "new");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(orderCount(sessionId)).isEqualTo(1);
            assertThat(seckillService.getResult(sessionId, userId)).containsEntry("status", 1);
        });
        assertThat(reservations.releaseUnclaimed(sessionId, userId, "new")).isFalse();
        assertThat(redis.opsForValue().get(stockKey(sessionId))).isEqualTo("0");
    }

    @Test
    void exhaustedConsumerRetriesReturnOnlyTheFailedReservation() {
        int productId = product(0);
        int sessionId = session(productId, 1);
        int userId = 9007;
        seckillService.execute(sessionId, userId);
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(seckillService.getResult(sessionId, userId)).containsEntry("status", -1);
            assertThat(redis.opsForValue().get(stockKey(sessionId))).isEqualTo("1");
            assertThat(orderCount(sessionId)).isZero();
            assertThat(stock(productId)).isZero();
        });
    }

    private JsonNode request(HttpMethod method, String url, HttpHeaders headers, Map<String, Object> body) {
        var response = http.exchange(url, method, new HttpEntity<>(body, headers), JsonNode.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).as("%s %s %s", method, url, response.getBody()).isTrue();
        JsonNode payload = response.getBody();
        assertThat(payload).isNotNull();
        assertThat(payload.path("code").asInt()).as("%s %s %s", method, url, payload).isEqualTo(20000);
        return payload.path("data");
    }

    private int product(int stock) {
        Product product = new Product().setName("CI-" + UUID.randomUUID())
                .setPrice(BigDecimal.TEN).setStock(stock).setCategoryId(1);
        products.insert(product);
        return product.getId();
    }

    private int session(int productId, int stock) {
        LocalDateTime now = LocalDateTime.now();
        SeckillSession session = new SeckillSession().setProductId(productId).setSeckillPrice(BigDecimal.ONE)
                .setSeckillStock(stock).setStartTime(now.minusHours(1)).setEndTime(now.plusHours(1));
        sessions.insert(session);
        return session.getId();
    }

    private int pendingOrder(int userId, Integer sessionId, Integer productId) {
        Order order = new Order().setUserId(userId).setSeckillSessionId(sessionId)
                .setStatus(OrderStatusEnum.PENDING_PAY).setOrderSn(UUID.randomUUID().toString().substring(0, 22))
                .setTotalAmount(BigDecimal.ONE);
        orders.insert(order);
        if (productId != null) {
            items.insert(new OrderItem().setOrderId(order.getId()).setProductId(productId)
                    .setProductName("CI").setProductPrice(BigDecimal.ONE).setAmount(1));
        }
        return order.getId();
    }

    private int stock(int productId) {
        return products.selectById(productId).getStock();
    }

    private int orderCount(int sessionId) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM oms_order WHERE seckill_session_id = ?", Integer.class, sessionId);
    }

    private static String stockKey(int sessionId) {
        return "seckill:stock:" + sessionId;
    }

    private static OrderTimeoutMessage timeout(int orderId) {
        OrderTimeoutMessage message = new OrderTimeoutMessage();
        message.setOrderId(orderId);
        return message;
    }

    private void sendSeckill(int sessionId, int userId, int productId, String token) {
        SeckillMessage message = new SeckillMessage();
        message.setSessionId(sessionId);
        message.setUserId(userId);
        message.setProductId(productId);
        message.setSeckillPrice(BigDecimal.ONE);
        message.setReservationId(token);
        publisher.send(RabbitMQConfig.SECKILL_EXCHANGE, RabbitMQConfig.SECKILL_ROUTING_KEY,
                message, outbound -> outbound);
    }
}
