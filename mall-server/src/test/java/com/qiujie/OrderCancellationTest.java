package com.qiujie;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.qiujie.cache.SeckillReservationStore;
import com.qiujie.entity.Order;
import com.qiujie.enums.OrderStatusEnum;
import com.qiujie.mapper.OrderItemMapper;
import com.qiujie.mapper.OrderMapper;
import com.qiujie.mapper.ProductMapper;
import com.qiujie.service.OrderCancellationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;

import org.junit.jupiter.api.AfterEach;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderCancellationTest {
    private EmbeddedDatabase database;
    private JdbcTemplate jdbc;
    private OrderMapper orders;
    private SeckillReservationStore reservations;
    private OrderCancellationService cancellations;

    @BeforeEach
    void openDatabase() throws Exception {
        database = new EmbeddedDatabaseBuilder().generateUniqueName(true)
                .setType(EmbeddedDatabaseType.H2).addScript("checkout-schema.sql").build();
        jdbc = new JdbcTemplate(database);

        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setMapUnderscoreToCamelCase(true);
        configuration.addMapper(OrderMapper.class);
        configuration.addMapper(OrderItemMapper.class);

        MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
        factory.setDataSource(database);
        factory.setConfiguration(configuration);
        factory.setMapperLocations(new ClassPathResource("mapper/ProductMapper.xml"));
        SqlSessionTemplate session = new SqlSessionTemplate(factory.getObject());

        orders = session.getMapper(OrderMapper.class);
        reservations = mock(SeckillReservationStore.class);
        cancellations = new OrderCancellationService(orders, session.getMapper(OrderItemMapper.class),
                session.getMapper(ProductMapper.class),
                new TransactionTemplate(new DataSourceTransactionManager(database)), reservations);
        jdbc.update("INSERT INTO pms_product (id, stock) VALUES (1, 7)");
        jdbc.update("INSERT INTO oms_order (id, user_id, status) VALUES (51, 4, 0)");
        jdbc.update("INSERT INTO oms_order_item (id, order_id, product_id, amount) VALUES (1, 51, 1, 2)");
    }

    @AfterEach
    void closeDatabase() {
        if (database != null) {
            database.shutdown();
        }
    }

    @Test
    void repeatedCancellationReturnsStockOnceAndBlocksPayment() {
        assertThat(cancellations.cancel(51)).isEqualTo(OrderStatusEnum.CANCELLED);
        assertThat(cancellations.cancel(51)).isEqualTo(OrderStatusEnum.CANCELLED);
        assertThat(orders.payPending(payment(), 1)).isZero();
        assertThat(stock()).isEqualTo(9);
        verifyNoInteractions(reservations);
    }

    @Test
    void paidOrderKeepsStockAndCannotBePaidTwice() {
        assertThat(orders.payPending(payment(), 1)).isEqualTo(1);
        assertThat(orders.payPending(payment(), 1)).isZero();
        assertThat(cancellations.cancel(51)).isEqualTo(OrderStatusEnum.PAID);
        assertThat(stock()).isEqualTo(7);
        verifyNoInteractions(reservations);
    }

    @Test
    void missingProductRollsBackCancellationAndOtherStockUpdates() {
        jdbc.update("INSERT INTO oms_order_item (id, order_id, product_id, amount) VALUES (2, 51, 999, 1)");

        assertThatThrownBy(() -> cancellations.cancel(51)).isInstanceOf(IllegalStateException.class);
        assertThat(orders.selectById(51).getStatus()).isEqualTo(OrderStatusEnum.PENDING_PAY);
        assertThat(stock()).isEqualTo(7);
        verifyNoInteractions(reservations);
    }

    @Test
    void redisFailurePreservesAReplayableReleaseWithoutReturningDatabaseStockTwice() {
        jdbc.update("UPDATE oms_order SET seckill_session_id = 83 WHERE id = 51");
        doThrow(new IllegalStateException("Redis unavailable")).doNothing()
                .when(reservations).restoreCancelledStock(83, 51);

        assertThatThrownBy(() -> cancellations.cancel(51)).hasMessage("Redis unavailable");
        assertThat(orders.selectById(51).getStatus()).isEqualTo(OrderStatusEnum.CANCELLED);
        assertThat(orders.pendingStockRelease(51)).isEqualTo(83);
        assertThat(stock()).isEqualTo(9);

        assertThat(cancellations.cancel(51)).isEqualTo(OrderStatusEnum.CANCELLED);
        assertThat(orders.pendingStockRelease(51)).isNull();
        assertThat(stock()).isEqualTo(9);
    }

    private Order payment() {
        return new Order().setId(51).setUserId(4).setPaymentTime(LocalDateTime.now())
                .setPaymentSn("test-payment");
    }

    private int stock() {
        return jdbc.queryForObject("SELECT stock FROM pms_product WHERE id = 1", Integer.class);
    }
}
