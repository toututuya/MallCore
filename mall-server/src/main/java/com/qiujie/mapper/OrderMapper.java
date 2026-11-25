package com.qiujie.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.qiujie.entity.Order;
import com.qiujie.vo.CategorySalesVO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface OrderMapper extends BaseMapper<Order> {

    @Update("""
            UPDATE oms_order SET status = 4, update_time = CURRENT_TIMESTAMP
            WHERE id = #{id} AND status = 0 AND is_deleted = 0
            """)
    int cancelPending(@Param("id") Integer id);

    @Update("""
            UPDATE oms_order SET status = 1, payment_time = #{order.paymentTime},
                payment_sn = #{order.paymentSn}, pay_method = COALESCE(#{payMethod}, pay_method),
                update_time = CURRENT_TIMESTAMP
            WHERE id = #{order.id} AND user_id = #{order.userId} AND status = 0 AND is_deleted = 0
            """)
    int payPending(@Param("order") Order order, @Param("payMethod") Integer payMethod);

    @Select("""
            SELECT id FROM oms_order WHERE user_id = #{userId} AND seckill_session_id = #{sessionId}
            """)
    Integer findSeckillOrderId(@Param("userId") Integer userId, @Param("sessionId") Integer sessionId);

    @Insert("""
            INSERT INTO oms_order_stock_release (order_id, session_id) VALUES (#{orderId}, #{sessionId})
            """)
    void recordStockRelease(@Param("orderId") Integer orderId, @Param("sessionId") Integer sessionId);

    @Select("SELECT session_id FROM oms_order_stock_release WHERE order_id = #{orderId}")
    Integer pendingStockRelease(@Param("orderId") Integer orderId);

    @Delete("DELETE FROM oms_order_stock_release WHERE order_id = #{orderId}")
    int finishStockRelease(@Param("orderId") Integer orderId);

    List<Order> selectByUserId(@Param("userId") Integer userId);

    Order selectDetailById(@Param("id") Integer id);

    IPage<Order> selectPageWithParams(Page<Order> page,
                                        @Param("orderSn") String orderSn,
                                        @Param("userName") String userName,
                                        @Param("statusList") List<Integer> statusList,
                                        @Param("startTime") String startTime,
                                        @Param("endTime") String endTime);

    List<Map<String, Object>> selectTrendData(@Param("days") Integer days);

    List<CategorySalesVO> selectCategorySales();

    /** 聚合查询指定时间范围内的订单数和销售总额，避免拉全量数据到内存 */
    Map<String, Object> selectDailyStats(@Param("start") LocalDateTime start, @Param("end") LocalDateTime end);
}
