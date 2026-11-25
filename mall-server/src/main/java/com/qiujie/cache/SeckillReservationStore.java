package com.qiujie.cache;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

@Component
public class SeckillReservationStore {
    public static final long RESERVED = 1L;
    public static final long DUPLICATE = -1L;
    public static final long SOLD_OUT = -2L;
    private static final String LEGACY = "legacy";

    private static final DefaultRedisScript<Long> RESERVE = new DefaultRedisScript<>("""
            if redis.call('EXISTS', KEYS[2]) == 1 then return -1 end
            local raw = redis.call('GET', KEYS[1])
            local stock = raw and tonumber((raw:gsub('"', ''))) or nil
            if raw and not stock then return redis.error_reply('Invalid seckill stock') end
            if not raw then stock = tonumber(ARGV[1]) end
            if stock <= 0 then return -2 end
            redis.call('SET', KEYS[1], stock - 1)
            redis.call('SET', KEYS[2], 'reserved:' .. ARGV[2], 'EX', ARGV[3])
            redis.call('DEL', KEYS[3])
            return 1
            """, Long.class);

    private static final DefaultRedisScript<Long> CLAIM = new DefaultRedisScript<>("""
            local mark = redis.call('GET', KEYS[1])
            local processing = 'processing:' .. ARGV[1]
            if mark == processing then return 1 end
            if mark == 'reserved:' .. ARGV[1]
                or (ARGV[1] == 'legacy' and (mark == '1' or mark == '"1"')) then
                redis.call('SET', KEYS[1], processing, 'KEEPTTL')
                return 1
            end
            return 0
            """, Long.class);

    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>("""
            local mark = redis.call('GET', KEYS[2])
            if mark ~= 'reserved:' .. ARGV[1] then
                if ARGV[2] == '0' then return 0 end
                if mark ~= 'processing:' .. ARGV[1]
                    and not (ARGV[1] == 'legacy' and (mark == '1' or mark == '"1"')) then
                    return 0
                end
            end
            local raw = redis.call('GET', KEYS[1])
            if raw then
                local stock = tonumber((raw:gsub('"', '')))
                if not stock then return redis.error_reply('Invalid seckill stock') end
                redis.call('SET', KEYS[1], stock + 1)
            end
            redis.call('DEL', KEYS[2])
            redis.call('SET', KEYS[3], cjson.encode('{"status":-1,"msg":"秒杀失败"}'), 'EX', 120)
            return 1
            """, Long.class);

    private static final DefaultRedisScript<Long> COMPLETE = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) ~= 'processing:' .. ARGV[1] then return 0 end
            redis.call('SET', KEYS[2], cjson.encode('{"status":1,"msg":"秒杀成功"}'), 'EX', 120)
            return 1
            """, Long.class);

    private static final DefaultRedisScript<Long> RETURN_CANCELLED = new DefaultRedisScript<>("""
            if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
            local raw = redis.call('GET', KEYS[1])
            if raw then
                local stock = tonumber((raw:gsub('"', '')))
                if not stock then return redis.error_reply('Invalid seckill stock') end
                redis.call('SET', KEYS[1], stock + 1)
            end
            redis.call('SET', KEYS[2], '1')
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;

    public SeckillReservationStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public long reserve(int sessionId, int userId, int initialStock, String reservationId, Duration retention) {
        return Objects.requireNonNull(redis.execute(RESERVE, keys(sessionId, userId),
                Integer.toString(initialStock), reservationId, Long.toString(retention.toSeconds())));
    }

    public boolean claim(int sessionId, int userId, String reservationId) {
        return Long.valueOf(1).equals(redis.execute(CLAIM, List.of(orderKey(sessionId, userId)),
                token(reservationId)));
    }

    public boolean releaseUnclaimed(int sessionId, int userId, String reservationId) {
        return Long.valueOf(1).equals(redis.execute(RELEASE, keys(sessionId, userId), reservationId, "0"));
    }

    public void failReservation(int sessionId, int userId, String reservationId) {
        redis.execute(RELEASE, keys(sessionId, userId), token(reservationId), "1");
    }

    public void recordSuccess(int sessionId, int userId, String reservationId) {
        redis.execute(COMPLETE, List.of(orderKey(sessionId, userId), resultKey(sessionId, userId)),
                token(reservationId));
    }

    public void restoreCancelledStock(int sessionId, int orderId) {
        redis.execute(RETURN_CANCELLED, List.of("seckill:stock:" + sessionId, returnedKey(orderId)));
    }

    public void expireCancelledStockMarker(int orderId) {
        redis.expire(returnedKey(orderId), Duration.ofDays(7));
    }

    private static List<String> keys(int sessionId, int userId) {
        return List.of("seckill:stock:" + sessionId, orderKey(sessionId, userId), resultKey(sessionId, userId));
    }

    private static String orderKey(int sessionId, int userId) {
        return "seckill:order:" + sessionId + ":" + userId;
    }

    private static String resultKey(int sessionId, int userId) {
        return "seckill:result:" + sessionId + ":" + userId;
    }

    private static String returnedKey(int orderId) {
        return "mallcore:cancelled-stock:" + orderId;
    }

    private static String token(String reservationId) {
        return reservationId == null ? LEGACY : reservationId;
    }
}
