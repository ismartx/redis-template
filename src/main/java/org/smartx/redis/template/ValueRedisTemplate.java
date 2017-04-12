package org.smartx.redis.template;

import org.springframework.stereotype.Component;

/**
 * <b><code>ValueRedisTemplate</code></b>
 * <p>
 * Strings 数据结构的常用方法封装，支持Object 类型
 * </p>
 *
 * @author kext
 */
@Component
public class ValueRedisTemplate extends BasicRedisTemplate {

    /**
     * set key-value
     *
     * @param value String
     */
    public void set(String key, String value) {
        set(key, value, 0);
    }

    /**
     * set key-value
     *
     * @param value String
     */
    public void set(String key, String value, int ttl) {
        jc.set(key, value);
        if (ttl > 0) {
            this.expire(key, ttl);
        }
    }

    /**
     * set key-value For Object(NOT String)
     *
     * @param value Object
     */
    public void set(String key, Object value) {
        set(key, value, 0);
    }

    /**
     * set key-value For Object(NOT String)
     *
     * @param o   Object
     * @param ttl int
     */
    public <T> void set(String key, Object o, int ttl) {
        if (null == o) {
            return;
        }
        String value = toJsonString(o);
        jc.set(key, value);
        if (ttl > 0) {
            jc.expire(key, ttl);
        }
    }

    /**
     * set key-value with expired time(s)
     */
    public String setex(String key, int seconds, String value) {
        return jc.setex(key, seconds, value);

    }

    /**
     * set key-value For Object(NOT String) with expired time(s)
     */
    public String setex(String key, int seconds, Object value) {
        return jc.setex(key, seconds, toJsonString(value));
    }

    /**
     * 如果key还不存在则进行设置，返回true，否则返回false.
     */
    public boolean setnx(String key, String value, int ttl) {
        Long reply = jc.setnx(key, value);
        if (ttl > 0) {
            this.expire(key, ttl);
        }
        return reply == 1;
    }

    /**
     * 如果key还不存在则进行设置，返回true，否则返回false.
     */
    public boolean setnx(String key, String value) {
        return setnx(key, value, 0);
    }

    /**
     * 如果key还不存在则进行设置 For Object，返回true，否则返回false.
     */
    public boolean setnx(String key, Object value) {
        return jc.setnx(key, toJsonString(value)) == 1;
    }

    /**
     * 如果key还不存在则进行设置 For Object，返回true，否则返回false.
     */
    public boolean setnx(String key, Object value, int ttl) {
        Long reply = jc.setnx(key, toJsonString(value));
        if (ttl > 0) {
            this.expire(key, ttl);
        }
        return reply == 1;
    }

    /**
     * 如果key不存在, 返回null.
     */
    public String get(String key) {
        return jc.get(key);
    }

    /**
     * For Object, 如果key不存在, 返回null.
     */
    public <T> T get(String key, Class<T> clazz) {
        return fromJson(jc.get(key), clazz);
    }

    /**
     * 自增 +1
     *
     * @return 返回自增后结果
     */
    public Long incr(final String key) {
        return jc.incr(key);
    }

    /**
     * 自减 -1
     *
     * @return 返回自减后结果
     */
    public Long decr(final String key) {
        return jc.decr(key);
    }

}
