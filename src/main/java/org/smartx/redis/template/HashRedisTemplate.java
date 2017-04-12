package org.smartx.redis.template;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * <b><code>HashRedisTemplate</code></b>
 * <p>
 * Hashes 数据结构的常用方法封装，Hashes不宜存放过大数据，暂不开放支持Object 类型
 * </p>
 *
 * @author kext
 */
@Component
public class HashRedisTemplate extends BasicRedisTemplate {

    /**
     * Set the string value of a hash field
     */
    public Boolean hset(final String key, final String field, final String value) {
        return hset(key, field, value, 0);
    }

    /**
     * Set the string value of a hash field
     */
    public Boolean hset(final String key, final String field, final String value, final int ttl) {
        Long reply = jc.hset(key, field, value);
        if (ttl > 0) {
            jc.expire(key, ttl);
        }
        return reply == 1;

    }

    /**
     * Set the Object value of a hash field
     */
    public Boolean hset(final String key, final String field, final Object value) {
        return hset(key, field, value, 0);
    }

    /**
     * Set the Object value of a hash field
     */
    public Boolean hset(final String key, final String field, final Object value, final int ttl) {
        Long reply = jc.hset(key, field, toJsonString(value));
        if (ttl > 0) {
            jc.expire(key, ttl);
        }
        return reply == 1;

    }

    /**
     * Get the value of a hash field
     */
    public String hget(final String key, final String field) {
        return hget(key, field, 0);
    }

    /**
     * Get the value of a hash field
     */
    public String hget(final String key, final String field, int ttl) {
        String res = jc.hget(key, field);
        if (ttl > 0) {
            this.expire(key, ttl);
        }
        return res;
    }

    /**
     * Get the value of a hash field
     */
    public <T> T hget(final String key, final String field, final Class<T> clazz) {
        return hget(key, field, clazz, 0);
    }

    /**
     * Get the value of a hash field
     */
    public <T> T hget(final String key, final String field, final Class<T> clazz, final int ttl) {
        T res = fromJson(jc.hget(key, field), clazz);
        if (ttl > 0) {
            this.expire(key, ttl);
        }
        return res;
    }

    /**
     * Delete one or more hash fields
     */
    public Boolean hdel(final String key, final String... fields) {
        return jc.hdel(key, fields) == 1;
    }

    /**
     * Check if a hash field exists
     */
    public Boolean hexists(final String key, final String field) {
        return jc.hexists(key, field);
    }

    /**
     * Get all the fields and values in a hash
     * 当Hash较大时候，慎用！
     */
    public Map<String, String> hgetAll(final String key) {
        return jc.hgetAll(key);
    }

    /**
     * Get all the fields and values in a hash
     * 当Hash较大时候，慎用！
     */
    public <T> Map<String, T> hgetAllObject(final String key, Class<T> clazz) {
        Map<String, String> byteMap = jc.hgetAll(key);
        if (byteMap != null && byteMap.size() > 0) {
            Map<String, T> map = new HashMap<>();
            for (Entry<String, String> e : byteMap.entrySet()) {
                map.put(e.getKey(), fromJson(e.getValue(), clazz));
            }
            return map;
        }
        return null;
    }

    /**
     * Get the values of all the given hash fields.
     */
    public List<String> hmget(final String key, final String... fields) {
        return jc.hmget(key, fields);
    }

    /**
     * Get the value of a mulit fields
     */
    public Map<String, Object> hmgetObject(final String key, final int ttl, final String... fields) {
        if (null == fields) {
            return null;
        }
        List<String> res = jc.hmget(key, fields);
        Map<String, Object> resMap = null;
        if (null != res) {
            resMap = new HashMap<>();
            for (int i = 0; i < res.size(); i++) {
                resMap.put(fields[i], fromJson(res.get(i), Object.class));
            }
        }
        if (ttl > 0) {
            this.expire(key, ttl);
        }
        return resMap;
    }

    /**
     * Get the value of a mulit fields
     */
    public Map<String, Object> hmgetObject(final String key, final String... fields) {
        return hmgetObject(key, 0, fields);
    }

    /**
     * Set multiple hash fields to multiple values.
     */
    public String hmset(final String key, final Map<String, String> hash) {
        return jc.hmset(key, hash);
    }

    /**
     * Increment the integer value of a hash field by the given number.
     */
    public Long hincrBy(final String key, final String field, final long value) {
        return jc.hincrBy(key, field, value);
    }

    /**
     * Get all the fields in a hash.
     */
    public Set<String> hkeys(final String key) {
        return jc.hkeys(key);
    }

    /**
     * Get all the fields in a hash.
     */
    public <T> Set<T> hkeys(final String key, final Class<T> clazz) {
        Set<String> set = jc.hkeys(key);
        Set<T> objectSet = new HashSet<>();
        if (set != null && set.size() != 0) {
            for (String b : set) {
                objectSet.add(fromJson(b, clazz));
            }
        }
        return objectSet;
    }

    /**
     * Get the number of fields in a hash.
     */
    public Long hlen(final String key) {
        return jc.hlen(key);
    }
}
