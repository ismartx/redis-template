package org.smartx.redis.template;

import com.google.common.collect.Lists;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * <b><code>ListRedisTemplate</code></b>
 * <p>
 * Lists 数据结构的常用方法封装，支持Object 类型
 * </p>
 *
 * @author kext
 */
@Component
public class ListRedisTemplate extends BasicRedisTemplate {

    /**
     * Prepend one or multiple values to a list
     */
    public void lpush(String key, String... values) {
        if (null == values) {
            return;
        }
        jc.lpush(key, values);
    }

    /**
     * Prepend one or multiple values to a list
     */
    public void lpush(String key, Object... values) {
        if (null == values) {
            return;
        }
        String[] strings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            strings[i] = toJsonString(values[i]);
        }
        jc.lpush(key, strings);
    }

    /**
     * Append one or multiple values to a list
     */
    public void rpush(String key, String... values) {
        if (values == null) {
            return;
        }
        jc.rpush(key, values);
    }

    /**
     * Prepend one or multiple values to a list
     */
    public void rpush(String key, Object... values) {
        if (null == values) {
            return;
        }
        String[] strings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            strings[i] = toJsonString(values[i]);
        }
        jc.rpush(key, strings);
    }

    /**
     * Remove and get the last element in a list
     */
    public String rpop(String key) {
        return jc.rpop(key);
    }

    /**
     * For Object, Remove and get the last element in a list
     */
    public <T> T rpop(String key, Class<T> clazz) {
        return fromJson(jc.rpop(key), clazz);
    }

    /**
     * Remove and get the first element in a list
     */
    public String lpop(String key) {
        return jc.lpop(key);
    }

    /**
     * For Object, Remove and get the first element in a list
     */
    public <T> T lpop(String key, Class<T> clazz) {
        return fromJson(jc.lpop(key), clazz);
    }

    /**
     * Get the length of a list
     */
    public Long llen(String key) {
        return jc.llen(key);
    }

    /**
     * 删除List中的等于value的元素
     *
     * count = 1 :删除第一个；
     * count = 0 :删除所有；
     * count = -1:删除最后一个；
     */
    public Long lrem(String key, long count, String value) {
        return jc.lrem(key, count, value);
    }

    /**
     * Get a range of elements from a list.
     * <P>
     * For example LRANGE foobar 0 2 will return the first three elements of the
     * list.
     * </p>
     * <P>
     * For example LRANGE foobar -1 -2 will return the last two elements of the
     * list.
     * </p>
     */
    public List<String> lrange(String key, long start, long end) {
        return jc.lrange(key, start, end);
    }

    /**
     * For Object, Get a range of elements from a list.
     * <P>
     * For example LRANGE foobar 0 2 will return the first three elements of the
     * list.
     * </p>
     * <P>
     * For example LRANGE foobar -1 -2 will return the last two elements of the
     * list.
     * </p>
     */
    public <T> List<T> lrange(String key, long start, long end, Class<T> clazz) {
        List<String> list = jc.lrange(key, start, end);
        if (list != null && list.size() > 0) {
            List<T> results = Lists.newArrayList();
            for (String bytes : list) {
                results.add(fromJson(bytes, clazz));
            }
            return results;
        }
        return null;
    }
}
