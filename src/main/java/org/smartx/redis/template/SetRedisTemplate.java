package org.smartx.redis.template;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.TreeSet;

/**
 * <b><code>SetRedisTemplate</code></b>
 * <p>
 * Set 数据结构的常用方法封装，支持Object 类型
 * </p>
 *
 * @author kext
 */
@Component
public class SetRedisTemplate extends BasicRedisTemplate {

    /**
     * Add one or more members to a set
     */
    public Boolean sadd(final String key, final String... members) {
        return jc.sadd(key, members) == 1;
    }

    /**
     * Remove one or more members from a set
     */
    public Boolean srem(final String key, final String... members) {
        return jc.srem(key, members) == 1;
    }

    /**
     * Get all the members in a set.
     */
    public Set<String> smembers(final String key) {
        return jc.smembers(key);
    }

    /**
     * For Object, Get all the members in a set.
     */
    public <T> Set<T> smembers(final String key, Class<T> clazz) {
        Set<String> tempSet = jc.smembers(key);
        if (tempSet != null && tempSet.size() > 0) {
            TreeSet<T> result = new TreeSet<>();
            for (String value : tempSet) {
                result.add(fromJson(value, clazz));
            }
            return result;
        }
        return null;
    }

}
