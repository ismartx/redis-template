package org.smartx.redis.template;


import org.smartx.commons.utils.JsonUtils;
import org.smartx.redis.RedisException;
import org.springframework.beans.factory.InitializingBean;

import javax.annotation.Resource;

import redis.clients.jedis.JedisCluster;

/**
 * <b><code>BasicRedisTemplate</code></b>
 * <p>
 * redis 基础方法
 * </p>
 *
 * @author kext
 */
public abstract class BasicRedisTemplate implements InitializingBean {

    @Resource
    protected JedisCluster jc;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (null == jc) {
            throw new RedisException("Jedis cluster can not be null");
        }
    }

    /**
     * 删除key, 如果key存在返回true, 否则返回false。
     */
    public boolean del(String key) {
        return jc.del(key) == 1;

    }

    /**
     * true if the key exists, otherwise false
     */
    public Boolean exists(String key) {
        return jc.exists(key);
    }

    /**
     * set key expired time
     */
    public Boolean expire(String key, int seconds) {
        return seconds == 0 || jc.expire(key, seconds) == 1;
    }

    /**
     * 把object转换为json byte array
     */
    protected String toJsonString(Object o) {
        return JsonUtils.toJsonString(o);
    }

    /**
     * 把json byte array转换为T类型object
     */
    protected <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.length() == 0) {
            return null;
        }
        return JsonUtils.parseJson(json, clazz);
    }

}
