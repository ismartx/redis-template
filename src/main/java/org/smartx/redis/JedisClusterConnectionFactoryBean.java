package org.smartx.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * redis 连接器
 *
 * @author yaoab
 */
public class JedisClusterConnectionFactoryBean implements FactoryBean<JedisCluster>, InitializingBean, DisposableBean {

    private static Logger logger = LoggerFactory.getLogger(JedisClusterConnectionFactoryBean.class);

    // 格式为： 127.0.0.1:6379,10.1.2.242:6379 多个之间用逗号分隔
    // 对于server的权重目前没有需求，统一用默认值
    private String servers;

    private int timeout = 3000; // 3seconds

    private int maxRedirections = 3;

    private GenericObjectPoolConfig poolConfig;

    private JedisCluster jc;

    @Override
    public JedisCluster getObject() throws Exception {
        return jc;
    }

    @Override
    public Class<?> getObjectType() {
        return (this.jc != null ? this.jc.getClass() : JedisCluster.class);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (null == poolConfig) {
            throw new RedisException("Pool config cant not be null");
        }
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        for (String server : servers.split("[,]")) {
            String[] sa = server.split("[:]");
            if (sa.length == 2) {
                String host = sa[0];
                int port = Integer.parseInt(sa[1]);
                jedisClusterNodes.add(new HostAndPort(host, port));
            }
        }
        jc = new JedisCluster(jedisClusterNodes, timeout, maxRedirections, poolConfig);
    }

    @Override
    public void destroy() throws Exception {
        if (jc != null) {
            try {
                jc.close();
            } catch (Exception ex) {
                logger.warn("Can not close Jedis cluster", ex);
            }
            jc = null;
        }
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getMaxRedirections() {
        return maxRedirections;
    }

    public void setMaxRedirections(int maxRedirections) {
        this.maxRedirections = maxRedirections;
    }

    public GenericObjectPoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

}
