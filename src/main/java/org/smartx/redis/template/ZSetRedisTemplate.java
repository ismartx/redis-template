package org.smartx.redis.template;

import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import redis.clients.jedis.Tuple;

/**
 * <b><code>ZSetRedisTemplate</code></b>
 * <p>
 * Sorted Sets 数据结构的常用方法封装，支持Object 类型
 * </p>
 *
 * @author kext
 */
@Component
public class ZSetRedisTemplate extends BasicRedisTemplate {

    /**
     * 加入Sorted set, 如果member在Set里已存在, 只更新score并返回false, 否则返回true.
     */
    public Boolean zadd(String key, double score, String member) {
        return jc.zadd(key, score, member) == 1;
    }

    /**
     * For Object, 加入Sorted set, 如果member在Set里已存在, 只更新score并返回false, 否则返回true.
     */
    public Boolean zadd(String key, double score, Object member) {
        return jc.zadd(key, score, toJsonString(member)) == 1;
    }

    /**
     * Return a range of members in a sorted set, by index. Ordered from the lowest to the highest
     * score.
     *
     * @return Ordered from the lowest to the highest score.
     */
    public Set<String> zrange(String key, long start, long end) {
        return jc.zrange(key, start, end);
    }

    /**
     * For Object, Return a range of members in a sorted set, by index.Ordered from the lowest to
     * the highest score.
     *
     * @return Ordered from the lowest to the highest score.
     */
    public <T> Set<T> zrange(String key, long start, long end, Class<T> clazz) {
        Set<String> tempSet = jc.zrange(key, start, end);
        if (tempSet != null && tempSet.size() > 0) {
            TreeSet<T> result = new TreeSet<>();
            for (String value : tempSet) {
                // result.add((T) HessianSerializer.deserialize(value));
                result.add(fromJson(value, clazz));
            }
            return result;
        }
        return null;
    }

    /**
     * Return a range of members in a sorted set, by index. Ordered from the highest to the lowest
     * score.
     *
     * @return Ordered from the highest to the lowest score.
     */
    public Set<String> zrevrange(String key, long start, long end) {
        return jc.zrevrange(key, start, end);
    }

    /**
     * For Object, Return a range of members in a sorted set, by index. Ordered from the highest to
     * the lowest score.
     *
     * @return Ordered from the highest to the lowest score.
     */
    public <T> Set<T> zrevrange(String key, long start, long end, Class<T> clazz) {
        Set<String> tempSet = jc.zrevrange(key, start, end);
        if (tempSet != null && tempSet.size() > 0) {
            Set<T> result = new TreeSet<T>();
            for (String value : tempSet) {
                result.add(fromJson(value, clazz));
            }
            return result;
        }
        return null;
    }

    /**
     * Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     *
     * @return Ordered from the lowest to the highest score.
     */
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return jc.zrangeByScore(key, min, max);
    }

    /**
     * For Object, Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     *
     * @return Ordered from the lowest to the highest score.
     */
    public <T> Set<T> zrangeByScore(final String key, final double min, final double max, Class<T> clazz) {
        Set<String> tempSet = jc.zrangeByScore(key, min, max);
        if (tempSet != null && tempSet.size() > 0) {
            TreeSet<T> result = new TreeSet<>();
            for (String value : tempSet) {
                result.add(fromJson(value, clazz));
            }
            return result;
        }
        return null;
    }

    /**
     * For Object, Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     *
     * @return Ordered from the lowest to the highest score.
     */
    public <T> Set<T> zrangeHashSetByScore(final String key, final double min, final double max, Class<T> clazz) {
        Set<String> tempSet = jc.zrangeByScore(key, min, max);
        if (tempSet != null && tempSet.size() > 0) {
            HashSet<T> result = new HashSet<>();
            for (String value : tempSet) {
                result.add(fromJson(value, clazz));
            }
            return result;
        }
        return null;
    }

    /**
     * Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     *
     * @return Ordered from the highest to the lowest score.
     */
    public Set<String> zrevrangeByScore(final String key, final double min, final double max) {
        return jc.zrevrangeByScore(key, max, min);
    }

    /**
     * For Object, Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     *
     * @return Ordered from the highest to the lowest score.
     */
    public <T> Set<T> zrevrangeByScore(final String key, final double min, final double max, Class<T> clazz) {
        Set<String> tempSet = jc.zrevrangeByScore(key, max, min);
        if (tempSet != null && tempSet.size() > 0) {
            TreeSet<T> result = new TreeSet<>();
            for (String value : tempSet) {
                result.add(fromJson(value, clazz));
            }
            return result;
        }
        return null;
    }

    /**
     * Return a range of members with scores in a sorted set, by index. Ordered from the lowest to
     * the highest score.
     */
    public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
        return jc.zrangeWithScores(key, start, end);
    }

    /**
     * Return a range of members with scores in a sorted set, by index. Ordered from the highest  to
     * the lowest score.
     */
    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
        return jc.zrevrangeWithScores(key, start, end);
    }

    /**
     * Return the all the elements in the sorted set at key with a score between min and max
     * (including elements with score equal to min or max). Ordered from the lowest to the highest
     * score.
     *
     * @return Ordered from the lowest to the highest score.
     */
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return jc.zrangeByScoreWithScores(key, min, max);
    }

    /**
     * Return the all the elements in the sorted set at key with a score between min and max
     * (including elements with score equal to min or max). Ordered from the highest to the lowest
     * score.
     *
     * @return Ordered from the highest to the lowest score.
     */
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double min, final double max) {
        return jc.zrevrangeByScoreWithScores(key, max, min);
    }

    /**
     * Remove one or more members from a sorted set
     */
    public Boolean zrem(final String key, final String... members) {
        return jc.zrem(key, members) == 1;
    }

    /**
     * For Object, Remove one or more members from a sorted set
     */
    public Boolean zrem(final String key, final Object... members) {
        if (null == members) {
            return true;
        }
        String[] strings = new String[members.length];
        for (int j = 0; j < members.length; j++) {
            strings[j] = toJsonString(members[j]);
        }
        return jc.zrem(key, strings) == 1;
    }

    /**
     * Get the score associated with the given member in a sorted set
     */
    public Double zscore(final String key, final String member) {
        return jc.zscore(key, member);
    }

    /**
     * For ObjecGet the score associated with the given member in a sorted set
     */
    public Double zscore(final String key, final Object member) {
        return jc.zscore(key, toJsonString(member));
    }

    /**
     * Remove all elements in the sorted set at key with rank between start and
     * end. Start and end are 0-based with rank 0 being the element with the
     * lowest score. Both start and end can be negative numbers, where they
     * indicate offsets starting at the element with the highest rank. For
     * example: -1 is the element with the highest score, -2 the element with
     * the second highest score and so forth.
     * <p>
     * <b>Time complexity:</b> O(log(N))+O(M) with N being the number of
     * elements in the sorted set and M the number of elements removed by the
     * operation
     */
    public Long zremrangeByRank(String key, long start, long end) {
        return jc.zremrangeByRank(key, start, end);
    }

    /**
     * Get the length of a sorted set
     */
    public Long zcount(final String key, final double min, final double max) {
        return jc.zcount(key, min, max);
    }

}
