package com.xm.sdalg.commons

import redis.clients.jedis._
import scala.collection.JavaConverters._

/**
 * Redis工具类 (使用Jedis操作Redis in Scala)
 *
 * 提供了简单易用的方法来操作Redis的五种数据结构：String, Hash, List, Set 和 Sorted Set。
 *
 * '''Reference:'''
 * - [[https://xm.f.mioffice.cn/wiki/wikk4Mx3zL42BzkBD6owYQMnaxd]]
 *
 * '''Usage示例:'''
 * {{{
 * val jedis = getJedis or getJedisCluster // 获取连接池
 *
 * // 1. 插入字符串（String）
 * jedis.set("myKey", "myValue")
 * println(jedis.get("myKey"))
 *
 * // 2. 插入哈希表（Hash）
 * jedis.hset("myHash", "field1", "value1")
 * jedis.hset("myHash", "field2", "value2")
 * println(jedis.hgetAll("myHash"))
 *
 * // 3. 插入列表（List）
 * jedis.rpush("myList", "value1", "value2", "value3")
 * println(jedis.lrange("myList", 0, -1))
 *
 * // 4. 插入集合（Set）
 * jedis.sadd("mySet", "member1", "member2", "member3")
 * println(jedis.smembers("mySet"))
 *
 * // 5. 插入有序集合（Sorted Set）
 * jedis.zadd("myZset", 1, "member1")
 * jedis.zadd("myZset", 2, "member2")
 * jedis.zadd("myZset", 3, "member3")
 * println(jedis.zrange("myZset", 0, -1))
 * }}}
 *
 */
object RedisUtils {

    /**
     * 获取完整的JedisPoolConfig配置。
     *
     * @param maxTotal                      最大连接数。默认为200。
     * @param maxIdle                       最大空闲连接数。默认为100。
     * @param minIdle                       最小空闲连接数。默认为10。
     * @param blockWhenExhausted            连接耗尽时是否阻塞，false则报异常。默认为true。
     * @param maxWaitMillis                 当连接耗尽时，调用者的最大等待时间（单位为毫秒）。默认为-1，即永不超时。
     * @param testWhileIdle                 在空闲时检查连接的可用性。默认为true。
     * @param minEvictableIdleTimeMillis    一个连接在池中最小生存的时间（单位为毫秒），然后可以被清除。默认为30000。
     * @param timeBetweenEvictionRunsMillis 对空闲连接进行清除的周期。单位是毫秒。默认为30000。
     * @param numTestsPerEvictionRun        在每次空闲连接清除周期中，要检查的连接数。默认为3。
     * @return 返回JedisPoolConfig配置。
     */
    def getJedisPoolConfig(maxTotal: Int = 200,
                           maxIdle: Int = 100,
                           minIdle: Int = 10,
                           blockWhenExhausted: Boolean = true,
                           maxWaitMillis: Long = -1L,
                           testWhileIdle: Boolean = true,
                           minEvictableIdleTimeMillis: Long = 30000,
                           timeBetweenEvictionRunsMillis: Long = 30000,
                           numTestsPerEvictionRun: Int = 3): JedisPoolConfig = {
        val config = new JedisPoolConfig()
        config.setMaxTotal(maxTotal)
        config.setMaxIdle(maxIdle)
        config.setMinIdle(minIdle)
        config.setBlockWhenExhausted(blockWhenExhausted)
        config.setMaxWaitMillis(maxWaitMillis)
        config.setTestWhileIdle(testWhileIdle)
        config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis)
        config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis)
        config.setNumTestsPerEvictionRun(numTestsPerEvictionRun)
        config
    }

    /**
     * redis集群模式获得连接
     *
     * @param host 主机数组
     * @param port 端口数组
     * @param      ...                                                                                                                                (其他参数，与getJedisPoolConfig中的参数相同)
     * @return 返回JedisCluster连接
     */
    def getJedisCluster(host: Array[String],
                        port: Array[Int],
                        redisTimeout: Int = 30000,
                        maxAttempts: Int = 5,
                        maxTotal: Int = 200,
                        maxIdle: Int = 100,
                        minIdle: Int = 10,
                        blockWhenExhausted: Boolean = true,
                        maxWaitMillis: Long = -1L,
                        testWhileIdle: Boolean = true,
                        minEvictableIdleTimeMillis: Long = 30000,
                        timeBetweenEvictionRunsMillis: Long = 30000,
                        numTestsPerEvictionRun: Int = 3): JedisCluster = {

        if (host.length != port.length) {
            throw new Exception("redis集群连接池主机与端口个数不匹配")
        }
        val config = getJedisPoolConfig(maxTotal, maxIdle, minIdle,
            blockWhenExhausted, maxWaitMillis,
            testWhileIdle, minEvictableIdleTimeMillis,
            timeBetweenEvictionRunsMillis, numTestsPerEvictionRun)
        val jedisClusterNodes = host.zip(port).map { case (h, p) => new HostAndPort(h, p) }.toSet.asJava
        new JedisCluster(jedisClusterNodes, redisTimeout, maxAttempts, config)
    }

    /**
     * redis单机模式获得连接
     *
     * @param host 主机
     * @param port 端口
     * @param      ...                                                                                                                                (其他参数，与getJedisPoolConfig中的参数相同)
     * @return 返回Jedis连接
     */
    def getJedis(host: String,
                 port: Int,
                 password: String = null,
                 redisTimeout: Int = 30000,
                 maxTotal: Int = 200,
                 maxIdle: Int = 100,
                 minIdle: Int = 10,
                 blockWhenExhausted: Boolean = true,
                 maxWaitMillis: Long = -1L,
                 testWhileIdle: Boolean = true,
                 minEvictableIdleTimeMillis: Long = 30000,
                 timeBetweenEvictionRunsMillis: Long = 30000,
                 numTestsPerEvictionRun: Int = 3): Jedis = {
        val config = getJedisPoolConfig(maxTotal, maxIdle, minIdle,
            blockWhenExhausted, maxWaitMillis,
            testWhileIdle, minEvictableIdleTimeMillis,
            timeBetweenEvictionRunsMillis, numTestsPerEvictionRun)
        val pool = new JedisPool(config, host, port, redisTimeout, password)
        pool.getResource
    }

}