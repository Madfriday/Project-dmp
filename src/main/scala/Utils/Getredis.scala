package Utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object Getredis {
  private val jedis = new JedisPool(new GenericObjectPoolConfig(),"192.168.9.13",6379,300000)
 def jedi() = jedis.getResource


}
