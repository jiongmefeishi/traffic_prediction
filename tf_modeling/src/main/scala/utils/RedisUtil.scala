package utils

import redis.clients.jedis._

object RedisUtil {
  //配置redis基本连接参数
  val host = "192.168.126.132"
  val port = 6379
  val timeout = 30000

  val config = new JedisPoolConfig

  //设置允许最大的连接个数
  config.setMaxTotal(200)
  //最大空闲连接数
  config.setMaxIdle(50)
  //最小空闲连接数
  config.setMinIdle(8)

  //设置连接时的最大等待毫秒数
  config.setMaxWaitMillis(10000)
  //设置在获取连接时，是否检查连接的有效性
  config.setTestOnBorrow(true)
  //设置释放连接到池中时是否检查有效性
  config.setTestOnReturn(true)

  //在连接空闲时，是否检查连接有效性
  config.setTestWhileIdle(true)

  //两次扫描之间的时间间隔毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000)
  //每次扫描的最多的对象数
  config.setNumTestsPerEvictionRun(10)
  //逐出连接的最小空闲时间，默认是180000（30分钟）
  config.setMinEvictableIdleTimeMillis(60000)

  //连接池
  lazy val pool = new JedisPool(config, host, port, timeout)

  //释放资源
  lazy val hook = new Thread{
    override def run() = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook)
}

