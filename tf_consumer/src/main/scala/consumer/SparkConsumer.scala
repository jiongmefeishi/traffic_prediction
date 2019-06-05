package consumer

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{PropertyUtil, RedisUtil}

object SparkConsumer {
  def main(args: Array[String]): Unit = {

    println("初始化spark")
    //初始化Spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TrafficStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ssc/checkpoint")

    //配置kafka参数
    val kafkaParams = Map("metadata.broker.list" -> PropertyUtil.getProperty("metadata.broker.list"))

    //配置kafka主题
    val topics = Set(PropertyUtil.getProperty("kafka.topics"))

    //读取kafka主题中的，每一个事件
    val kafkaLineDStream = KafkaUtils.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](ssc, kafkaParams, topics)
      .map(_._2)

    //解析json字符串,最终得到的event的形式是：{"0001" -> "57"}
    val event = kafkaLineDStream.map(line => {
      //使用fastjson解析当前事件中封装的数据信息
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String, String]](){})
      //将这个JavaMap转化成Scala Map
      import scala.collection.JavaConverters._
      val lineScalaMap: collection.mutable.Map[String, String] = mapAsScalaMapConverter(lineJavaMap).asScala
      println(lineScalaMap)
      lineScalaMap
    })

    //将每一条数据按照monitor_id聚合，聚合时每一条数据中的“车辆速度”叠加
    //例如,聚合好的数据形式：(monitor_id, (speed, 1)), (0001, (57, 1))
    //最终结果举例：(0001, (1365, 30))
    val sumOfSpeedAndCount = event
      .map(e => (e.get("monitor_id").get, e.get("speed").get))//("0001", "57")
      .mapValues(s => (s.toInt, 1))//("0001", (57, 1)) (0001, (58, 1))
      .reduceByKeyAndWindow(
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2),
      Seconds(60),
      Seconds(60))

    //定义redis中的数据库索引
    val dbIndex = 1
    //将采集到的数据，按照每分钟放置于redis中，将用于后边的数据建模
    sumOfSpeedAndCount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        partitionRecords
          .filter((tuple: (String, (Int, Int))) => tuple._2._2 > 0)//过滤车辆个数小于1的情况
          .foreach(pair => {
          //开始取出这60秒的window中所有的聚合数据
          //例如：1356_30
          val jedis = RedisUtil.pool.getResource
          val monitorId = pair._1
          val sumOfSpeed = pair._2._1
          val sumOfCarCount = pair._2._2

          //两种情况：
          //1、数据生产时，会产生时间戳字段，流入到kafka的事件中
          //2、实时数据，数据消费的时间，就是数据时间
          val currentTime = Calendar.getInstance().getTime
          val dateSDF = new SimpleDateFormat("yyyyMMdd")//用于redis中的key
          val hourMinuteSDF = new SimpleDateFormat("HHmm")//用于redis中的fields

          val hourMinuteTime = hourMinuteSDF.format(currentTime)//1634
          val date = dateSDF.format(currentTime)//20180203

          jedis.select(dbIndex)
          jedis.hset(date + "_" + monitorId, hourMinuteTime, sumOfSpeed + "_" + sumOfCarCount)
          println(date + "_" + monitorId)
          RedisUtil.pool.returnResource(jedis)
        })
      })
    })

    //spark开始工作
    ssc.start()
    ssc.awaitTermination()
  }
}
