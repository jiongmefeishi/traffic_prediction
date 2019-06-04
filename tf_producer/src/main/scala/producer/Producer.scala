package producer

import java.text.DecimalFormat
import java.util.Calendar

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.PropertyUtil

import scala.util.Random
import java._

import com.alibaba.fastjson.JSON
/**
  * 模拟数据生产
  * 随机产生：监测点id，车速（按照5分钟的频率变换堵车状态）
  * 序列化为Json
  * 发送给kafka
  */
object Producer {
  def main(args: Array[String]): Unit = {
    //读取kafka配置信息
    val props = PropertyUtil.properties
    //创建Kafka生产者对象
    val producer = new KafkaProducer[String, String](props)

    //模拟产生实时数据，单位：秒
    var startTime = Calendar.getInstance().getTimeInMillis() / 1000
    //数据模拟，堵车状态切换的周期单位为：秒
    val trafficCycle = 300
    //开始不停的产生实时数据
    val df = new DecimalFormat("0000")
    while(true){
      //模拟产生监测点id：0001~0020
      val randomMonitorId = df.format(Random.nextInt(20) + 1)
      //模拟车速
      var randomSpeed = "000"
      //得到本条数据产生时的当前时间，单位：秒
      val currentTime = Calendar.getInstance().getTimeInMillis() / 1000
      //每5分钟切换一次公路状态
      if(currentTime - startTime > trafficCycle){
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(15))
        if(currentTime - startTime > trafficCycle * 2){
          startTime = currentTime
        }
      }else{
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(30) + 30)
      }

      //该Map集合用于存放产生出来的数据
      val jsonMap = new util.HashMap[String, String]()
      jsonMap.put("monitor_id", randomMonitorId)
      jsonMap.put("speed", randomSpeed)

      //序列化
      val event = JSON.toJSON(jsonMap)
      println(event)

      //发送事件到kafka集群中
      producer.send(new ProducerRecord[String, String](PropertyUtil.getProperty("kafka.topics"), event.toString))
      Thread.sleep(100)
    }
  }
}
