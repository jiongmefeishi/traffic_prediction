package predict

import java.text.SimpleDateFormat

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer

/**
  * 堵车预测
  */
object Prediction {
  def main(args: Array[String]): Unit = {
    //配置spark
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TrafficPredict")

    val sc = new SparkContext(sparkConf)

    //时间设置，目的是：为了拼凑出redis中的key和field的字符串
    val dateSDF = new SimpleDateFormat("yyyyMMdd")
    val hourMinuteSDF = new SimpleDateFormat("HHmm")
    //2019-02-05 17:00
    val userSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm")

    //定义用于传入的，想要预测是否堵车的日期
    //    val inputDateString = "2019-06-04 15:41"
    // 这里为了测试准确性，使用历史数据进行预测，即由模型推原历史数据
    val inputDateString = "2019-06-04 13:56"
    val inputDate = userSDF.parse(inputDateString)

    //得到redis中的key,例如：20190205
    val dayOfInputDate = dateSDF.format(inputDate)
    //1700
    val hourMinuteOfInputDate = hourMinuteSDF.format(inputDate)

    val dbIndex = 1
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)


    //想要预测的监测点
    val monitorIDs = List("0005", "0015")
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017"))

    monitorIDs.map(monitorID => {
      val monitorRealtionArray = monitorRelations(monitorID)
      val relationInfo = monitorRealtionArray.map(monitorID => {
        (monitorID, jedis.hgetAll(dayOfInputDate + "_" + monitorID))
      })

      //装载目标时间点之前的3分钟的历史数据
      val dataX = ArrayBuffer[Double]()

      //组装数据的过程
      for (index <- Range(3, 0, -1)) {
        val oneMoment = inputDate.getTime - 60 * index * 1000
        val oneHM = hourMinuteSDF.format(oneMoment) //1657

        for ((k, v) <- relationInfo) {
          if (v.containsKey(oneHM)) {
            val speedAndCarCount = v.get(oneHM).split("_")
            val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
            dataX += valueX
          } else {
            dataX += 60.0F
          }
        }
      }

      // 打印三分钟之前的数据
      println(dataX)
      //加载模型
      val modelPath = jedis.hget("model", monitorID)
      val model = LogisticRegressionModel.load(sc, modelPath)

      //预测
      val prediction = model.predict(Vectors.dense(dataX.toArray))
      println(monitorID + ",堵车评估值：" + prediction + ",是否通畅：" + (if (prediction >= 3) "通畅" else "拥堵"))
    })
    RedisUtil.pool.returnResource(jedis)
  }
}
