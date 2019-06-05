package modeling

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer

/**
  * 数据建模
  */
object Train {
  def main(args: Array[String]): Unit = {
    //写入文件的输出流
    val writer = new PrintWriter(new File("model_train.txt"))
    //初始化spark
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TrafficModel")
    val sc = new SparkContext(sparkConf)

    //定义redis的数据库相关
    val dbIndex = 1
    //获取redis连接
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)

    //设立目标监测点：你要对哪几个监测点进行建模
    val monitorIDs = List("0005", "0015")
    //取出相关监测点
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017"))

    //遍历上边所有的监测点，读取数据
    monitorIDs.map(monitorID => {
      //得到当前“目标监测点”的相关监测点
      val monitorRelationArray = monitorRelations(monitorID)//得到的是Array（相关监测点）

      //初始化时间
      val currentDate = Calendar.getInstance().getTime
      //当前小时分钟数
      val hourMinuteSDF = new SimpleDateFormat("HHmm")
      //当前年月日
      val dateSDF = new SimpleDateFormat("yyyyMMdd")
      //以当前时间，格式化好年月日的时间
      val dateOfString = dateSDF.format(currentDate)

      //根据“相关监测点”，取得当日的所有的监测点的平均车速
      //最终结果样式：(0005, {1033=93_2, 1034=1356_30})
      val relationsInfo = monitorRelationArray.map(monitorID => {
        (monitorID, jedis.hgetAll(dateOfString + "_" + monitorID))
      })

      //确定使用多少小时内的数据进行建模
      val hours = 1
      //创建3个数组，一个数组用于存放特征向量，一数组用于存放Label向量，一个数组用于存放前两者之间的关联
      val dataX = ArrayBuffer[Double]()
      val dataY = ArrayBuffer[Double]()

      //用于存放特征向量和特征结果的映射关系
      val dataTrain = ArrayBuffer[LabeledPoint]()

      //将时间拉回到1个小时之前，倒序，拉回单位：分钟

      for(i <- Range(60 * hours, 2, -1)){
        dataX.clear()
        dataY.clear()
        //以下内容包含：线性滤波
        for(index <- 0 to 2){
          //当前毫秒数 - 1个小时之前的毫秒数 + 1个小时之前的后0分钟，1分钟，2分钟的毫秒数（第3分钟作为Label向量）
          val oneMoment = currentDate.getTime - 60 * i * 1000 + 60 * index * 1000
          //拼装出当前(当前for循环这一次的时间)的小时分钟数
          val oneHM = hourMinuteSDF.format(new Date(oneMoment))//1441 field
          //取得该时刻下里面数据
          //取出的数据形式距离：(0005, {1033=93_2, 1034=1356_30})
          for((k, v) <- relationsInfo){
            //如果index==2，意味着前3分钟的数据已经组装到了dataX中，那么下一时刻的数据，如果是目标卡口，则需要存放于dataY中
            if(k == monitorID && index == 2){
              val nextMoment = oneMoment + 60 * 1000
              val nextHM = hourMinuteSDF.format(new Date(nextMoment))//1027

              //判断是否有数据
              if(v.containsKey(nextHM)){
                val speedAndCarCount = v.get(nextHM).split("_")
                val valueY = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat//得到第4分钟的平均车速
                dataY += valueY
              }
            }

            //组装前3分钟的dataX
            if(v.containsKey(oneHM)){
              val speedAndCarCount = v.get(oneHM).split("_")
              val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat//得到当前这一分钟的特征值
              dataX += valueX
            }else{
              dataX += 60.0F  // 车速
            }
          }
        }
        //准备训练模型
        //先将dataX和dataY映射于一个LabeledPoint对象中
        if(dataY.toArray.length == 1){
          val label = dataY.toArray.head//答案的平均车速
          //label的取值范围是：0~15， 30~60  ----->  0, 1, 2, 3, 4, 5, 6
          //真实情况：0~120KM/H车速，划分7个级别，公式就如下：
          val record = LabeledPoint(if (label / 10 < 6) (label / 10).toInt else 6, Vectors.dense(dataX.toArray))
          dataTrain += record
        }
      }

      //将数据集写入到文件中方便查看
      dataTrain.foreach(record => {
        println(record)
        writer.write(record.toString() + "\n")
      })

      //开始组装训练集和测试集
      val rddData = sc.parallelize(dataTrain)
      val randomSplits = rddData.randomSplit(Array(0.6, 0.4), 11L)
      //训练集
      val trainData = randomSplits(0)
      //测试集
      val testData = randomSplits(1)

      //使用训练集进行建模
      val model = new LogisticRegressionWithLBFGS().setNumClasses(7).run(trainData)
      //完成建模之后，使用测试集，评估模型精确度
      val predictionAndLabels = testData.map{
        case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
      }

      //得到当前评估值
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val accuracy = metrics.accuracy//取值范围：0.0~1.0

      println("评估值：" + accuracy)
      writer.write(accuracy + "\n")

      //设置评估阈值:超过多少精确度，则保存模型
      if(accuracy > 0.0){
        //将模型保存到HDFS
//        val hdfsPath = "hdfs://192.168.126.132:8020/traffic/model/" +
        val hdfsPath = "hdfs://192.168.126.132:9000/traffic/model/" +
          monitorID +
          "_" +
          new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()))
        model.save(sc, hdfsPath)
        jedis.hset("model", monitorID, hdfsPath)
      }
    })

    RedisUtil.pool.returnResource(jedis)
    writer.flush
    writer.close
  }
}
