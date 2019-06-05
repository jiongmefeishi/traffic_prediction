package utils

import java.util.Properties

object PropertyUtil {
  val properties = new Properties()

  try{
    //加载配置属性
    val inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties")
    properties.load(inputStream)
  }catch {
    case ex:Exception => println(ex)
  }finally {}

  // 通过key得到kafka的属性值
  def getProperty(key: String): String = properties.getProperty(key)

}
