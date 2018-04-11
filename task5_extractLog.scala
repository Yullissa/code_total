/**
  * Created by admin on 2018/4/5.
  */

package com.yidian.youking

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer

/**
  * Created by admin on 2018/4/4.
  */
object extractLog {
  val LOG = LoggerFactory.getLogger(classOf[extractLog])
  //  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  case class Config(input_data: String = "",
                    output_data: String = "",
                    start_time: String = "",
                    end_time: String = "") extends Serializable

  @transient val CONFIG_PARSER = new scopt.OptionParser[Config]("VideoInfoDumper") {
    head("generate clusterset2news trainning data")
    opt[String]('i', "input_data").required().valueName("<hdfs path>").action((x, c) =>
      c.copy(input_data = x)).text("input hdfs path")
    opt[String]('o', "output_data").required().valueName("<hdfs path>").action((x, c) =>
      c.copy(output_data = x)).text("output hdfs path")
    opt[String]('s', "start_time").required().valueName("<time>").action((x, c) =>
      c.copy(start_time = x)).text("start_time")
    opt[String]('e', "end_time").required().valueName("<time>").action((x, c) =>
      c.copy(end_time = x)).text("end_time")
  }

  def main(args: Array[String]) {
    CONFIG_PARSER.parse(args, Config()) match {
      case Some(config) =>
        val sparkConf = new SparkConf().setAppName("vivobrowserlog_extract")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.kryoserializer.buffer.max", "1024m")
        val pathArr: ArrayBuffer[String] = new extractLog().parsePath(sparkConf, config)
        val ret = new extractLog().run(sparkConf, config, pathArr)
      case _ =>
    }
  }
}

class extractLog extends Serializable {

  import com.yidian.youking.extractLog.Config

  def run(sparkConf: SparkConf, config: Config, pathArr: ArrayBuffer[String]): Unit = {
    val sc = new SparkContext(sparkConf)

    if (pathArr.size >= 1) {
      val realPath = config.input_data + pathArr(0)
      var viewRdd = sc.textFile(realPath)
      pathArr.remove(0, 1)
      pathArr.foreach(timePath => {
        val realPath = config.input_data + timePath
        viewRdd = viewRdd union (sc.textFile(realPath))
      })

      viewRdd.map(line => {
        if (JSON.parseObject(line).containsKey("appid")) {
          if (JSON.parseObject(line).get("appid") == "vivobrowser") {
            line
          } else {
            "None"
          }
        } else {
          "None"
        }
      }
      ).filter(_ != "None").repartition(100).saveAsTextFile(config.output_data, classOf[org.apache.hadoop.io.compress.GzipCodec])
    }
  }

  def parsePath(sparkConf: SparkConf, config: Config): ArrayBuffer[String] = {
    val dateArray: ArrayBuffer[String] = ArrayBuffer()

    val dateFiled = Calendar.DAY_OF_MONTH
    val hourFiled = Calendar.HOUR_OF_DAY

    val hourFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
    val startHour = config.start_time
    val endHour = config.end_time
    var beginHour = hourFormat.parse(startHour)
    val calendarH = Calendar.getInstance()

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startDay = config.start_time.substring(0, 10)
    val endDay = config.end_time.substring(0, 10)
    var beginDate = dateFormat.parse(startDay)
    val endDate = dateFormat.parse(endDay)
    val calendarD = Calendar.getInstance()
    calendarD.setTime(beginDate)
    calendarD.add(dateFiled, 1)
    var beginDate2 = calendarD.getTime

    val beginHourEnd = hourFormat.parse(startHour.substring(0, 11) + "23")
    calendarH.setTime(beginHour)
    while (beginHour.compareTo(beginHourEnd) <= 0) {
      dateArray += hourFormat.format(beginHour).substring(0, 10) + "/" + hourFormat.format(beginHour).substring(11, 13) + "/*"
      calendarH.add(hourFiled, 1)
      beginHour = calendarH.getTime
    }

    var lastHour = hourFormat.parse(endHour)
    var lastHourBegin = hourFormat.parse(endHour.substring(0, 11) + "00")
    calendarH.setTime(lastHourBegin)
    while (lastHourBegin.compareTo(lastHour) <= 0) {
      dateArray += hourFormat.format(lastHourBegin).substring(0, 10) + "/" + hourFormat.format(lastHourBegin).substring(11, 13) + "/*"
      calendarH.add(hourFiled, 1)
      lastHourBegin = calendarH.getTime
    }

    while (beginDate2.compareTo(endDate) < 0) {
      dateArray += dateFormat.format(beginDate2) + "/*"
      calendarD.add(dateFiled, 1)
      beginDate2 = calendarD.getTime
    }
    dateArray
  }
}
