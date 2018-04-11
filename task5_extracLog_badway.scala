/**
  * Created by admin on 2018/4/5.
  */

package com.yidian

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable._
import scala.sys.process._

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
        //本地化时长  提高collect回收数据的容量
        sparkConf.set("spark.locality.wait", "10")
        sparkConf.set("spark.driver.maxResultSize", "3g")
        //        sparkConf.set("spark.dynamicAllocation.enabled", "true")
        //        sparkConf.set("spark.shuffle.service.enabled", "true")
        val pathArr: ArrayBuffer[String] = new extractLog().parsePath(sparkConf, config)
        val ret = new extractLog().run(sparkConf, config, pathArr)
      case _ =>
    }
  }
}

class extractLog extends Serializable {

  import com.yidian.extractLog.Config

  def run(sparkConf: SparkConf, config: Config, pathArr: ArrayBuffer[String]): Unit = {
    val sc = new SparkContext(sparkConf)
    //    pathArr.clear()
    //    pathArr += "/Users/admin/IdeaProjects/gzPrimitiveData/0lessSample/test.txt"
    //    pathArr += "/Users/admin/IdeaProjects/gzPrimitiveData/0lessSample/test1.txt"
    //    sc.makeRDD(pathArr)
    val acc = sc.accumulator(0, "My Accumulator")

    pathArr.foreach(timePath => {
      val realPath = config.input_data + timePath
      //      val realPath = timePath
      val viewRdd = sc.textFile(realPath)
      acc += 1
      var vivoBrowserLog: ArrayBuffer[String] = ArrayBuffer[String]()
      viewRdd.map(line => {
        //        acc += 1
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
      ).filter(_ != "None").repartition(10).saveAsTextFile(config.output_data + "tempresult" + acc.toString)
      //      sc.parallelize(vivoBrowserLog).repartition(1).saveAsTextFile(config.output_data)
    })

    val finalPath = config.output_data + "*"
    val viewRdd1 = sc.textFile(finalPath)
    viewRdd1.map(line => {
      line
    }).repartition(1).saveAsTextFile(config.output_data)
  }

  def parsePath(sparkConf: SparkConf, config: Config): ArrayBuffer[String] = {
    //    val clean = "rm -r ./output" !
    //    val sc = new SparkContext(sparkConf)
    val monLi: List[Int] = List(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    //    val monLiBD = sc.broadcast(monLi)
    val inputBasePath = config.input_data
    val outputPath = config.output_data
    //2018-03-20-07 2018-03-20-07
    val startTimeArr = config.start_time.split("\\-")
    val endTimeArr = config.end_time.split("\\-")
    val yearStart = startTimeArr(0).toInt
    val monthStart = startTimeArr(1).toInt
    val dayStart = startTimeArr(2).toInt
    val hourStart = startTimeArr(3).toInt
    val yearEnd = endTimeArr(0).toInt
    val monthEnd = endTimeArr(1).toInt
    val dayEnd = endTimeArr(2).toInt
    val hourEnd = endTimeArr(3).toInt
    val inputPathArr: ArrayBuffer[String] = ArrayBuffer[String]()
    if (yearStart + 1 <= yearEnd) {
      if (yearStart + 2 <= yearEnd) {
        val startYearHead = yearStart / 10
        val endYearHead = yearStart / 10
        if (startYearHead + 1 < endYearHead) {
          for (yearHead <- (startYearHead + 1) to (endYearHead - 1)) {
            inputPathArr += yearHead + "?-*-*/*/*.gz"
          }
        } else if (startYearHead + 1 == endYearHead) {
          if (yearStart % 10 < 9) {
            inputPathArr += startYearHead + "[" + (yearStart % 10 + 1) + "-9]-*-*/*/*.gz"
          }
          if (yearEnd % 10 > 0) {
            inputPathArr += endYearHead + "[0-" + (yearEnd % 10 - 1) + "]-*-*/*/*.gz"
          }
        } else if (startYearHead == endYearHead) {
          inputPathArr += startYearHead + "[" + (yearStart % 10 + 1) + "-" + (yearEnd % 10 - 1) + "]" + "-*-*/*/*.gz"
        }
      }
      inputPathArr ++= monStart(monthStart, yearStart)
      inputPathArr ++= monEnd(monthEnd, yearEnd)
      inputPathArr ++= dStart(dayStart, monthStart, yearStart, monLi)
      inputPathArr ++= dEnd(dayEnd, monthEnd, yearEnd)
      inputPathArr ++= hStart(hourStart, dayStart, monthStart, yearStart)
      inputPathArr ++= hEnd(hourEnd, dayEnd, monthEnd, yearEnd)
    } else if (yearStart == yearEnd) {
      if (monthStart + 1 <= monthEnd) {
        if (monthStart + 2 <= monthEnd) {
          if (monthStart < 9) {
            if (monthEnd <= 10) {
              inputPathArr += yearStart + "-0[" + (monthStart + 1) + "-" + (monthEnd - 1) + "]-*/*/*.gz"
            } else {
              inputPathArr += yearStart + "-0[" + (monthStart + 1) + "-9]-*/*/*.gz"
              inputPathArr += yearStart + "-1[0-" + (monthEnd % 10 - 1) + "]-*/*/*.gz"
            }
          } else {
            inputPathArr += yearStart + "-" + (monthStart / 10) + "[" + (monthStart % 10 + 1) + "-" + (monthEnd % 10 - 1) + "]-*/*/*.gz"
          }
        }
        inputPathArr ++= dStart(dayStart, monthStart, yearStart, monLi)
        inputPathArr ++= dEnd(dayEnd, monthEnd, yearEnd)
        inputPathArr ++= hStart(hourStart, dayStart, monthStart, yearStart)
        inputPathArr ++= hEnd(hourEnd, dayEnd, monthEnd, yearEnd)
      } else if (monthStart == monthEnd) {
        if (dayStart + 1 <= dayEnd) {
          if (dayStart + 2 <= dayEnd) {
            if (dayStart / 10 + 1 < dayEnd / 10) {
              for (dayHead <- (dayStart / 10 + 1) to (dayEnd / 10 - 1)) {
                inputPathArr += yearStart + "-" + monthStart + "-" + dayHead + "?]/*/*.gz"
              }
            } else if (dayStart / 10 + 1 == dayEnd / 10) {
              if (dayStart % 10 < 9) {
                inputPathArr += yearStart + "-" + monthStart + "-" + dayStart / 10 + "[" + (dayStart % 10 + 1) + "-9]/*/*.gz"
              }
              if (dayStart % 10 > 0) {
                inputPathArr += yearStart + "-" + monthStart + "-" + dayEnd / 10 + "[0-" + (dayEnd % 10 - 1) + "]/*/*.gz"
              }
            } else if (dayStart / 10 == dayEnd / 10) {
              inputPathArr += yearStart + "-" + monthStart + "-" + dayStart / 10 + "[" + (dayStart % 10 + 1) + "-" + (dayEnd % 10 - 1) + "]/*/*.gz"
            }
          }
          inputPathArr ++= hStart(hourStart, dayStart, monthStart, yearStart)
          inputPathArr ++= hEnd(hourEnd, dayEnd, monthEnd, yearEnd)
        } else if (dayStart == dayEnd) {
          if (hourStart < hourEnd) {
            val hourStartHead = hourStart / 10
            val hourEndHead = hourEnd / 10
            if (hourStartHead + 1 < hourEndHead) {
              for (hourHead <- hourEndHead + 1 to hourEndHead - 1) {
                inputPathArr += yearStart + "-" + monthStart + "-" + dayStart + "/" + hourHead + "?]/*.gz"
              }
            } else if (hourStartHead + 1 == hourEndHead) {
              inputPathArr += yearStart + "-" + monthStart + "-" + dayStart + "/" + hourStartHead + "[" + hourStart % 10 + "-9]/*.gz"
              inputPathArr += yearStart + "-" + monthStart + "-" + dayStart + "/" + hourEndHead + "[0-" + hourEnd % 10 + "]/*.gz"
            } else if (hourStartHead == hourEndHead) {
              inputPathArr += yearStart + "-" + monthStart + "-" + dayStart + "/" + hourStartHead + "[" + hourStart % 10 + "-" + hourEnd % 10 + "]/*.gz"
            }
          }
        }
      }
    }
    //    println("start_time" + config.start_time.toString)
    //    println("end_time" + config.end_time.toString)
    //    //    sc.parallelize(config.toString).saveAsTextFile(outputPath+"/inputArgs")
    //    //    sc.parallelize(inputPathArr).saveAsTextFile(outputPath+"/outputPathArr")
    //    inputPathArr.foreach(println)
    inputPathArr
  }


  def monStart(monthStart: Int, yearStart: Int): ArrayBuffer[String] = {
    val inputPathArr: ArrayBuffer[String] = ArrayBuffer[String]()
    if (monthStart < 9) {
      inputPathArr += yearStart + "-0[" + (monthStart + 1) + "-9]-*/*/*.gz"
      inputPathArr += yearStart + "-1?-*/*/*.gz"
    } else if (monthStart < 12) {
      inputPathArr += yearStart + "-1[" + ((monthStart + 1) % 10) + "-2]-*/*/*.gz"
    }
    inputPathArr
  }

  def monEnd(monthEnd: Int, yearEnd: Int): ArrayBuffer[String] = {
    val inputPathArr: ArrayBuffer[String] = ArrayBuffer[String]()

    if (monthEnd >= 2 && monthEnd <= 10) {
      inputPathArr += yearEnd + "-0[1-" + (monthEnd - 1) + "]-*/*/*.gz"
    } else if (monthEnd <= 12) {
      inputPathArr += yearEnd + "-0?-*/*/*.gz"
      inputPathArr += yearEnd + "-1[0-" + (monthEnd % 10 - 1) + "]-*/*/*.gz"
    }
    inputPathArr
  }

  def dStart(dayStart: Int, monthStart: Int, yearStart: Int, monLi: List[Int]): ArrayBuffer[String] = {
    val inputPathArr: ArrayBuffer[String] = ArrayBuffer[String]()
    val endOfStartMonth = if (monthStart == 2 && yearStart % 4 == 0) (monLi(monthStart - 1) + 1) % 10 else (monLi(monthStart - 1)) % 10
    val monthStartPrefix = if (monthStart <= 9) "-0" else "-"
    val flag = if (monthStart == 2) 1 else 0

    val dayStartHead = dayStart / 10
    val dayStartTail = dayStart % 10

    if (dayStartHead == 3 && dayStartTail < endOfStartMonth) {
      inputPathArr += yearStart + monthStartPrefix + monthStart + "-3[" + (dayStartTail + 1) + "-" + endOfStartMonth + "]/*/*.gz"
    } else if (dayStartHead == 2) {
      if (flag == 1 && dayStartTail < endOfStartMonth) {
        inputPathArr += yearStart + monthStartPrefix + monthStart + "-2[" + (dayStartTail + 1) + "-" + endOfStartMonth + "]/*/*.gz"
      } else if (flag == 0) {
        if (dayStartTail < 9) {
          inputPathArr += yearStart + monthStartPrefix + monthStart + "-2[" + (dayStartTail + 1) + "-9]/*/*.gz"
        }
        inputPathArr += yearStart + monthStartPrefix + monthStart + "-3?/*/*.gz"
      }
    } else if (dayStartHead == 1) {
      if (dayStartTail < 9) {
        inputPathArr += yearStart + monthStartPrefix + monthStart + "-1[" + (dayStartTail + 1) + "-9]/*/*.gz"
      }
      inputPathArr += yearStart + monthStartPrefix + monthStart + "-2?/*/*.gz"
      if (flag == 0) {
        inputPathArr += yearStart + monthStartPrefix + monthStart + "-3?/*/*.gz"
      }
    } else if (dayStartHead == 0) {
      if (dayStartTail < 9) {
        inputPathArr += yearStart + monthStartPrefix + monthStart + "-0[" + (dayStartTail + 1) + "-9]/*/*.gz"
      }
      inputPathArr += yearStart + monthStartPrefix + monthStart + "-1?/*/*.gz"
      inputPathArr += yearStart + monthStartPrefix + monthStart + "-2?/*/*.gz"
      if (flag == 0) {
        inputPathArr += yearStart + monthStartPrefix + monthStart + "-3?/*/*.gz"
      }
    }
    inputPathArr
  }

  def dEnd(dayEnd: Int, monthEnd: Int, yearEnd: Int): ArrayBuffer[String] = {
    val inputPathArr: ArrayBuffer[String] = ArrayBuffer[String]()

    val monthEndPrefix = if (monthEnd <= 9) "-0" else "-"
    val dayEndHead = dayEnd / 10
    val dayEndTail = dayEnd % 10

    if (dayEndHead == 0 && dayEndTail >= 2) {
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-0[1-" + (dayEndTail - 1) + "]/*/*.gz"
    } else if (dayEndHead == 1) {
      if (dayEndTail >= 1) {
        inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-1[0-" + (dayEndTail - 1) + "]/*/*.gz"
      }
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-0?/*/*.gz"
    } else if (dayEndHead == 2) {
      if (dayEndTail >= 1) {
        inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-2[0-" + (dayEndTail - 1) + "]/*/*.gz"
      }
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-0?/*/*.gz"
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-1?/*/*.gz"
    } else if (dayEndHead == 3) {
      if (dayEndTail >= 1) {
        inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-3[0-" + (dayEndTail - 1) + "]/*/*.gz"
      }
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-0?/*/*.gz"
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-1?/*/*.gz"
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + "-2?/*/*.gz"
    }
    inputPathArr
  }

  def hStart(hourStart: Int, dayStart: Int, monthStart: Int, yearStart: Int): ArrayBuffer[String] = {
    val inputPathArr: ArrayBuffer[String] = ArrayBuffer[String]()
    val monthStartPrefix = if (monthStart <= 9) "-0" else "-"
    val dayStartPrefix = if (dayStart <= 9) "-0" else "-"
    val hourStartHead = hourStart / 10
    val hourStartTail = hourStart % 10
    if (hourStartHead == 2) {
      inputPathArr += yearStart + monthStartPrefix + monthStart + dayStartPrefix + dayStart + "/2[" + hourStartTail + "-3]/*.gz"
    } else if (hourStartHead == 1) {
      inputPathArr += yearStart + monthStartPrefix + monthStart + dayStartPrefix + dayStart + "/2[0-3]/*.gz"
      inputPathArr += yearStart + monthStartPrefix + monthStart + dayStartPrefix + dayStart + "/1[" + hourStartTail + "-9]/*.gz"
    } else if (hourStartHead == 0) {
      inputPathArr += yearStart + monthStartPrefix + monthStart + dayStartPrefix + dayStart + "/2?/*.gz"
      inputPathArr += yearStart + monthStartPrefix + monthStart + dayStartPrefix + dayStart + "/1?/*.gz"
      inputPathArr += yearStart + monthStartPrefix + monthStart + dayStartPrefix + dayStart + "/0[" + hourStartTail + "-9]/*.gz"
    }
    inputPathArr
  }

  def hEnd(hourEnd: Int, dayEnd: Int, monthEnd: Int, yearEnd: Int): ArrayBuffer[String] = {
    val inputPathArr: ArrayBuffer[String] = ArrayBuffer[String]()
    val monthEndPrefix = if (monthEnd <= 9) "-0" else "-"
    val dayEndPrefix = if (dayEnd <= 9) "-0" else "-"
    val hourEndHead = hourEnd / 10
    val hourEndTail = hourEnd % 10
    if (hourEndHead == 0) {
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + dayEndPrefix + dayEnd + "/0[0-" + hourEndTail + "]/*.gz"
    } else if (hourEndHead == 1) {
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + dayEndPrefix + dayEnd + "/0?/*.gz"
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + dayEndPrefix + dayEnd + "/1[0-" + hourEndTail + "]/*.gz"
    } else if (hourEndHead == 2) {
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + dayEndPrefix + dayEnd + "/0?/*.gz"
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + dayEndPrefix + dayEnd + "/1?/*.gz"
      inputPathArr += yearEnd + monthEndPrefix + monthEnd + dayEndPrefix + dayEnd + "/2[0-" + hourEndTail + "]/*.gz"
    }
    inputPathArr
  }

}
