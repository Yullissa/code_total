package com.yidian.collectTrainData

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.Partitioner
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.sys.process._
import org.apache.log4j.{LogManager, Logger}

import scala.util.Random

/**
  * Created by admin on 2018/3/24.
  */
object collectTrainData {
  val LOG = LoggerFactory.getLogger(classOf[collectTrainData])

  //  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  case class Config(input_data: String = "",
                    output_data: String = "",
                    task: String = "",
                    partNum: String = "") extends Serializable

  @transient val CONFIG_PARSER = new scopt.OptionParser[Config]("VideoInfoDumper") {
    head("generate clusterset2news trainning data")
    opt[String]('i', "input_data").required().valueName("<hdfs path>").action((x, c) =>
      c.copy(input_data = x)).text("input hdfs path")
    opt[String]('o', "output_data").required().valueName("<hdfs path>").action((x, c) =>
      c.copy(output_data = x)).text("output hdfs path")
    opt[String]('t', "task").required().valueName("<task>").action((x, c) =>
      c.copy(task = x)).text("dump info task")
    opt[String]('p', "partNum").required().valueName("<Int>").action((x, c) =>
      c.copy(partNum = x)).text("part Num")
  }

  def main(args: Array[String]) {

    CONFIG_PARSER.parse(args, Config()) match {
      case Some(config) =>
        val sparkConf = new SparkConf().setAppName("traindata_collect")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.kryoserializer.buffer.max", "1024m")
        //本地化时长  提高collect回收数据的容量
        sparkConf.set("spark.locality.wait", "10")
        sparkConf.set("spark.driver.maxResultSize", "3g")
        //        sparkConf.set("spark.dynamicAllocation.enabled", "true")
        //        sparkConf.set("spark.shuffle.service.enabled", "true")
        val ret = new collectTrainData().run(sparkConf, config.input_data, config.output_data, config.partNum.toInt)
      case _ =>
    }
  }
}

class collectTrainData extends Serializable {

  def run(sparkConf: SparkConf, inputData: String, outputData: String, partNum: Int): Unit = {
    import com.yidian.collectTrainData.collectTrainData.LOG
    val clean = "rm -r ./output" !

    val sc = new SparkContext(sparkConf)
    val viewRdd = sc.textFile(inputData)
    val eveValue = Map("vw" -> 0, "clk" -> 1, "ac" -> 2, "shr" -> 3, "lk" -> 3, "dlk" -> 4)
    val eveValueBD = sc.broadcast(eveValue)

    val nowTime: Date = new Date()
    val endTime = nowTime.getTime / 1000 - 86400 * 30
    val startTime = nowTime.getTime / 1000 - 86400 * 5
    val lifeTime = 86400 * 3 // 3 days
    val minStrongNum = 10
    val paramMap: Map[String, Long] = Map("endTime" -> endTime, "startTime" -> startTime, "lifeTime" -> lifeTime, "minStrongNum" -> minStrongNum, "partNum" -> partNum)
    val paramMapBD = sc.broadcast(paramMap)

    //input size:200M(gz)/partition * 200partitoins/day
    // input: userid docid event[vw,clk,ac,lk,shr,dlk] datetime platform
    val viewRdd2 = viewRdd.map { line =>
      try {
        val elements = line.split("\t")
        if (elements.length >= 5 && eveValueBD.value.contains(elements(2))) {
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val date = (format.parse(elements(3)).getTime / 1000).toInt
          if (date >= 1506614400) { // 2017-9-29 00:00:00
            val p: ArrayBuffer[(Int, Int, String, String, Int, Int, ArrayBuffer[(Int, String, String)])] = ArrayBuffer((date, date, elements(1), elements(2), 0, 1, ArrayBuffer((date, elements(1), elements(2)))))
            Option(elements(0), p)
          } else {
            None
          }
        } else {
          None
        }
      } catch {
        case _: Throwable => None
      }
    }.filter(_.isDefined).map(_.get).reduceByKey((a, b) => {
      if (a.length == 0 || b.length == 0) {
        ArrayBuffer()
      } else if (a(0)._7.length + b(0)._7.length > 20000) {
        ArrayBuffer()
      } else {
        var strongNum: Int = a(0)._5 + b(0)._5
        if (eveValueBD.value.contains(a(0)._4)) {
          if (eveValueBD.value(a(0)._4) >= 1 && eveValueBD.value(a(0)._4) <= 3) {
            strongNum += 1
          }
        }
        if (eveValueBD.value.contains(b(0)._4)) {
          if (eveValueBD.value(b(0)._4) >= 1 && eveValueBD.value(b(0)._4) <= 3) {
            strongNum += 1
          }
        }
        a(0)._7 ++= b(0)._7
        ArrayBuffer((math.max(a(0)._1, b(0)._1), math.min(a(0)._2, b(0)._2), "", "", strongNum, a(0)._6 + b(0)._6, a(0)._7))
      }
    }, paramMapBD.value("partNum").toInt).filter(line => {
      //strongTimes>=minStrongNum  && maxTime>=endTime           minTime<=startTime         strongTime*2 <= totalNum         duringTime>=lifeTime
      line._2.length != 0 && line._2(0)._5 >= paramMapBD.value("minStrongNum") && line._2(0)._1 >= paramMapBD.value("endTime") && line._2(0)._2 <= paramMapBD.value("startTime") && line._2(0)._5 * 2 <= line._2(0)._6 && ((line._2(0)._1 - line._2(0)._2) >= paramMapBD.value("lifeTime"))
    }).flatMap { item =>
      //docid 相加
      val userId = item._1
      val sorted_list = item._2(0)._7.sortBy(-_._1)

      var resPartList = ""
      val resultList = ArrayBuffer[String]()
      var weakArr = ArrayBuffer[String]()
      val strongArr = ArrayBuffer[String]()
      val strongNegArr = ArrayBuffer[String]()

      val docMp = Map[String, (String, Int)]()
      var docId = ""
      var event = ""
      //line :lastTime firstTime  docid event strongTimes totalTimes
      var curEventWei = -1
      var oldEventWei = -1

      var pos = 0
      val maxEvent = 1000
      //update weekArr strongArr strongNegArr
      sorted_list.foreach(line => {
        docId = line._2
        event = line._3
        curEventWei = eveValueBD.value(event)
        if (docMp.contains(docId)) {
          oldEventWei = eveValueBD.value(docMp(docId)._1)
          if (curEventWei > oldEventWei) {
            if (oldEventWei == 0) {
              weakArr.update(docMp(docId)._2, "")
            } else if (oldEventWei <= 3 && oldEventWei >= 1) {
              strongArr.update(docMp(docId)._2, "")
            }
            if (curEventWei <= 3 && curEventWei >= 1) {
              strongArr += docId
              docMp.update(docId, (event, strongArr.size - 1))
            } else if (curEventWei == 4) {
              strongNegArr += docId
              docMp.update(docId, (event, strongNegArr.size - 1))
            }
          }
        } else {
          if (curEventWei == 0) {
            weakArr += docId
            docMp.update(docId, (event, weakArr.size - 1))
          } else if (curEventWei >= 1 && curEventWei <= 3) {
            strongArr += docId
            docMp.update(docId, (event, strongArr.size - 1))
          } else if (curEventWei == 4) {
            strongNegArr += docId
            docMp.update(docId, (event, strongNegArr.size - 1))
          }
        }
      }
      )

      strongNegArr ++= weakArr

      strongArr.foreach(x => {
        resPartList = userId.toString
        if ((pos + 2 < strongNegArr.size) && resultList.size < (maxEvent / 4) && pos < strongNegArr.size && x != "") {
          resPartList += "\t"
          resPartList += x
          for (i <- 1 to 3) {
            while (pos < strongNegArr.size && strongNegArr(pos) == "") {
              pos += 1
            }
            if (pos < strongNegArr.size) {
              resPartList += "\t"
              resPartList += strongNegArr(pos)
              pos += 1
            }
          }
          resultList += resPartList
        }
      })
      resultList
    }.mapPartitions(Random.shuffle(_)).coalesce(100, shuffle = true).saveAsTextFile(outputData, classOf[org.apache.hadoop.io.compress.GzipCodec])
    //      .map(x=>(x.tail,x.head)).partitionBy(new HashPartitioner(2)).map(x=>x._2+x._1).saveAsTextFile(outputData)
    //    scala.util.Random.shuffle(viewRdd2).saveAsTextFile(outputData)
    //    , classOf[org.apache.hadoop.io.compress.GzipCodec]
  }
}

//class userPartitioner(partitions: Int) extends Partitioner {
//  require(partitions >= 0)
//  override def numPartitions: Int = partitions
//  override def getPartition(key: Any): Int = {
////    val k = key.asInstanceOf[(String,String,String,String)]
////    println(key.toString.split("\\\t")(0),(scala.util.Random.nextInt() % numPartitions.abs))
//    (scala.util.Random.nextInt() % numPartitions).abs
//    //    var k = 0
//    //    if (key.isInstanceOf[(Long, Long)]) {
//    //      k = (key.toString.split("\\,")(0).split("\\(")(1).toLong.abs % numPartitions).toInt
//    //    }
//  }
//}

//      .repartitionAndSortWithinPartitions(new userPartitioner(6)).saveAsTextFile(outputData)
//
//      .map(x=>(x,1)).partitionBy(new userPartitioner(6)).map(x=>x._1).saveAsTextFile(outputData)
//      coalesce(3,shuffle = true).saveAsTextFile(outputData)
//
//    map(x=>(scala.util.Random.nextInt()%14,x)).