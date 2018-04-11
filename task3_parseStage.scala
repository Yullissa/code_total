/**
  * Created by admin on 2018/3/13.
  */
package com.yidian.parseStaggingData

import org.apache.spark.{SparkConf, SparkContext}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import scala.collection.mutable._
import scala.util.control.Breaks._

object parseStage {

  //   Parse parameters
  case class Config(input_data: String = "",
                    output_data: String = "",
                    task: String = "") extends Serializable

  @transient val CONFIG_PARSER = new scopt.OptionParser[Config]("VideoInfoDumper") {
    head("generate clusterset2news trainning data")
    opt[String]('i', "input_data").required().valueName("<hdfs path>").action((x, c) =>
      c.copy(input_data = x)).text("input hdfs path")
    opt[String]('o', "output_data").required().valueName("<hdfs path>").action((x, c) =>
      c.copy(output_data = x)).text("output hdfs path")
    opt[String]('t', "task").required().valueName("<task>").action((x, c) =>
      c.copy(task = x)).text("dump info task")
  }

  def main(args: Array[String]) {
    CONFIG_PARSER.parse(args, Config()) match {
      case Some(config) =>
        val sparkConf = new SparkConf().setAppName("doc_cluster")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.kryoserializer.buffer.max", "1024m")
        val ret = new ParseStageData().run(sparkConf, config.input_data, config.output_data)
      case _ =>
    }
  }

  //  def main(args: Array[String]) {
  //    val sparkConf = new SparkConf().setAppName("testdocid").setMaster("local")
  //    val input_data = "/Users/admin/IdeaProjects/Parser/2018-3-*"
  //    val output_data = "/Users/admin/IdeaProjects/Parser/output"
  //    new ParseStageData().run(sparkConf, input_data, output_data)
  //  }
}

class ParseStageData extends Serializable {
  def run(sparkConf: SparkConf, inputdata: String, output_data: String): Unit = {
    val sc = new SparkContext(sparkConf)
    val viewRdd = sc.textFile(inputdata)
    try {
      val aimUsers = viewRdd.map { line =>
        var tuple = ("None", 0L)
        if (JSON.parseObject(line).containsKey("userid")) {
          val user = JSON.parseObject(line).get("userid").toString
          if (JSON.parseObject(line).containsKey("click")) {
            val clickData = JSON.parseObject(line).getJSONObject("click")
            if (clickData.containsKey("event") && clickData.containsKey("details")) {
              var event = clickData.get("event").toString
              val details = clickData.getJSONObject("details")
              var e = "None"
              if (event == "clickDoc") {
                var dewell = 0.0
                if (details != "None") {
                  if (details.containsKey("dwell")) {
                    dewell = details.get("dwell").toString.toFloat
                  }
                  if (dewell < 8 && dewell > 0) {
                    0
                  }
                }
                e = "clk"
                tuple = (user, 1L)
              }
            }
          }
        }
        tuple
      }.reduceByKey((a, b) => a + b).filter(x => x._2 >= 20).collect().toMap
      aimUsers.foreach(x => println(x._1 + "\t" + x._2))

      val bmap = sc.broadcast(aimUsers)

      val viewlst = viewRdd.map { line =>
        var tuple = ("None", "None", "None", "None")
        if (JSON.parseObject(line).containsKey("userid")) {
          val userid = JSON.parseObject(line).get("userid").toString
          if (JSON.parseObject(line).containsKey("click")) {
            val clickData = JSON.parseObject(line).getJSONObject("click")
            if (clickData.containsKey("event") && clickData.containsKey("date") && clickData.containsKey("docid") && clickData.containsKey("details")) {
              var event = clickData.get("event").toString
              val date = clickData.get("date").toString
              val docid = clickData.get("docid").toString
              val details = clickData.getJSONObject("details")
              var e = "None"
              if (event == "clickDoc") {
                var dewell = 0.0
                if (details != "None") {
                  if (details.containsKey("dwell")) {
                    dewell = details.get("dwell").toString.toFloat
                  }
                  if (dewell < 8 && dewell > 0) {
                    0
                  }
                }
                e = "clk"
                tuple = (userid, e, date, docid)
              }
            }
          }
        }
        tuple
      }.filter(x => bmap.value.contains(x._1) && x._2 == "clk").groupBy(_._1).map { line =>
        var arr = line._2.toArray
        val mp = Set[String]()
        val lst = ArrayBuffer[String]()
        breakable(
          arr.sortWith(_._3 > _._3).foreach { x =>
            val docid = x._4
            if (!mp.contains(docid)) {
              mp += docid
              lst += docid
              lst += x._3
              if (lst.size >= 40) break;
            }
          }
        )
        (line._1, lst)
      }
      viewlst.map(x => x._1 + "\t" + x._2.mkString(" ")).repartition(1).saveAsTextFile(output_data)
    }
    catch {
      case ex: Throwable => println(ex)
    }
    0
  }


  //  def parseViews(line: String, platform: String): ArrayBuffer[String] = {
  //    val mutableArr = new ArrayBuffer[String];
  //    try {
  //      val userid = JSON.parseObject(line).get("userid")
  //      val jsonObject = JSON.parseObject(line).getJSONArray("views").iterator()
  //      while (jsonObject.hasNext) {
  //        println(jsonObject.next())
  //        val temp = jsonObject.next()
  //        val eachDoc = JSON.toJSONString(temp, SerializerFeature.PrettyFormat)
  //        val docid = JSON.parseObject(eachDoc).get("docid")
  //        val date = JSON.parseObject(eachDoc).get("date")
  //        val e = "vw"
  //        val outputData = userid + "\t" + docid + "\t" + e + "\t" + date + "\t" + platform
  //        mutableArr += outputData
  //      }
  //      return mutableArr
  //    } catch {
  //      case ex: Throwable => {
  //        mutableArr += ex.toString
  //        return mutableArr
  //      }
  //    }
  //  }
  //
  //
  //  def parseClick(line: String, platform: String): ArrayBuffer[String] = {
  //    val mutableArr = new ArrayBuffer[String];
  //    try {
  //      val userid = JSON.parseObject(line).get("userid")
  //      val clickData = JSON.parseObject(line).getJSONObject("click")
  //      var event = clickData.get("event")
  //      val date = clickData.get("date")
  //      val details = clickData.getJSONObject("details")
  //      val docid = clickData.get("docid").toString()
  //      if (docid.contains("_")) {
  //        0
  //      }
  //      var e = "None"
  //      if (event == "clickDoc") {
  //        var dewell = 0.0
  //        if (details != "None") {
  //          dewell = details.get("dwell").toString.toFloat
  //        }
  //        if (dewell < 8 && dewell > 0) {
  //          0
  //        }
  //        e = "clk"
  //      }
  //      else if (event == "viewComment") {
  //        e = "vc"
  //      }
  //      else if (event == "shareDoc") {
  //        e = "shr"
  //      }
  //      else if (event == "like") {
  //        e = "lk"
  //      }
  //      else if (event == "dislike") {
  //        e = "dlk"
  //      }
  //      else if (event == "addComment") {
  //        e = "ac"
  //      }
  //      if (!e.isEmpty()) {
  //        val outputData = userid + "\t" + docid + "\t" + e + "\t" + date + "\t" + platform
  //        println(outputData)
  //        mutableArr += outputData
  //      }
  //      return mutableArr
  //    } catch {
  //      case ex: Throwable => {
  //        mutableArr += ex.toString
  //        return mutableArr
  //      }
  //    }
  //  }
}