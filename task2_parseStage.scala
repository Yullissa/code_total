/**
  * Created by admin on 2018/3/13.
  */
package com.yidian.parseStaggingData

import org.apache.spark.{SparkConf, SparkContext}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import scala.collection.mutable.ArrayBuffer

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
}

class ParseStageData extends Serializable {
  def run(sparkConf: SparkConf, inputdata: String, output_data: String): Int = {
    val sc = new SparkContext(sparkConf)
    val viewRdd = sc.textFile(inputdata)
    val apps = List("apple", "oppo", "xiaomi")
    val browsers = List("oppobrowser", "mibrowser")
    var platform = "None"
    try {
      val viewId = viewRdd.flatMap { line =>
        val userid = JSON.parseObject(line).get("userid").toString
        val appid = JSON.parseObject(line).get("appid").toString
        if (apps.contains(appid)) {
          platform = "app_" + appid
        }
        else if (browsers.contains(appid)) {
          platform = appid
        }
        else {
          platform = "other_" + appid
        }
//        println(userid)
//        println(appid)
//        println(platform)
        val resArray = new ArrayBuffer[String];
        if (JSON.parseObject(line).containsKey("views")) {
          for (ele <- parseViews(line, platform)) {
            resArray += ele
          }
        }
        else if (JSON.parseObject(line).containsKey("click")) {
          for (ele <- parseClick(line, platform)) {
            resArray += ele
          }
        }
        resArray
      }.saveAsTextFile(output_data)
    }
    catch {
      case ex: Throwable => println(ex)
    }
    0
  }

  def parseViews(line: String, platform: String): ArrayBuffer[String] = {
    val mutableArr = new ArrayBuffer[String];
    try {
      val userid = JSON.parseObject(line).get("userid")
      val jsonObject = JSON.parseObject(line).getJSONArray("views").iterator()
      while (jsonObject.hasNext) {
        println(jsonObject.next())
        val temp = jsonObject.next()
        val eachDoc = JSON.toJSONString(temp, SerializerFeature.PrettyFormat)
        val docid = JSON.parseObject(eachDoc).get("docid")
        val date = JSON.parseObject(eachDoc).get("date")
        val e = "vw"
        val outputData = userid + "\t" + docid + "\t" + e + "\t" + date + "\t" + platform
        mutableArr += outputData
      }
      return mutableArr
    } catch {
      case ex: Throwable => {
        mutableArr += ex.toString
        return mutableArr
      }
    }
  }


  def parseClick(line: String, platform: String): ArrayBuffer[String] = {
    val mutableArr = new ArrayBuffer[String];
    try {
      val userid = JSON.parseObject(line).get("userid")
      val clickData = JSON.parseObject(line).getJSONObject("click")
      var event = clickData.get("event")
      val date = clickData.get("date")
      val details = clickData.getJSONObject("details")
      val docid = clickData.get("docid").toString()
      if (docid.contains("_")) {
        0
      }
      var e = "None"
      if (event == "clickDoc") {
        var dewell = 0.0
        if (details != "None") {
          dewell = details.get("dwell").toString.toFloat
        }
        if (dewell < 8 && dewell > 0) {
          0
        }
        e = "clk"
      }
      else if (event == "viewComment") {
        e = "vc"
      }
      else if (event == "shareDoc") {
        e = "shr"
      }
      else if (event == "like") {
        e = "lk"
      }
      else if (event == "dislike") {
        e = "dlk"
      }
      else if (event == "addComment") {
        e = "ac"
      }
      if (!e.isEmpty()) {
        val outputData = userid + "\t" + docid + "\t" + e + "\t" + date + "\t" + platform
        println(outputData)
        mutableArr += outputData
      }
      return mutableArr
    } catch {
      case ex: Throwable => {
        mutableArr += ex.toString
        return mutableArr
      }
    }
  }
}