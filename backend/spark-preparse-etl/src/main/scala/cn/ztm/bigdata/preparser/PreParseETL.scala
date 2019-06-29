package cn.ztm.bigdata.preparser

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object PreParseETL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PreParseETL")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val rawdataInputPath = spark.conf.get("spark.traffic.analysis.rawdata.input",
      "hdfs://master:9999/user/hadoop-ztm/data/traffic-analysis/rawlog/20180615")
    val numberPartitions = spark.conf.get("spark.traffic.analysis.rawdata.numberPartitions", "2").toInt

    val preParsedLogRdd = spark.sparkContext.textFile(rawdataInputPath).flatMap(
      line => Option(WebLogPreParser.parse(line)) // 解析的时候会返回null，加上Option就可以避免这个情况，另外用flatMap取出里面的值
    )

    val preParsedLogDS = spark.createDataset(preParsedLogRdd)(Encoders.bean(classOf[PreParsedLog]))
    preParsedLogDS.coalesce(numberPartitions) //todo 什么场景下使用coalesce
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day") // todo 这个地方的partitionBy是不是就是hive里的partitionBy
      .saveAsTable("rawdata.web")

    // 关闭会话
    spark.stop()
  }

}
