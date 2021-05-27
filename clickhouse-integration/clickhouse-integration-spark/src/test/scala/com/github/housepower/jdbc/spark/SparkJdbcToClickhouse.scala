package com.github.housepower.jdbc.spark

import java.sql.SQLFeatureNotSupportedException

import jodd.util.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.jdbc.{ClickHouseDialect, JdbcDialects}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkJdbcToClickhouse {

//  JdbcDialects.registerDialect(ClickHouseDialect)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkJdbcToClickhouse").setMaster("local[1]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val dataFilePath = "D:\\Spark_Ws\\spark-apache\\examples\\src\\main\\resources\\people.csv"

    val fileName = "D:\\Spark_Ws\\spark-apache\\examples\\src\\main\\resources\\application.properties"
    val props = PropertiesUtil.createFromFile(fileName)

    val ckDriver =  props.getProperty("ck.driver") //"com.github.housepower.jdbc.ClickHouseDriver"
    val ckUrl = props.getProperty("ck.url")
    val ckUserName = props.getProperty("ck.username")
    val ckPassword = props.getProperty("ck.password")
    val table = "jdbc_test_table"

    // 读取people.csv测试文件内容
    val peopleDFCsv =
      spark.read
        .format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(dataFilePath)

    peopleDFCsv.show()

    try {
      val pro = new java.util.Properties
      pro.put("driver", ckDriver)
      pro.put("user", ckUserName)
      pro.put("password", ckPassword)

      peopleDFCsv.write.mode(SaveMode.Append)
        .option("batchsize", "20000")
        .option("isolationLevel", "NONE")
        .option("numPartitions", "2")
        .jdbc(ckUrl, table, pro)


//      peopleDFCsv
//        .write
//        .format("jdbc")
//        .mode("overwrite")
//        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
//        .option("url", ckUrl)
//        .option("user", ckUserName)
//        .option("password", ckPassword)
//        .option("dbtable", table)
//        .option("truncate", "true")
//        .option("batchsize", 1000)
//        .option("isolationLevel", "NONE")
//        .save

    } catch {
      // 这里注意下，spark里面JDBC datasource用到的一些获取元数据的方法插件里并没有支持，比如getPrecision & setQueryTimeout等等，都会抛出异常，但是并不影响写入
      case e: SQLFeatureNotSupportedException =>
        println("catch and ignore!")
    }
    spark.close()
  }
}

/**
  * 在上面抛异常的地方卡了很久，Spark在JDBC读取的时候，会先尝试获取目标表的schema元数据，调用一些方法，
  * 比如resolvedTable时会getSchema：
  *
  */
