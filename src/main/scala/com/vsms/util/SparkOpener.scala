package com.vsms.util

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkOpener
{

  def SparkSessionLoc(name:String):SparkSession={
    val conf=new SparkConf().setAppName(name +"Local" ).setMaster("local")
    conf.set("spark.testing.memory","571859200").set("spark.ui.enabled","true").set("spark.driver.host","localhost").set("spark.sql.parquet.binaryAsString","true").set("spark.sql.avro.binaryAsString","true")
    System.setProperty("hadoop.home.dir","C:\\HiveJar\\")
   /*Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)*/
    SparkSession.builder().config(conf).getOrCreate()
  }

}
