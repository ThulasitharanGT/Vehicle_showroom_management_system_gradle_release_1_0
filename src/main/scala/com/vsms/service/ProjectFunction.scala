package com.vsms.service

import com.vsms.controller.TotalFunction.spark
import org.apache.spark.sql.DataFrame

package object ProjectFunction {

    def Retrival_info_fun(scn_no: Int ,Query_data: DataFrame, ArgMap : collection.mutable.Map[String,String]) : DataFrame = {

      var Scn_df:DataFrame=null
      val SqlString:String =try {Query_data.filter("SCN_NO=" + scn_no).select("QUERY").rdd.map(r => r(0)).collect().mkString(",")}
      println(SqlString)
      var SqlString_final:String = null

      if (scn_no == 1) {
        SqlString_final = SqlString.replace("%t1%", ArgMap("%t1%").toString).replace("%where_condition%", ArgMap("%where_condition%").toString).replace("*", ArgMap("*").toString)
        println(SqlString_final)
        Scn_df = spark.sql(SqlString_final.toString())
        Scn_df
      }
      else if (scn_no == 2) {
        SqlString_final = SqlString.replace("%t1%", ArgMap("%t1%").toString).replace("%t2%", ArgMap("%t2%").toString).replace("%join%", ArgMap("%join%").toString).replace("%join_condition%", ArgMap("%join_condition%").toString).replace("*", ArgMap("*").toString)
        println(SqlString_final)
        Scn_df = spark.sql(SqlString_final.toString())
        Scn_df
      }
      else if (scn_no == 3) {
        SqlString_final = SqlString.replace("*", ArgMap("*").toString).replace("%t1%", ArgMap("%t1%").toString).replace("%t2%", ArgMap("%t2%").toString).replace("%join%", ArgMap("%join%").toString).replace("%join_condition%", ArgMap("%join_condition%").toString).replace("%where_condition%", ArgMap("%where_condition%").toString)
        println(SqlString_final)
        Scn_df = spark.sql(SqlString_final.toString())
        Scn_df
      }
      else
      {
        Scn_df= spark.createDataFrame(Seq(("Empty_DF", "Invalid SCN Number"))).toDF("Error", "ERROR_MESSAGE")
        Scn_df
      }
    }

}
