package com.epam.songwordcounterborisov.service

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.springframework.stereotype.Service

@Service
class TopWordsService(val garbage: Broadcast[java.util.List[String]]) extends Serializable {

  def topXWithSQL(sqlcode: SQLContext): Unit ={
    val dataFrame = sqlcode.read.json("src/main/songs/names.json")
//     dataFrame.registerTempTable("q")
//    sqlcode.

  }

  def topX(lines:RDD[String], x:Int): List[String] ={


   lines.flatMap(_.split("[^a-zA-Z]+"))
     .map(_.toLowerCase())
     .filter(!this.garbage.value.contains(_))
     .groupBy(identity)
     .mapValues(v=>Integer.valueOf(v.count(_=>true)))
     .sortBy(_._2, ascending = false)
     .take(x)
     .map(_._1)
     .toList
  }

}
