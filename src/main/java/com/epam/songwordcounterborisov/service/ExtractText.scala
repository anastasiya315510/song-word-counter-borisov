package com.epam.songwordcounterborisov.service

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.springframework.stereotype.Service

@Service
class ExtractText(val sc: SparkContext) {
  def readLinesFromFile(path:String):RDD[String]={
   sc.textFile(path)
  }

}
