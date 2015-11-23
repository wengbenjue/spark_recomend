package com.soledede.cf.copy

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONArray

/**
 * Created by soledede on 2014/9/15.
 */
object JsonTest {

  def main(args: Array[String]) {

    var listBuffer = ListBuffer[String]()
    listBuffer += "dsf"
    listBuffer += "dsff"
    listBuffer += "dsf"
    listBuffer += "sdfdsf"

    val jsonArray = new JSONArray(listBuffer.toList)
    println(jsonArray)
  }

}
