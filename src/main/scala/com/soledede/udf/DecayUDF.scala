package com.soledede.udf

import org.apache.hadoop.hive.ql.exec.UDF

/**
 * Created by soledede on 15/11/22.
 */
class DecayUDF extends UDF{


  def evaluate(x: Double): Double = evaluate(3.6f, x, 3*60*60*1000f, 1f)

  def evaluate(m: Float, x: Double, a: Float, b: Float): Double = {
    if (x == null) return 0
    var m1 = 3.16f
    var a1:Float = 1
    var b1:Float = 1
    if (m != null) m1 = m
    if (a != null) a1 = a
    if (b != null) b1= b

    val r = f"${a1 / (m1 * x + b1)}%1.2f".toDouble
    if (r >= 3) 3
    else if (r <= 0) 0
    else r
  }
}


object DecayUDF {
  def main(args: Array[String]) {
    println(new DecayUDF().evaluate(4*60*60*1000))
  }
}