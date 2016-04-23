package com.soledede.cf.copy

import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer

/**
 * Created by soledede on 2014/9/15.
 */
object ByteConvertTest {
  def main(args: Array[String]) {
    val outputStream = new java.io.ByteArrayOutputStream()
    val bf = ByteBuffer.allocate(4096)
    val ob = new ObjectOutputStream(outputStream)
    ob.writeObject(List(1, 3, 4));
    outputStream.toByteArray()
    bf.put(outputStream.toByteArray())
    println(bf)
  }

}
