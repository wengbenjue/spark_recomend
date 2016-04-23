package com.soledede.cf.copy

import java.io.{FileInputStream, ObjectInputStream}

/**
 * Created by soledede on 2014/9/12.
 */
object testReadObj {

  var model_path = "D:\\movielen\\model.bin"


  def main(args: Array[String]) {


    var m: org.apache.spark.mllib.recommendation.MatrixFactorizationModel = null
    val fos1 = new FileInputStream(model_path)
    val oos1 = new ObjectInputStream(fos1)
    val newModel = oos1.readObject().asInstanceOf[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]
    //val newModel = oos1.readObject()
    m = newModel // newModel is the loaded one, see above post of mine
    oos1.close
  }
}
