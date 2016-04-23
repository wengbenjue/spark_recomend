package com.soledede.cf.copy

import java.io.{FileInputStream, ObjectInputStream}

import scala.io.Source


/**
 * Created by soledede on 2014/9/11.
 */
object ReadModel {
  var model_path = "D:\\movielen\\model.bin"
  //var model_path = "home/hadoop/mllib/model/model.bin";
  var item_path = "D:\\movielen\\movies.dat"
  //var item_path = "/home/hadoop/mllib/movielen/movies.dat";
  var rating_path = "D:\\movielen\\ratings.dat"
  // var rating_path ="/home/hadoop/mllib/movielen/ratings.dat";

  def main(args: Array[String]) {
    println("model read......")



    var m: org.apache.spark.mllib.recommendation.MatrixFactorizationModel = null
    val fos1 = new FileInputStream(model_path)
    val oos1 = new ObjectInputStream(fos1)
    val newModel = oos1.readObject().asInstanceOf[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]
    m = newModel // newModel is the loaded one, see above post of mine
    oos1.close

    val movieids = Source.fromFile(item_path).getLines().map { line =>
      val fields = line.split("::")
      (fields(0).toInt)
    }

    //movieids.foreach(println)

    //userid movieid
    val ratings = Source.fromFile(rating_path).getLines().map { line =>
      val fields = line.split("::")
      (fields(0).toInt, fields(1).toInt)
    }
    val user_items = ratings.filter(line => line._1 == 1)



    /*   user_items.foreach(e => {
         val (k,v) = e
         println(k+":"+v)
       }
       )*/

    val conf = new org.apache.spark.SparkConf()

    val sc = new org.apache.spark.SparkContext(conf)


    val myRatedItemids = user_items.map(_._2.toInt).toSet

    // myRatedItemids.foreach(println)

    val shoudPredicateItemsRDD = sc.parallelize(movieids.filter(!myRatedItemids.contains(_)).toSeq)
    //val shoudPredicateItems = movieids.filter(!myRatedItemids.contains(_))

    // shoudPredicateItemsRDD.foreach(println)
    println("started predict....................")
    val recommendations = m.predict(shoudPredicateItemsRDD.map((1, _))).collect.sortBy(_.rating).take(50)
    //val recommendations = m.predict(shoudPredicateItems.map((1,_))).collect.sortBy(_.rating).take(50)
    //recomends.......
    println("recomendings......")
    recommendations.foreach { r =>
      println("推荐餐厅：" + r.product)
    }

    sc.stop()


    //ratings.get(1).foreach()


    //val predictions: RDD[Rating] = m.predict()

  }

}
