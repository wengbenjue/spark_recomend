package com.soledede.cf.copy

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object MLCF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleCFs Application")
    val sc = new SparkContext(conf)
    val data = sc.textFile("mllib/ratings.dat")

    val ratings = data.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val rank = 10
    val numIterations = 5
    println("Starting ......... ")
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
  }
}

