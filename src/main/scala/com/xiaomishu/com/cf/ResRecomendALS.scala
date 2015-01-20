package com.xiaomishu.com.cf

import java.io.{ObjectOutputStream, Serializable}
import java.nio.ByteBuffer
import java.util.UUID.randomUUID

import com.esotericsoftware.kryo.Kryo
import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, Put, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes


import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONArray



/**
 * Created by wengbenjue on 2014/9/10.
 */
object ResRecomendALS extends Serializable {

  class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
    }
  }

  case class Params(
                     input: String = null,
                     kryo: Boolean = false,
                     numIterations: Int = 20,
                     lambda: Double = 1.0,
                     rank: Int = 10,
                     implicitPrefs: Boolean = false,
                     userInput: String = null,
                     itemInput: String = null,
                     recomendNum: Int = 50,
                     separator: String = "\t",
                     userSeprator: String = "\t",
                     itemSeprator: String = "\t",
                     zookeeper_quorum: String = "spark1.xiaomishu.com,spark2.xiaomishu.com,spark4.xiaomishu.com,spark5.xiaomishu.com,spark7.xiaomishu.com")

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ResRecomendALS") {
      head("ResALS: ALS for res recomended.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Int]("recomendNum")
        .text(s"recomendNum, default: ${defaultParams.recomendNum}}")
        .action((x, c) => c.copy(recomendNum = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text(s"use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      opt[String]("separator")
        .text(s"separator of ratings, default: ${defaultParams.separator}")
        .action((x, c) => c.copy(separator = x))
      opt[String]("userSeprator")
        .text(s"separator of users, default: ${defaultParams.userSeprator}")
        .action((x, c) => c.copy(userSeprator = x))
      opt[String]("itemSeprator")
        .text(s"separator of items, default: ${defaultParams.itemSeprator}")
        .action((x, c) => c.copy(itemSeprator = x))
      opt[String]("zookeeper_quorum")
        .text(s"zookeeper_quorum, default: ${defaultParams.zookeeper_quorum}")
        .action((x, c) => c.copy(zookeeper_quorum = x))
      arg[String]("<input>")
        .required()
        .text("input paths to a Res dataset of ratings")
        .action((x, c) => c.copy(input = x))
      arg[String]("userInput")
        .required()
        .text("userInput paths to a Res dataset of userids")
        .action((x, c) => c.copy(userInput = x))
      arg[String]("itemInput")
        .required()
        .text("itemInput paths to a Res dataset of itemids")
        .action((x, c) => c.copy(itemInput = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | /opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --class com.xiaomishu.com.cf.ResRecomendALS  \
          |  /home/hadoop/mllib/scala/spark_resrecomend-assembly-1.0.jar \
          |  --rank 5 --numIterations 20 --lambda 1.0 --kryo --separator :: --userSeprator ::  --itemSeprator ::\
          |  /user/hadoop/mllib/movielen/ratings.dat /user/hadoop/mllib/movielen/users.dat /user/hadoop/mllib/movielen/movies.dat \
          |  --recomendNum 10
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"MovieLensALS with res")
    if (params.kryo) {
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[ALSRegistrator].getName)
        .set("spark.kryoserializer.buffer.mb", "8")
    }
    val sc = new SparkContext(conf)


    //lond the config of Hbase，create Table recomend
    val confHbase = HBaseConfiguration.create()
    confHbase.set("hbase.zookeeper.property.clientPort", "2181")
    confHbase.set("hbase.zookeeper.quorum",params.zookeeper_quorum)
    confHbase.set("hbase.master", "h1.xiaomishu.com:60000")
    confHbase.addResource("/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml")
    confHbase.set(TableInputFormat.INPUT_TABLE, "recomend")

    val admin = new HBaseAdmin(confHbase)
    if (!admin.isTableAvailable("recomend")) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor("recomend")
      tableDesc.addFamily(new HColumnDescriptor("top".getBytes()))
      admin.createTable(tableDesc)
    }
    val table = new HTable(confHbase, "recomend")

    Logger.getRootLogger.setLevel(Level.WARN)



    val table1 = new HTable(confHbase, "mapping")


    val ratings = sc.textFile(params.input).map { line =>
      val fields = line.split(params.separator)
      if (params.implicitPrefs) {
        /*
         * MovieLens ratings are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the predicted rating is less than 3.
         * To map ratings to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
      } else {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    }.cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies items.")

    val splits = ratings.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */
      splits(1).map(x => Rating(x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
    } else {
      splits(1)
    }.cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")



    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .run(training)

    training.unpersist(blocking = false)

    ///////////////////////////////
    val rmse = computeRmse(model, test, params.implicitPrefs)

    println(s"Test RMSE = $rmse.")
    ////////////////////////////

    println("model finished")

    test.unpersist(blocking = false)
    //////////////////////////////////////////////////////////////////////////
    //predict Res
    // predictMode2Hbase(model,sc,params,ratings,table)

    //read all userids
    /**
    val userIds = sc.textFile(params.userInput).map { line =>
          val fields = line.split("::")
          (fields(0).toInt)
      }.cache()
      * */
    //val userIds = sc.textFile(params.userInput).cache()
    //read all item
    val userIds = sc.textFile(params.userInput).map { line =>
      if (line.contains(params.userSeprator)) {
        val fields = line.split(params.userSeprator)
        (fields(0).toInt)
      } else
        line.toInt
    }.cache()

    val itemIds = sc.textFile(params.itemInput).map { line =>
      if (line.contains(params.itemSeprator)) {
        val fields = line.split(params.itemSeprator)
        (fields(0).toInt)
      } else
        line.toInt
    }.cache()


    //recomend resids for user ,then read the result to Hbase


    // for(userid <- userIds){
    val useridSA = userIds.toArray()

    userIds.unpersist(blocking = false)
    var i = 0;
    println("recomendings......")
    while (i < useridSA.length) {
      //predictByUser(userid,model,sc,ratings,params,table)

      //all itemids that userid have made rating
      val userItemRatings = ratings.filter(line => line.user == useridSA(i))
      ratings.unpersist(blocking = false)
      val myRatedItemids = userItemRatings.map(_.product.toInt).toArray()
      val shoudPredicateItemsRDD = sc.parallelize(itemIds.filter(!myRatedItemids.contains(_)).toArray())
      println("started predict....................\ndefault recomend " + params.recomendNum)
      val recommendations = model.predict(shoudPredicateItemsRDD.map((useridSA(i), _))).collect.sortBy(_.rating).take(params.recomendNum)
      //val recommendations = m.predict(shoudPredicateItems.map((1,_))).collect.sortBy(_.rating).take(50)
      //recomends.......

      recommendations.foreach {r =>}

      var listBuffer = ListBuffer[String]()
      val jsonArray = new JSONArray(listBuffer.toList)
      recommendations.foreach { r =>
        val resrsc = r.product
        val row1 =  new Get(Bytes.toBytes(resrsc.toString))
        val HBaseRow = table1.get(row1)
        if(HBaseRow != null && !HBaseRow.isEmpty){
          val result = Bytes.toString(HBaseRow.getValue(Bytes.toBytes("res"), Bytes.toBytes("resid")))
          listBuffer += result
          println("推荐餐厅：" + result)
          }
      }

      //入库
      if(listBuffer!=null && listBuffer.length>0) {
        //def uuid = randomUUID.toString
        //useridSA(i).hashCode().toString.reverse + "_" +
        val rowkeyUserId = useridSA(i).toString
        val put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes(rowkeyUserId))
        val jsoanRes = new JSONArray(listBuffer.toList)

        val bf = ByteBuffer.allocate(4096)
        val outputStream = new java.io.ByteArrayOutputStream()
        val ob = new ObjectOutputStream(outputStream)
        ob.writeObject(jsoanRes)
        ob.flush()
        bf.put(outputStream.toByteArray())
        ob.close()
        //write to hbase
        put.add(Bytes.toBytes("top"), Bytes.toBytes("resid"), Bytes.toBytes(bf))
        table.put(put);
        table.flushCommits();
      }
      i += 1
    }
    //}
    //userIds.foreach(userid => predictByUser(userid,model,sc,ratings,params,table))

    ////////////////////////////////////////////////////////////////////
    itemIds.unpersist(blocking = false)

    sc.stop()
  }


  /**
   * predict according the model
   * @param model
   * @param sc
   */
  def predictMode2Hbase(model: MatrixFactorizationModel, sc: SparkContext, params: Params, ratings: RDD[Rating], table: org.apache.hadoop.hbase.client.HTable): Unit = {


    //read all userids
    val userIds = sc.textFile(params.userInput).map { line =>
      val fields = line.split("::")
      (fields(0).toInt)
    }.cache()

    //recomend resids for user ,then read the result to Hbase

    for (userid <- userIds) {
      //predictByUser(userid,model,sc,ratings,params,table)


      //read all item
      val itemIds = sc.textFile(params.itemInput).map { line =>
        val fields = line.split("::")
        (fields(0).toInt)
      }.cache()
      //all itemids that userid have made rating
      val userItemRatings = ratings.filter(line => line.user == userid)
      ratings.unpersist(blocking = false)
      val myRatedItemids = userItemRatings.map(_.product.toInt).toArray()
      val shoudPredicateItemsRDD = sc.parallelize(itemIds.filter(!myRatedItemids.contains(_)).toArray())
      println("started predict....................\ndefault recomend " + params.recomendNum)
      val recommendations = model.predict(shoudPredicateItemsRDD.map((userid, _))).collect.sortBy(_.rating).take(params.recomendNum)
      //val recommendations = m.predict(shoudPredicateItems.map((1,_))).collect.sortBy(_.rating).take(50)
      //recomends.......
      println("recomendings......")
      def uuid = randomUUID.toString
      val rowkeyUserId = uuid.reverse + "_" + userid
      val put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes(rowkeyUserId))
      recommendations.foreach { r =>
        println("推荐餐厅：" + r.product)
        //write to hbase
        put.add(Bytes.toBytes("top"), Bytes.toBytes("resid"), Bytes.toBytes(r.product))
      }
      table.put(put);
      table.flushCommits();


    }
    //userIds.foreach(userid => predictByUser(userid,model,sc,ratings,params,table))


  }

  //predict by userid and write to hbase
  def predictByUser(userid: Int, model: MatrixFactorizationModel, sc: SparkContext, ratings: org.apache.spark.rdd.RDD[Rating], params: Params, table: org.apache.hadoop.hbase.client.HTable): Unit = {

    println("进来了predictByUser..............................")


    //read all item
    val itemIds = sc.textFile(params.itemInput).map { line =>
      val fields = line.split("::")
      (fields(0).toInt)
    }.cache()



    //all itemids that userid have made rating
    val userItemRatings = ratings.filter(line => line.user == userid)
    ratings.unpersist(blocking = false)
    val myRatedItemids = userItemRatings.map(_.product.toInt).toArray()

    val shoudPredicateItemsRDD = sc.parallelize(itemIds.filter(!myRatedItemids.contains(_)).toArray())

    println("started predict....................\ndefault recomend " + params.recomendNum)
    val recommendations = model.predict(shoudPredicateItemsRDD.map((userid, _))).collect.sortBy(_.rating).take(params.recomendNum)
    //val recommendations = m.predict(shoudPredicateItems.map((1,_))).collect.sortBy(_.rating).take(50)
    //recomends.......
    println("recomendings......")





    def uuid = randomUUID.toString
    val rowkeyUserId = uuid.reverse + "_" + userid
    val put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes(rowkeyUserId))
    recommendations.foreach { r =>
      println("推荐餐厅：" + r.product)
      //write to hbase
      put.add(Bytes.toBytes("top"), Bytes.toBytes("resid"), Bytes.toBytes(r.product))
    }
    table.put(put);
    table.flushCommits();

  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {

    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
}


