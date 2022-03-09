package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")
  
  /* We firstly define the scale function */
  def scale(x: Double, user_avg: Double): Double = (
    (x - user_avg) match {
      case 0 => 1
      case y if y > 0 => 5 - user_avg
      case _ => user_avg - 1
    }
  )
    
  /* We compute the map (user, Item(user)) as well as the map (item, User(item)) */
  val user_set_item_map = train.groupBy(_.user).map(x => (x._1, x._2.map(_.item).toSet)).collectAsMap()
  val item_set_user_map = train.groupBy(_.item).map(x => (x._1, x._2.map(_.user).toSet)).collectAsMap()
  
  /* We compute the user average by combining mapValues and reduce by key on the RDD (user, rating)*/
  val user_avg_rdd = train.map(d => (d.user, d.rating)).mapValues(r => (r, 1)).reduceByKey(
    (r1, r2) => (r1._1 + r2._1, r1._2 + r2._2)
  ).mapValues(x => x._1 / x._2)
  val user_avg_rdd_map = user_avg_rdd.collectAsMap()

  /* Computation of the normalized deviation for every pair of user - items when the item is in the training set */ 
  val normalized_deviation = train.map(d => (d.user, (d.item, d.rating))).join(user_avg_rdd).map{
    case (user, ((item, rating), mean_rating_user)) => ((user, item), (rating, mean_rating_user))
    }.mapValues(
    x => (x._1 - x._2) / scale(x._1, x._2)
  )
  val normalized_deviation_map = normalized_deviation.collectAsMap()

  /* We compute the denominator of the normalized deviation, by mapping with user as key, then reducing by key*/
  val denominator = normalized_deviation.map{
    case ((user, item), normalized_rating) => (user, normalized_rating * normalized_rating)
  }.reduceByKey(_+_).mapValues(x => math.sqrt(x))

  /* We compute the preprocessing indices shown in equation (4) by using user as a key to join with denominator, then computing value*/
  val preprocessing = normalized_deviation.map{
    case ((user, item), normalized_rating) => (user, ((user, item), normalized_rating))
  }.join(denominator).map{
    case (user_key, (((user, item), normalized_rating), denominator)) =>  ((user, item), normalized_rating/denominator)
     }

  /* Computation of the similarity score, by mapping with item as key, joining on them, and computing similarity from it*/
  val preprocessing_item = preprocessing.map{
    case ((user, item), preprocessed_rate) => (item, (user, item, preprocessed_rate))
  }
  val similarity_score = preprocessing_item.join(preprocessing_item)
                         .values.filter{
                           case ((user_1, item_1, prepocessed_rate_1), (user_2, item_2, preprocessed_rate_2)) => user_1 < user_2
                         }.map{
                           case ((user_1, item_1, prepocessed_rate_1), (user_2, item_2, preprocessed_rate_2)) => ((user_1, user_2), prepocessed_rate_1 * preprocessed_rate_2)
                         }.reduceByKey(_+_)

  /* Sorting KNN - by aggregating on same user and ranking based on decreasing similarity, we have a RDD with user as key, and ordered pair of similarities as values*/  
  val knn_similarity_sorted = similarity_score.map{
    case ((user_1, user_2), similarity) => (user_1, (user_2, similarity))
  }.sortBy(x => (x._1, -x._2._2)).groupByKey().map{
    case (user_1, tuple) => (user_1, tuple.toList)
  }.cache()

  /* Compute ratings using knn 
     This code doesn't work and get stuck when collecting the users
     I've tried several approach, but all were stuck - thus, I've given the code but I can't give the values
  */ 
  def compute_k_nn(user: Int, item: Int, k: Int): Double = {
    
    /* Take the given user, take the k first of the sorted list and collect it*/
    val filtered_knn = knn_similarity_sorted.filter(x => x._1 == user)
    val ordered_list = filtered_knn.map{
      case (user_1, user_2_sorted_list) => user_2_sorted_list.take(k)
    }
    val knn_users_collected = ordered_list.collect()(0)

    /* Join the top k users to their rating of the given item, as well as their similarities*/
    val similarity_and_rating = knn_users_collected.map(
      x => normalized_deviation_map.filter(y => (x._1 == y._1._1) && (y._1._2 == item)).map(y => (x._2, y._2))
    ).filter(_.nonEmpty).flatMap(x => x.map(y => (y._1, y._2)))

    /* Computing numerator and denominator */
    val numerator_usws = similarity_and_rating.map(x => x._1 * x._2).sum
    val denominator_usws = similarity_and_rating.map(x => math.abs(x._1)).sum
      
    if (denominator_usws == 0) {
      return 0.0
    } else {
      return numerator_usws / denominator_usws
    }
  }

  /* Predicting KNN */
  def k_nn_pred(user:Int, item:Int, k:Int): Double ={
    val mean_user = user_avg_rdd_map.getOrElse(user, 0.0)
    val usws = compute_k_nn(user,item,k)
    mean_user + (usws * scale((mean_user + usws), mean_user))
  }
  
  /*K values, and data structure used to store MAE*/
  val knn_k = List(10, 30, 50, 100, 200, 300, 400, 800, 942)
  var knn_mae: Array[Double] = new Array[Double](9)
  
  /* Helper variables to store lowest k s.t. MAE is lower than the baseline*/
  var mae_lower_reached = false
  val baseline_mae = 0.7669
  var lowest_k = -1
  var how_much_lower = -1.0

  /* Helper data structure to store the number of bytes*/
  val nb_user = train.map(_.user).distinct().count() - 1
  var number_of_bytes: Array[Double] = new Array[Double](9)

  for (w <- 0 to 8) {
  /* Computing MAE for the K*/
  val k = knn_k(2)
  knn_mae(w) = test.map(r => scala.math.abs(r.rating - k_nn_pred(r.user,r.item, k))).reduce(_+_) / test.count.toDouble

  /* Computing number of bytes needed - more informations about the computation in the report*/
  number_of_bytes(w) = nb_user * (64/8) * k 

  /* When it is the first time knn performs better than baseline, store the result and the k in variables*/
  if (knn_mae(w) < baseline_mae && !mae_lower_reached){
    lowest_k = w
    how_much_lower = knn_mae(w) - baseline_mae
    mae_lower_reached = true
  }
  }

  /* */
  val ram_size: Long = 16138216000L

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> knn_mae(0), // Datatype of answer: Double
            "MaeForK=30" -> knn_mae(1), // Datatype of answer: Double
            "MaeForK=50" -> knn_mae(2), // Datatype of answer: Double
            "MaeForK=100" -> knn_mae(3), // Datatype of answer: Double
            "MaeForK=200" -> knn_mae(4), // Datatype of answer: Double
            "MaeForK=300" -> knn_mae(5), // Datatype of answer: Double
            "MaeForK=400" -> knn_mae(6), // Datatype of answer: Double
            "MaeForK=800" -> knn_mae(7), // Datatype of answer: Double
            "MaeForK=943" -> knn_mae(8), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> lowest_k, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> how_much_lower // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> number_of_bytes(0), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> number_of_bytes(1), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> number_of_bytes(2), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> number_of_bytes(3), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> number_of_bytes(4), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> number_of_bytes(5), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> number_of_bytes(6), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> number_of_bytes(7), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> number_of_bytes(8) // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> ram_size, // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> 0 // Datatype of answer: Long
          )

          // Answer the Question 3.2.04 exclusively on the report.
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
