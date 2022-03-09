package similarity

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


  /* _________________________________________ 2.2.1 _________________________________________ */

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
  val similarity_score_map = preprocessing_item.join(preprocessing_item)
                         .values.filter{
                           case ((user_1, item_1, prepocessed_rate_1), (user_2, item_2, preprocessed_rate_2)) => user_1 < user_2
                         }.map{
                           case ((user_1, item_1, prepocessed_rate_1), (user_2, item_2, preprocessed_rate_2)) => ((user_1, user_2), prepocessed_rate_1 * preprocessed_rate_2)
                         }.reduceByKey(_+_).collectAsMap()  
  


  def cosine_based_prediction(user: Int, item: Int): Double = {
    
    /* Keeping the entry corresponding to the item, and taking the associated set of user*/
      val filtered_item_set_user = item_set_user_map.filter(x => x._1 == item)
      if (filtered_item_set_user.isEmpty) {
        return 0.0
      }
      val set_user_of_item = filtered_item_set_user.map(x => x._2).head

      /* For each user having rated the item, take his rating as well as the similarity score with the original user */
      val similarity_and_rating = set_user_of_item.map(x =>(normalized_deviation_map.getOrElse((x, item), 42.0), 
      if (x < user) similarity_score_map.getOrElse((x, user), 42.0) else similarity_score_map.getOrElse((user, x), 42.0))) 
      val similarity_and_rating_cleaned = similarity_and_rating.filter(x => x._1 != 42.0 && x._2 != 42.0)

      /* Computing numerator and denominator */
      val numerator_usws = similarity_and_rating_cleaned.map(x => x._1 * x._2).sum
      val denominator_usws = similarity_and_rating_cleaned.map(x => math.abs(x._2)).sum
      
      if (denominator_usws == 0) {
        return 0.0
      } else {
        return numerator_usws / denominator_usws
      }
  }

  /* Predicting using cosine based prediction*/
  def cosine_pred(user:Int, item:Int): Double ={
    val mean_user = user_avg_rdd_map.getOrElse(user, 0.0)
    val usws = cosine_based_prediction(user,item)
    mean_user + (usws * scale((mean_user + usws), mean_user))
  }

  val cosine_mae = test.map(r => scala.math.abs(r.rating - cosine_pred(r.user,r.item))).reduce(_+_) / test.count.toDouble

  /* _________________________________________ 2.2.2 _________________________________________ */

  /* Computing Jaccard index between 2 users by leveraging the previously computed map (user, Item(user)) */
  def jaccard_index(u: Int, v: Int): Double = {
    val item_u = user_set_item_map.getOrElse(u, Set())
    val item_v = user_set_item_map.getOrElse(v, Set())
    val intersection_size = item_u.intersect(item_v).size.toDouble
    val union_size = item_u.union(item_v).size.toDouble

    intersection_size / union_size    
  }

  /* Predicting using jaccard index, by changing the cosine similarity by jaccard similarity*/
  def jaccard_based_prediction(user: Int, item: Int): Double = {
      val filtered_item_set_user = item_set_user_map.filter(x => x._1 == item)
      if (filtered_item_set_user.isEmpty) {
        return 0.0
      }
      val set_user_of_item = filtered_item_set_user.map(x => x._2).head
      
      /* Computation of the jaccard index*/
      val user_jaccard_index = set_user_of_item.map(x => (x, jaccard_index(x, user))).toMap

      val similarity_and_rating = set_user_of_item.map(x =>(normalized_deviation_map.getOrElse((x, item), 42.0), 
      user_jaccard_index.getOrElse(x, 42.0))) 
      val similarity_and_rating_cleaned = similarity_and_rating.filter(x => x._1 != 42.0 && x._2 != 42.0)

      val numerator_usws = similarity_and_rating_cleaned.map(x => x._1 * x._2).sum
      val denominator_usws = similarity_and_rating_cleaned.map(x => math.abs(x._2)).sum
      
      if (denominator_usws == 0) {
        return 0.0
      } else {
        return numerator_usws / denominator_usws
      }
  }

  /* Predicting using jaccard similarity*/
  def jaccard_pred(user:Int, item:Int): Double ={
    val mean_user = user_avg_rdd_map.getOrElse(user, 0.0)
    val usws = jaccard_based_prediction(user,item)
    mean_user + (usws * scale((mean_user+usws), mean_user))
  }

  val jaccard_mae = test.map(r => scala.math.abs(r.rating - jaccard_pred(r.user,r.item))).reduce(_+_) / test.count.toDouble
  
  /* _________________________________________ 2.2.3 _________________________________________ */

  /* Computing number of users, and worst case computation number of multiplication (see report attached for justification) */
  val nb_user = train.map(_.user).distinct().count()
  val worst_case_computation = nb_user * (nb_user + 1) / 2 
  
  /* _________________________________________ 2.2.4 _________________________________________ */

  val user_list = train.map(_.user).distinct()

  /*Computing all possibles user pairs by combining 2 maps - we map all users to their rated items, and compute the intersection between
  the original user set of rated items, and all user set of rated items*/
  val cartesian_intersection  = user_list.flatMap(u => {
    val u_set_item = user_set_item_map.getOrElse(u, Set())
    user_set_item_map.map(v => (if (u < v._1) (u, v._1) else (v._1, u), (u_set_item.intersect(v._2)).size.toDouble))
  }).distinct().map(_._2).collect

  val nb_of_possible_suv = cartesian_intersection.size

  /* Computing metrics*/
  val nb_computations_max = cartesian_intersection.max
  val nb_computations_min = cartesian_intersection.min
  val nb_computations_mean = cartesian_intersection.sum / nb_of_possible_suv
  val nb_computations_std = math.sqrt(cartesian_intersection.map(x => math.pow(x - nb_computations_mean, 2)).sum / nb_of_possible_suv)

  /* Removing user pairs with no common item */
  val nb_computations_non_zero = cartesian_intersection.filter(x => x != 0).size
  
  /* _________________________________________ 2.2.5 _________________________________________ */

  val zero_value_byte = nb_of_possible_suv * (64/8)
  val non_zero_value_byte = nb_computations_non_zero * (64/8)  
  
  /* _________________________________________ 2.2.6 / 2.2.7 _________________________________________ */

  /* Creating array to store benchmarking*/
  var cosine_method_time_array: Array[Double] = new Array[Double](5)
  var similarity_method_time_array: Array[Double] = new Array[Double](5)

  /* Benchmarking 5 times similarity and prediction computation - taking 2.1.1 code and picking computation time*/
  for (w <- 0 to 4) {
    val start_iter = System.nanoTime() 

    def scale_iter(x: Double, user_avg: Double): Double = (
    (x - user_avg) match {
      case 0 => 1
      case y if y > 0 => 5 - user_avg
      case _ => user_avg - 1
    }
    )

  val user_set_item_map_iter = train.groupBy(_.user).map(x => (x._1, x._2.map(_.item).toSet)).collectAsMap()
  val item_set_user_map_iter = train.groupBy(_.item).map(x => (x._1, x._2.map(_.user).toSet)).collectAsMap()
  
  val user_avg_rdd_iter = train.map(d => (d.user, d.rating)).mapValues(r => (r, 1)).reduceByKey(
    (r1, r2) => (r1._1 + r2._1, r1._2 + r2._2)
  ).mapValues(x => x._1 / x._2)
  val user_avg_rdd_map_iter = user_avg_rdd_iter.collectAsMap()

  val normalized_deviation_iter = train.map(d => (d.user, (d.item, d.rating))).join(user_avg_rdd_iter).map{
    case (user, ((item, rating), mean_rating_user)) => ((user, item), (rating, mean_rating_user))
    }.mapValues(
    x => (x._1 - x._2) / scale(x._1, x._2)
  )
  val normalized_deviation_map_iter = normalized_deviation_iter.collectAsMap()

  val denominator_iter = normalized_deviation_iter.map{
    case ((user, item), normalized_rating) => (user, normalized_rating * normalized_rating)
  }.reduceByKey(_+_).mapValues(x => math.sqrt(x))

  val preprocessing_iter = normalized_deviation_iter.map{
    case ((user, item), normalized_rating) => (user, ((user, item), normalized_rating))
  }.join(denominator_iter).map{
    case (user_key, (((user, item), normalized_rating), denominator)) =>  ((user, item), normalized_rating/denominator)
     }

  val preprocessing_item_iter = preprocessing_iter.map{
    case ((user, item), preprocessed_rate) => (item, (user, item, preprocessed_rate))
  }
  val similarity_score_map_iter = preprocessing_item_iter.join(preprocessing_item_iter)
                         .values.filter{
                           case ((user_1, item_1, prepocessed_rate_1), (user_2, item_2, preprocessed_rate_2)) => user_1 < user_2
                         }.map{
                           case ((user_1, item_1, prepocessed_rate_1), (user_2, item_2, preprocessed_rate_2)) => ((user_1, user_2), prepocessed_rate_1 * preprocessed_rate_2)
                         }.reduceByKey(_+_).collectAsMap()  

  /* First benchmark on similarities*/
  val end_iter_similarity = System.nanoTime()
  val nb_of_microseconds_iter_similarity = ((end_iter_similarity - start_iter) / 1e3 )
  similarity_method_time_array(w) = nb_of_microseconds_iter_similarity

  def cosine_based_prediction_iter(user: Int, item: Int): Double = {
      val filtered_item_set_user_iter = item_set_user_map_iter.filter(x => x._1 == item)

      if (filtered_item_set_user_iter.isEmpty) {
        return 0.0
      } 
      val set_user_of_item_iter = filtered_item_set_user_iter.map(x => x._2).head

      val similarity_and_rating_iter = set_user_of_item_iter.map(x =>(normalized_deviation_map_iter.getOrElse((x, item), 42.0), 
      if (x < user) similarity_score_map_iter.getOrElse((x, user), 42.0) else similarity_score_map_iter.getOrElse((user, x), 42.0))) 

      val similarity_and_rating_cleaned_iter = similarity_and_rating_iter.filter(x => x._1 != 42.0 && x._2 != 42.0)

      val numerator_usws_iter = similarity_and_rating_cleaned_iter.map(x => x._1 * x._2).sum

      val denominator_usws_iter = similarity_and_rating_cleaned_iter.map(x => math.abs(x._2)).sum
      
      if (denominator_usws_iter == 0) {
        return 0.0
      } else {
        return numerator_usws_iter / denominator_usws_iter
      }
  }


  def cosine_pred_iter(user:Int,item:Int): Double ={
    val mean_user_iter = user_avg_rdd_map_iter.getOrElse(user, 0.0)
    val usws_iter = cosine_based_prediction_iter(user,item)
    mean_user_iter + (usws_iter * scale_iter((mean_user_iter + usws_iter), mean_user_iter))
    }

  val cosine_mae_iter = test.map(r => scala.math.abs(r.rating - cosine_pred_iter(r.user,r.item))).reduce(_+_) / test.count.toDouble

  /* Second benchmark*/
  val end_iter = System.nanoTime()
  val nb_of_microseconds_iter = ((end_iter - start_iter) / 1e3 )
  cosine_method_time_array(w) = nb_of_microseconds_iter
  }

  /* Statistic computations */
  val cosine_max_mae_time = cosine_method_time_array.max
  val cosine_min_mae_time = cosine_method_time_array.min
  val cosine_mean_mae_time = cosine_method_time_array.sum / cosine_method_time_array.length
  val cosine_std_mae_time = math.sqrt(cosine_method_time_array.map(x => math.pow(x - cosine_mean_mae_time, 2)).sum / cosine_method_time_array.length)


  val similarity_max_mae_time = similarity_method_time_array.max
  val similarity_min_mae_time = similarity_method_time_array.min
  val similarity_mean_mae_time = similarity_method_time_array.sum / similarity_method_time_array.length
  val similarity_std_mae_time = math.sqrt(similarity_method_time_array.map(x => math.pow(x - similarity_mean_mae_time, 2)).sum / similarity_method_time_array.length)

  
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
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> cosine_mae, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (cosine_mae - 0.7669) // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> jaccard_mae, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (jaccard_mae - cosine_mae) // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> worst_case_computation // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> nb_computations_min,  // Datatype of answer: Double
              "max" -> nb_computations_max, // Datatype of answer: Double
              "average" -> nb_computations_mean, // Datatype of answer: Double
              "stddev" -> nb_computations_std // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> non_zero_value_byte // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> cosine_min_mae_time,  // Datatype of answer: Double
              "max" -> cosine_max_mae_time, // Datatype of answer: Double
              "average" -> cosine_mean_mae_time, // Datatype of answer: Double
              "stddev" -> cosine_std_mae_time // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> similarity_min_mae_time,  // Datatype of answer: Double
              "max" -> similarity_max_mae_time, // Datatype of answer: Double
              "average" -> similarity_mean_mae_time, // Datatype of answer: Double
              "stddev" -> similarity_std_mae_time // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> similarity_mean_mae_time / nb_of_possible_suv, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> similarity_mean_mae_time / cosine_mean_mae_time // Datatype of answer: Double
          )
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
