package com.samsung.vddil.recsys.utils

import com.samsung.vddil.recsys.job.Rating
import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source


/**
 *This object facilitates selection of ratings and writing them to disk.
 */
object RatingsWriter{


  /*
   * convert rating triplet "user, item, rating" per line in a file to 
   * 
   */
  def getBinRatingsRDD(fileName:String, sc:SparkContext, sep:String = ","): RDD[Rating] = {
    val ratings = Source.fromFile(fileName).getLines.map{line =>
      val fields = line.split(sep)
      val user = fields(0).toInt
      val item = fields(1).toInt
      Rating(user, item, 1)
    }.toList
    sc.parallelize(ratings)
  }


  def getFilteredRatings(ratings:RDD[Rating], userThresh:Int, 
    itemThresh:Int): RDD[Rating] = {
      
      val userRatings:RDD[(Int, (Int, Double))] = ratings.map{rating =>
        (rating.user, (rating.item, rating.rating))
      }
      val userRatingsCount:RDD[(Int, Int)] = userRatings.map{x=> (x._1, 1)}.reduceByKey(_+_)
      val filteredUsers:RDD[(Int, Int)] = userRatingsCount.filter(_._2 >
        userThresh)
      val filtUserRatings:RDD[(Int, (Int, Double))] =
        userRatings.join(filteredUsers).map{x =>
          val user = x._1
          val item = x._2._1._1
          val rating = x._2._1._2
          (item, (user, rating))
        }
      val itemRatings:RDD[(Int, (Int, Double))] = ratings.map{rating =>
        (rating.item, (rating.user, rating.rating))
      }
      val itemRatingsCount:RDD[(Int, Int)] = itemRatings.map{x=> (x._1, 1)}.reduceByKey(_+_)
      val filteredItems:RDD[(Int, Int)] = itemRatingsCount.filter(_._2 >
        itemThresh)
      filtUserRatings.join(filteredItems).map{x =>
        val item = x._1
        val user = x._2._1._1
        val rating = x._2._1._2
        Rating(user, item, rating)
      }
  }


  /**
   * convert passed RDD of ratings to RDD of item and its rating string 
   * @param ratings RDD of rating objects
   * @param items list of items from which ratings are selected
   * @param fileName hdfs file location to save ratings
   */
  def convRatingsToStr(ratings:RDD[Rating], items:List[Int], sep:String = ","):RDD[(Int, String)] = {
    //sort items in list
    val sortedItems = items.sorted
    
    //create mapping of indexes to items
    val itemInd = sortedItems.zip(1 to sortedItems.length).toMap
    
    //convert item in ratings to index and group by user
    val convRatings = ratings.map{rating =>
      val user = rating.user
      val item = itemInd(rating.item)
      val rat = rating.rating
      (user, (item, rat))
    }

    //get for each user all item ratings in string "item1 rat1 item2 rat2"
    val userRatingsStr:RDD[(Int, String)] = convRatings.groupByKey.map{userRatings =>
      val user = userRatings._1
      val itemRatings:Iterable[(Int, Double)] = userRatings._2
      //conv item ratings into string of form "item1 rating1 item2 rating2 ..."
      val itemRatingStr = itemRatings.map{case (item, rat) =>
        item + sep +  rat
      }.mkString(sep)
      (user, itemRatingStr)
    }
    userRatingsStr
  }


  
  /**
   * save the passed ratings into a file in format
   * "user item1Index rat1 item2Index rat2"
   * @param ratings RDD of rating objects
   * @param items list of items from which ratings are selected
   * @param fileName hdfs file location to save ratings
   */
  def saveUserItemRatToHDFSFile(ratings:RDD[Rating], items:List[Int], fileName:String, 
                                  sep:String=",") = {
    val userRatingsStr = convRatingsToStr(ratings, items)
    userRatingsStr.map{x => x._1 + sep + x._2}.saveAsTextFile(fileName)
  }



  /**
   * save the passed ratings into a file in format
   * "item1Index rat1 item2Index rat2"i
   * each line represents a row
   * @param ratings RDD of rating objects
   * @param items list of items from which ratings are selected
   * @param fileName hdfs file location to save ratings
   */
  def saveUserItemRatToCSRFile(ratings:RDD[Rating], items:List[Int], fileName:String) = {
    val userRatingsStr:RDD[(Int, String)] = convRatingsToStr(ratings, items)
    val sortByUserRatings = userRatingsStr.collect.sortBy(_._1)
    val writer = new PrintWriter(new File(fileName))
    sortByUserRatings.map(x => writer.write(x._2 + "\n"))
    writer.close
  }

}



