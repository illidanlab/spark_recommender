package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ItemFeatureGenreAgg {

    val ItemGenreInd    = 2
    val ItemIdInd       = 1
    val FeatSepChar     = '|'
    val GenreIdInd      = 1
    val GenreLangInd    = 2
    val GenreDescInd    = 3
    val GenreLangFilt   = "en" //default language
    val Param_GenreLang = "lang"


  //for each passed date generate "duid, genre, time, date"
  def getDailyAggGenreWTime(
    dates:Array[String],
    jobInfo:RecJob):RDD[(String, String, Double, String)] = {

    //get spark context
    val sc = jobInfo.sc
    
    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)

    //for each date 
    val aggWtimeByGenre:RDD[(String, String, Double, String)] = dates.map{ date =>
      //get all watchtimes for duids with items
      //TODO: confirm if following is unique
      val watchTimes:RDD[(String, String, Double)] = sc.textFile(watchTimeResLoc +
        date).map {line =>
          val fields = line.split('\t')
          //duid, item, watchtime
          (fields(0), fields(1), fields(2).toDouble)
        }

      //Logger.info("watchTimesCount: " + watchTimes.count)
      
      //get genres for the items (item, genre)
      val items:RDD[String] = watchTimes.map(_._2)
      val itemGenre:RDD[(String, String)] = getItemGenre(date, items, jobInfo)
      val itemGroupedGenre:RDD[(String, Iterable[String])] =
        itemGenre.groupByKey()
      
      //Logger.info("itemGroupedGenre count: " + itemGroupedGenre.count)

    
      val itemGenresWTime:RDD[(String, ((String, Double),(Iterable[String])))]= watchTimes.map{ x =>
        //item, (duid, wtime)
        (x._2, (x._1, x._3))
      }.join(
        itemGroupedGenre
      )
      
      //Logger.info("itemGenresWTime count: " + itemGenresWTime.count)

      //get 'duid, genre, watchtime'
      val duidGenres:RDD[(String, String, Double, String)] = itemGenresWTime.map{x => //item, ((duid,wtime), Iterable[Genre])
        
        val duid:String             = x._2._1._1
        val wtime:Double            = x._2._1._2
        val genres:Iterable[String] = x._2._2
        
        (duid, (genres, wtime)) 
      }.flatMapValues{ genreNWtime =>
          val genres:Iterable[String] = genreNWtime._1
          val wtime:Double            = genreNWtime._2
          genres.map{genre =>
            (genre, wtime)
          }
      }.map{x =>
        val duid:String  = x._1
        val wtime:Double = x._2._2
        val genre:String = x._2._1
        ((duid, genre), wtime)
      }.reduceByKey( (wtime1:Double, wtime2:Double) => 
          wtime1 + wtime2
      ).map{x =>
        val duid:String = x._1._1
        val genre:String = x._1._2
        val wtime:Double = x._2
        (duid, genre, wtime, date)
      }
      
      duidGenres      
    }.reduce((a,b) => a.union(b))
    //Logger.info("Aggreagated records count: " + aggWtimeByGenre.count)
    aggWtimeByGenre
  }


  //return RDD of item and Genrefor passed date 
  def getItemGenre(date:String, items:RDD[String], 
    jobInfo:RecJob):RDD[(String, String)] = {
    
    //spark context
    val sc:SparkContext = jobInfo.sc

    val featSrc:String = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + date + "/program_genre*"
    //TODO: verify toSet
    val bItemSet = sc.broadcast(items.collect.toSet)
    val itemGenre:RDD[(String, String)] = sc.textFile(featSrc).map{line =>
      
      val fields = line.split(FeatSepChar)
      val item   = fields(ItemIdInd)
      val genre  = fields(ItemGenreInd)
      
      (item, genre)
    }.filter{itemGenre =>
      val item = itemGenre._1
      bItemSet.value(item)
    }
    
    itemGenre 
  }


  def saveAggGenreWtime(jobInfo:RecJob) = {
   
    //get spark context
    val sc = jobInfo.sc
    val dates:Array[String] = jobInfo.trainDates 
    
    //(duid, genre, wtime, date)
    val aggGenreTime:RDD[(String, String, Double, String)] =
      getDailyAggGenreWTime(dates, jobInfo)

    //save aggregated time spent on genre to text file
    val aggGenreFileName:String =
      jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" +
       "aggGenreWTime"
    aggGenreTime.map{x =>
     
     val duid:String  = x._1
     val genre:String = x._2
     val wtime:Double = x._3
     val date:String  = x._4
     
     duid + "," + genre + "," + wtime + "," + date 
    }.saveAsTextFile(aggGenreFileName)

  }

}
