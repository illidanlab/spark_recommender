package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.Logger
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
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



  def getValidACRPaths(dates:Array[String], 
    watchTimeResLoc:String,
    sc:SparkContext):Array[(String, String)] = {
    
    val hadoopConf = sc.hadoopConfiguration
    val wTimeACRPaths = dates.map{date => 
      (date, watchTimeResLoc + date)
    }.filter(x => {
      val path = new Path(x._2)
      val fileSystem = path.getFileSystem(hadoopConf)
      fileSystem.exists(new Path(x._2))
    })
    wTimeACRPaths
  }


  //return list of active duids throughout the week
  def getActiveDuids(wTimeACRPaths:Array[(String, String)],
    sc:SparkContext):RDD[String] = {
    
    val activeDuids:RDD[String] = wTimeACRPaths.map{dateNACRPath =>
      
      val date      = dateNACRPath._1
      val acrPath   = dateNACRPath._2
      val month:Int = date.substring(4,6).toInt
      val day:Int   = date.substring(6,8).toInt
      val week:Int  = day / 7
   

      val duidsWeek:RDD[(String, Int)] = sc.textFile(acrPath).map{line =>
        val fields = line.split('\t')
        fields(0)
      }.map{x => (x, week)}
      duidsWeek
    }.reduce((a,b) => a.union(b)).distinct.groupByKey.map{x =>
      val duid = x._1
      val numWeeks = x._2.size
      (duid, numWeeks)
    }.filter{_._2 >= 2}.map{x =>
      x._1  
    }
    activeDuids  
  }


  def getGenreMapping(dates:Array[String],
    jobInfo:RecJob):RDD[(String, String)] = {
    
    val sc:SparkContext        = jobInfo.sc
    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)
    val wTimeACRPaths = getValidACRPaths(dates, watchTimeResLoc, sc)
    
    wTimeACRPaths.map{dateNACRPath =>
   
      val date:String            = dateNACRPath._1
      val acrPath:String         = dateNACRPath._2
      val param_GenreLang:String = GenreLangFilt
      val genreSource            = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + date + "/genre*"
    
      //genreId, genreDesc
      val genreMap:RDD[(String, String)] =
        sc.textFile(genreSource).map{line =>
          val fields = line.split('|')
          (fields(GenreIdInd), fields(GenreLangInd), fields(GenreDescInd))
        }.filter{genreTriplet =>
          val genreLang = genreTriplet._2
          genreLang == param_GenreLang
        }.map{x =>
          val genreId:String   = x._1
          val genreDesc:String = x._3
          (genreId, genreDesc)
        }.distinct

      genreMap
    }.reduce((a,b) => a.union(b)).distinct
  }


  //for each passed date generate 
  //duid, genre, watchtime, week, month
  def getWeeklyAggGenreWTime(
    dates:Array[String],
    jobInfo:RecJob):RDD[(String, String, Int, Int, Int)] = {

    //get spark context
    val sc = jobInfo.sc
    
    //get hadoop config
    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)
    val wTimeACRPaths = getValidACRPaths(dates, watchTimeResLoc, sc)

    //get active duids based on usage
    val activeDuids:RDD[String] = getActiveDuids(wTimeACRPaths, sc)

    //for each date 
    val aggWtimeByGenre:RDD[(String, String, Int, Int, Int)] =
      wTimeACRPaths.map{ dateNACRPath =>
      
      val date    = dateNACRPath._1
      val acrPath = dateNACRPath._2

      //get all watchtimes for duids with items
      val watchTimes:RDD[(String, String, Int)] = sc.textFile(acrPath).map{line =>
        val fields = line.split('\t')
        //duid, item, watchtime
        (fields(0), fields(1), fields(2).toInt)
      }.filter{x => 
          x._3 > 200 //filter watchtime greater than 200 seconds
      }

      //Logger.info("watchTimesCount: " + watchTimes.count)
      
      //get genres for the items (item, genre)
      val items:RDD[String] = watchTimes.map(_._2)
      val itemGenre:RDD[(String, String)] = getItemGenre(date, items, jobInfo)
      val itemGroupedGenre:RDD[(String, Iterable[String])] =
        itemGenre.groupByKey()
      
      //Logger.info("itemGroupedGenre count: " + itemGroupedGenre.count)
    
      val itemGenresWTime:RDD[(String, ((String, Int),(Iterable[String])))]= watchTimes.map{ x =>
        //item, (duid, wtime)
        (x._2, (x._1, x._3))
      }.join(
        itemGroupedGenre
      )
      
      //Logger.info("itemGenresWTime count: " + itemGenresWTime.count)

      //get 'duid, genre, watchtime'
      val duidGenres:RDD[((String, String, Int, Int), Int)] = itemGenresWTime.map{x => //item, ((duid,wtime), Iterable[Genre])
        
        val duid:String             = x._2._1._1
        val wtime:Int               = x._2._1._2
        val genres:Iterable[String] = x._2._2
        
        (duid, (genres, wtime)) 
      }.flatMapValues{genreNWtime =>
          
        val genres:Iterable[String] = genreNWtime._1
        val wtime:Int               = genreNWtime._2
          
        genres.map{genre =>
          (genre, wtime)
        }
      }.map{x =>
        
        val duid:String  = x._1
        val wtime:Int    = x._2._2
        val genre:String = x._2._1
        val month:Int    = date.substring(4,6).toInt
        val day:Int      = date.substring(6,8).toInt
        val week:Int     = day / 7

        ((duid, genre, week, month), wtime)
      }
     
      //for each date following RDD is output
      duidGenres      
      }.reduce((a,b) => a.union(b)).reduceByKey((a:Int,b:Int) => a+b).map{x =>
        
        val duid:String  = x._1._1
        val genre:String = x._1._2
        val week:Int     = x._1._3
        val month:Int    = x._1._4
        val wtime:Int    = x._2
        
        (duid, (genre, wtime, week, month))
        }.join(activeDuids.map{x => (x,1)}).map{x =>
          val duid:String = x._1
          val genre:String = x._2._1._1
          val wtime:Int = x._2._1._2
          val week:Int = x._2._1._3
          val month:Int = x._2._1._4
          (duid, genre, wtime, week, month)
        }

    //Logger.info("Aggreagated records count: " + aggWtimeByGenre.count)
    aggWtimeByGenre
  }


  //for each passed date generate "duid, genre, time, date"
  def getDailyAggGenreWTime(
    dates:Array[String],
    jobInfo:RecJob):RDD[(String, String, Int, String)] = {

    //get spark context
    val sc = jobInfo.sc
    
    //get hadoop config
    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)
    val wTimeACRPaths = getValidACRPaths(dates, watchTimeResLoc, sc)

    //for each date 
    val aggWtimeByGenre:RDD[(String, String, Int, String)] =
      wTimeACRPaths.map{ dateNACRPath =>
      
      val date    = dateNACRPath._1
      val acrPath = dateNACRPath._2

      //get all watchtimes for duids with items
      val watchTimes:RDD[(String, String, Int)] = sc.textFile(acrPath).map{line =>
        val fields = line.split('\t')
        //duid, item, watchtime
        (fields(0), fields(1), fields(2).toInt)
      }

      //Logger.info("watchTimesCount: " + watchTimes.count)
      
      //get genres for the items (item, genre)
      val items:RDD[String] = watchTimes.map(_._2)
      val itemGenre:RDD[(String, String)] = getItemGenre(date, items, jobInfo)
      val itemGroupedGenre:RDD[(String, Iterable[String])] =
        itemGenre.groupByKey()
      
      //Logger.info("itemGroupedGenre count: " + itemGroupedGenre.count)
    
      val itemGenresWTime:RDD[(String, ((String, Int),(Iterable[String])))]= watchTimes.map{ x =>
        //item, (duid, wtime)
        (x._2, (x._1, x._3))
      }.join(
        itemGroupedGenre
      )
      
      //Logger.info("itemGenresWTime count: " + itemGenresWTime.count)

      //get 'duid, genre, watchtime'
      val duidGenres:RDD[(String, String, Int, String)] = itemGenresWTime.map{x => //item, ((duid,wtime), Iterable[Genre])
        
        val duid:String             = x._2._1._1
        val wtime:Int            = x._2._1._2
        val genres:Iterable[String] = x._2._2
        
        (duid, (genres, wtime)) 
      }.flatMapValues{genreNWtime =>
          val genres:Iterable[String] = genreNWtime._1
          val wtime:Int            = genreNWtime._2
          genres.map{genre =>
            (genre, wtime)
          }
      }.map{x =>
        val duid:String  = x._1
        val wtime:Int = x._2._2
        val genre:String = x._2._1
        ((duid, genre), wtime)
      }.reduceByKey( (wtime1:Int, wtime2:Int) => 
          wtime1 + wtime2
      ).map{x =>
        val duid:String = x._1._1
        val genre:String = x._1._2
        val wtime:Int = x._2
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
    
    
    val param_GenreLang:String = GenreLangFilt
    val genreSource = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + date + "/genre*" 
    
    //genreId, genreDesc
    val genreMap:RDD[(String, String)] =
      sc.textFile(genreSource).map{line =>
        val fields = line.split('|')
        (fields(GenreIdInd), fields(GenreLangInd), fields(GenreDescInd))
      }.filter{genreTriplet =>
        val genreLang = genreTriplet._2
        genreLang == param_GenreLang
      }.map{x =>
        val genreId:String   = x._1
        val genreDesc:String = x._3
        (genreId, genreDesc)
      }.distinct

    val genreIds = genreMap.map{x => x._1}.collect.toSet  
    val bGenreIdSet = sc.broadcast(genreIds)

    val itemGenre:RDD[(String, String)] = sc.textFile(featSrc).map{line =>
      
      val fields = line.split(FeatSepChar)
      val item   = fields(ItemIdInd)
      val genre  = fields(ItemGenreInd)
      
      (item, genre)
    }.filter{itemGenre =>
      val item = itemGenre._1
      val genre = itemGenre._2
      bItemSet.value(item) && bGenreIdSet.value(genre) 
    }
    
    itemGenre 
  }


  def saveAggGenreWtime(jobInfo:RecJob) = {
   
    //get spark context
    val sc = jobInfo.sc
    val dates:Array[String] = jobInfo.trainDates 
    
    //(duid, genre, wtime, date)
    val aggGenreTime:RDD[(String, String, Int, String)] =
      getDailyAggGenreWTime(dates, jobInfo)

    //save aggregated time spent on genre to text file
    val aggGenreFileName:String =
      jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" +
       "aggGenreWTime"
    aggGenreTime.map{x =>
     
     val duid:String  = x._1
     val genre:String = x._2
     val wtime:Int = x._3
     val date:String  = x._4
     
     duid + "," + genre + "," + wtime + "," + date 
    }.coalesce(100).saveAsTextFile(aggGenreFileName)

  }


  def saveAggGenreWeeklyFromDailyFile(jobInfo:RecJob) = {
   
    val sc:SparkContext = jobInfo.sc
    val dailyFile:String = "s3n://vddil.recsys.east/mohit/aggGenre/201407/"

    val dailyAgg:RDD[(String, String, Int, String)] =
      sc.textFile(dailyFile).map{ line =>
        val fields = line.split(',')
        val duid:String = fields(0)
        val genre:String = fields(1)
        val wtime:Int = fields(2).toInt
        val date:String = fields(3)
        (duid, genre, wtime, date)
      }

    val aggGenreFileName:String =
      jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + "aggGenreWTimeWeekly"
    
    val weeklyAggGenreTime:RDD[(String, String, Int, Int, Int)] =
      getAggGenreWeekMonthDay(jobInfo, dailyAgg)

    weeklyAggGenreTime.persist()

    Logger.info("No. of weekly records: " + weeklyAggGenreTime.count())

    weeklyAggGenreTime.map{x =>
     
     val duid:String  = x._1
     val genre:String = x._2
     val wtime:Int    = x._3
     val week:Int     = x._4
     val month:Int    = x._5
     
     duid + "," + genre + "," + wtime + "," + week + "," + month 
    }.saveAsTextFile(aggGenreFileName)
  
  }


  def saveAggGenreWeekly(jobInfo:RecJob) = {
   
    //get spark context
    val sc = jobInfo.sc
    val dates:Array[String] = jobInfo.trainDates 
    
    //(duid, genre, watchtime, week, month)
    val weeklyAggGenreTime:RDD[(String, String, Int, Int, Int)] =
      getWeeklyAggGenreWTime(dates, jobInfo)  

    //save aggregated time spent on genre to text file
    val aggGenreFileName:String =
      jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + "aggGenreWTimeWeekly"
 
    Logger.info("No. of weekly records: " + weeklyAggGenreTime.count())

    weeklyAggGenreTime.map{x =>
     
     val duid:String  = x._1
     val genre:String = x._2
     val wtime:Int    = x._3
     val week:Int     = x._4
     val month:Int    = x._5
     
     duid + "," + genre + "," + wtime + "," + week + "," + month 
    }.saveAsTextFile(aggGenreFileName)

    val genreMapFileName:String =
      jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + "aggGenreMap"
    val genreMapping:RDD[(String, String)] = getGenreMapping(dates, jobInfo)
    genreMapping.map{x => x._1 + "," + x._2}.saveAsTextFile(genreMapFileName)
  
  }


  //duid, genre, watchtime, week, month
  def getAggGenreWeekMonthDay(
      jobInfo:RecJob,
      dailyAgg:RDD[(String, String, Int, String)]
    ):RDD[(String, String, Int, Int, Int)] = {
    
    dailyAgg.map{x =>

      val duid:String  = x._1
      val genre:String = x._2
      val wtime:Int    = x._3.toInt
      val dtStr:String = x._4
      val year:Int     = dtStr.substring(0,4).toInt
      val month:Int    = dtStr.substring(4,6).toInt
      val day:Int      = dtStr.substring(6,8).toInt
      val week:Int     = day / 7  
      ((duid, genre, week, month), wtime)
    }.reduceByKey((a:Int,b:Int) => a+b ).map {x =>
      val duid:String = x._1._1
      val genre:String = x._1._2
      val week:Int = x._1._3
      val month:Int = x._1._4
      val wtime:Int = x._2
      (duid, genre, wtime, week, month)
    }

  }


}
