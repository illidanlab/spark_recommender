package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.Logger
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util.Calendar


object ItemFeatureGenreAgg {

    val ItemGenreInd    = 2
    val ItemIdInd       = 1
    val ItemIfGenreTop  = 3
    val FeatSepChar     = '|'
    val GenreIdInd      = 1
    val GenreLangInd    = 2
    val GenreDescInd    = 3
    val GenreLangFilt   = "en" //default language
    val Param_GenreLang = "lang"
    val Min_Wtime       = 300;
    

  def getValidACRPaths(dates:Array[String], 
    watchTimeResLoc:String,
    sc:SparkContext):Map[Int, Array[(Int, (String, String))]] = {
   

    val calDates:Array[(Calendar, String)] = dates.map{date =>
      val month:Int = date.substring(4,6).toInt
      val day:Int   = date.substring(6,8).toInt
      val year:Int  = date.substring(0,4).toInt
      val dt:Calendar = Calendar.getInstance()
      dt.set(year, month-1, day)
      (dt, date)
    } 

    //filter in only weekends
    val weekends:Array[String] = calDates.filter{dt => 
      val dayOfWeek = dt._1.get(Calendar.DAY_OF_WEEK)
      dayOfWeek == Calendar.FRIDAY || dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY
    }.map{_._2}

    //filter out weekends
    val workdays:Array[String] = calDates.filterNot{dt => 
      val dayOfWeek = dt._1.get(Calendar.DAY_OF_WEEK)
      dayOfWeek == Calendar.FRIDAY || dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY
    }.map{_._2} 
  
    //val filtDates:Array[String] = dates
    val filtDates:Array[String] = weekends
    //val filtDates:Array[String] = workdays

    val hadoopConf = sc.hadoopConfiguration
    val wTimeACRPaths:Map[Int, Array[(Int, (String, String))]] = filtDates.map{date =>
      val month:Int = date.substring(4,6).toInt
      (month, (date, watchTimeResLoc + date))
    }.filter(x => {
      val path = new Path(x._2._2)
      val fileSystem = path.getFileSystem(hadoopConf)
      fileSystem.exists(path)
    }).groupBy(_._1)
    
    wTimeACRPaths
  
  }


  //return list of active duids throughout the week
  def getActiveDuids(wTimeACRPaths:Array[(String, String)],
    sc:SparkContext):RDD[String] = {
   
    val numMonths:Int = wTimeACRPaths.map{dateNACRPath =>
      val date = dateNACRPath._1
      val month:Int = date.substring(4,6).toInt
      month
    }.distinct.size

    Logger.info("\nNo. of months: " + numMonths)

    val activeDuids:RDD[String] = wTimeACRPaths.map{dateNACRPath =>
      
      val date      = dateNACRPath._1
      val acrPath   = dateNACRPath._2
      val month:Int = date.substring(4,6).toInt
      val day:Int   = date.substring(6,8).toInt
      val week:Int  = day / 7

      val duidsMonthWeek:RDD[((String,Int), Int)] = sc.textFile(acrPath).map{line =>
        val fields = line.split('\t')
        fields(0)
      }.map{x => ((x, month), week)}

      duidsMonthWeek
    }.reduce((a,b) => a.union(b)).distinct.groupByKey.map{x =>
      val duidMonth = x._1
      val numWeeks  = x._2.size
      (duidMonth, numWeeks)
    }.filter{_._2 >= 2}.map{x =>
      val duid:String = x._1._1
      val month:Int   = x._1._2
      (duid, month)
    }.groupByKey.filter(_._2.size == numMonths).map{x=>
      val duid:String = x._1
      duid
    }
    
    activeDuids  
  }


  def getGenreMapping(wTimeACRPaths:Array[(String, String)],
    jobInfo:RecJob):RDD[(String, String)] = {
    
    val sc:SparkContext        = jobInfo.sc
    
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
          val genreDesc:String = x._3.replaceAll("""\s""", "-")
          (genreId, genreDesc)
        }.distinct

      genreMap
    }.reduce((a,b) => a.union(b)).distinct
  }


  //for each passed date generate 
  //duid, genre, watchtime, week, month
  def getWeeklyAggGenreWTime(
    wTimeACRPaths:Array[(String, String)],
    jobInfo:RecJob):RDD[(String, String, Int, Int, Int)] = {

    //get spark context
    val sc = jobInfo.sc
    
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
        x._3 >= Min_Wtime //filter watchtime >= Min_Wtime seconds
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
    wTimeACRPaths:Array[(String, String)],
    jobInfo:RecJob):RDD[(String, String, Int, String)] = {

    //get spark context
    val sc = jobInfo.sc
    
    //get hadoop config
    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)

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
      
      val fields     = line.split(FeatSepChar)
      val item       = fields(ItemIdInd)
      val genre      = fields(ItemGenreInd)
      val isGenreTop:Int = fields(ItemIfGenreTop).replaceAll("""\s""","").toInt
      
      (item, genre, isGenreTop)
    }.filter{itemGenre =>

      val item       = itemGenre._1
      val genre      = itemGenre._2
      val isGenreTop = itemGenre._3
      //filter by broadcast items and genre present and if sub genre 
      bItemSet.value(item) && bGenreIdSet.value(genre) && ((isGenreTop == 1) || (isGenreTop == 2) || (isGenreTop == 3))
      //bItemSet.value(item) && bGenreIdSet.value(genre)
    }.map {x =>
      val item = x._1
      val genre = x._2
      (item, genre)
    }
    
    itemGenre 
  }


  def saveAggGenreWtime(jobInfo:RecJob) = {
   
    //get spark context
    val sc = jobInfo.sc
    val dates:Array[String] = jobInfo.trainDates 
    
    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)
    val wTimeMonthlyACRPaths:Map[Int, Array[(Int, (String, String))]] = getValidACRPaths(dates, watchTimeResLoc, sc)
    
    //for each month save aggregate Genre
    wTimeMonthlyACRPaths.map{case (month, monthPaths) =>
      
      val wTimeACRPaths:Array[(String, String)] = monthPaths.map(_._2)
      
      //(duid, genre, wtime, date)
      val aggGenreTime:RDD[(String, String, Int, String)] =
        getDailyAggGenreWTime(wTimeACRPaths, jobInfo)

      //save aggregated time spent on genre to text file
      val aggGenreFileName:String =
        jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + "aggGenreWTime_month_" + month
      
      aggGenreTime.map{x =>
       
       val duid:String  = x._1
       val genre:String = x._2
       val wtime:Int    = x._3
       val date:String  = x._4
       
       duid + "," + genre + "," + wtime + "," + date 
      }.coalesce(100).saveAsTextFile(aggGenreFileName)

    }

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

    //Logger.info("No. of weekly records: " + weeklyAggGenreTime.count())

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
   
    Logger.info("\nRunning weekly genre aggregation")

    //get spark context
    val sc = jobInfo.sc
    val dates:Array[String] = jobInfo.trainDates 
   
    Logger.info("No. of dates: " + dates.size)

    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)
    val wTimeMonthlyACRPaths:Map[Int, Array[(Int, (String, String))]] = getValidACRPaths(dates, watchTimeResLoc, sc)
  
    Logger.info("\nNo. of ACR paths: " + wTimeMonthlyACRPaths.size)

    //for each month save aggregate Genre
    wTimeMonthlyACRPaths.map{case (month, monthPaths) =>
      
      Logger.info("\nMonth: " + month)

      val wTimeACRPaths:Array[(String, String)] = monthPaths.map(_._2)
     
      //(duid, genre, watchtime, week, month)
      val weeklyAggGenreTime:RDD[(String, String, Int, Int, Int)] =
        getWeeklyAggGenreWTime(wTimeACRPaths, jobInfo)  

      //save aggregated time spent on genre to text file
      val aggGenreFileName:String =
        jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + "aggGenreWTimeWeekly_" + month
   
      Logger.info("Agg genre file: " + aggGenreFileName) 

      Logger.info("No. of weekly records: " + weeklyAggGenreTime.count())

      weeklyAggGenreTime.map{x =>
       
        val duid:String  = x._1
        val genre:String = x._2
        val wtime:Int    = x._3
        val week:Int     = x._4
        val month:Int    = x._5
        
        ((duid, week, month),(genre, wtime))
         
      }.groupByKey.map{x =>
        
        val duid:String = x._1._1
        val week:Int = x._1._2
        val month:Int = x._1._3
        val iterGenTime:Iterable[(String, Int)] = x._2
        val genreWTimes:String = iterGenTime.map{genreTime =>
          val genre:String = genreTime._1
          val wTime:Int = genreTime._2
          genre + "," + wTime
        }.mkString(",")

        duid + "," + week + "," + month + "," +genreWTimes
      }.saveAsTextFile(aggGenreFileName)
         
    }

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
