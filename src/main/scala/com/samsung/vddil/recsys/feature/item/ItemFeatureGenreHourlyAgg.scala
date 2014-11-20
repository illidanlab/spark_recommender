package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.job.TemporalAggJob
import com.samsung.vddil.recsys.utils.Logger
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util.Calendar



object ItemFeatureGenreHourlyAgg {

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

  def getValidSchedulePaths(dates:Array[String],
    jobInfo:TemporalAggJob):Array[(String, String)] = {
     
    val sc = jobInfo.sc

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
  
    val filtDates:Array[String] = dates
    //val filtDates:Array[String] = weekends
    //val filtDates:Array[String] = workdays

    val hadoopConf = sc.hadoopConfiguration

    val valSchedPaths = filtDates.map{date =>
      (date, jobInfo.resourceLoc(TemporalAggJob.ResourceLoc_SchedWTime) + date)
    }.filter(x => {
      val path = new Path(x._2)
      val fileSystem = path.getFileSystem(hadoopConf)
      fileSystem.exists(path)
    }) 

    valSchedPaths
  }

  
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
    //val filtDates:Array[String] = weekends
    val filtDates:Array[String] = workdays

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

  
  //return RDD of item and Genrefor passed date 
  def getItemGenre(date:String, items:RDD[String], 
    jobInfo:TemporalAggJob):RDD[(String, String)] = {
    
    //spark context
    val sc:SparkContext = jobInfo.sc

    val featSrc:String = jobInfo.resourceLoc(TemporalAggJob.ResourceLoc_RoviHQ) + date + "/program_genre*"
    //TODO: verify toSet
    val bItemSet = sc.broadcast(items.collect.toSet)
    
    
    val param_GenreLang:String = GenreLangFilt
    val genreSource = jobInfo.resourceLoc(TemporalAggJob.ResourceLoc_RoviHQ) + date + "/genre*" 
    
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
 

  //get duids which are active everyday
  def getRegularDuids(schedPaths:Array[(String, String)], 
    sc:SparkContext):RDD[String] = {
      val regDuids:RDD[String] = schedPaths.map{dateNSchedPath =>
        val date                          = dateNSchedPath._1
        val schedPath                     = dateNSchedPath._2
        val dailyDuids:RDD[(String, Int)] = sc.textFile(schedPath).map{line =>
          val fields = line.split('\t')
          (fields(0), 1)
        }.distinct
        dailyDuids
      }.reduce((a,b) => a.join(b).map(x => (x._1, 1))).map(_._1)
      regDuids
  }


  //duid, hour_slot, genre, time    
  def getHourlyAggGenreWTime(schedPaths:Array[(String, String)],
    jobInfo:TemporalAggJob):RDD[(String, Int, String, Int)] = {
    
    //spark context
    val sc = jobInfo.sc
    
    //regular duids
    val regDuids:RDD[String] = getRegularDuids(schedPaths, sc)

    schedPaths.map{dateNSchedPath =>
      val date          = dateNSchedPath._1
      val schedPath     = dateNSchedPath._2
      //get time slot for item with watchtime for duids
      //(duid, timeSlot, item, watchtime)
      val duidSchedWtimes:RDD[(String, Int, String, Int)] = sc.textFile(schedPath).map{line =>
        val fields           = line.split('\t')
        val duid:String      = fields(0)
        val item:String      = fields(1)
        val wt:Int           = fields(4).toInt
        val timestamp:String = fields(2)
        val hr:Int           = timestamp.split("T")(1).split(":")(0).toInt
        (duid, hr, item, wt) 
      }.filter(_._4 >= Min_Wtime)

      //get genres for items (item, genre)
      val items:RDD[String] = duidSchedWtimes.map(_._3) 
      val itemGenre:RDD[(String, String)] = getItemGenre(date, items, jobInfo)
      val itemGroupedGenre:RDD[(String, Iterable[String])] =
        itemGenre.groupByKey()
    
      //add genres for items
      val hourlyGenresWtimes:RDD[(String, 
        ((String, Int, Int),(Iterable[String])))] = duidSchedWtimes.map{ x =>
        //(item, (duid, hr, wt))
        (x._3, (x._1, x._2, x._4))
      }.join(itemGroupedGenre)


      //get 'duid, hr, genre, wt'
      val duidHourlyGenres:RDD[((String, Int, String), Int)] =
        hourlyGenresWtimes.map{x =>
          val item:String             = x._1
          val duid:String             = x._2._1._1
          val hr:Int                  = x._2._1._2
          val wt:Int                  = x._2._1._3
          val genres:Iterable[String] = x._2._2
          ((duid, hr), (genres, wt))
        }.flatMapValues{x =>
          val genres:Iterable[String] = x._1
          val wTime:Int               = x._2
          genres.map{genre => 
            (genre, wTime)
          }
        }.map{x =>
          val duid:String  = x._1._1
          val hr:Int       = x._1._2
          val genre:String = x._2._1
          val wTime:Int    = x._2._2
          ((duid, hr, genre), wTime)
        }
      
      duidHourlyGenres
    }.reduce((a,b) => a.union(b)).reduceByKey((a:Int, b:Int) => a+b).map{x =>
      val duid:String  = x._1._1
      val hr:Int       = x._1._2
      val genre:String = x._1._3
      val wTime:Int    = x._2
      (duid, (hr, genre, wTime))
    }.join(regDuids.map(x => (x,1))).map{x =>
      val duid:String  = x._1
      val hr:Int       = x._2._1._1
      val genre:String = x._2._1._2
      val wTime:Int    = x._2._1._3
      (duid, hr, genre, wTime)
    }
    
  }


  def saveAggGenreHourly(jobInfo:TemporalAggJob) = {
    
    //spark context
    val sc = jobInfo.sc
    val dates:Array[String] = jobInfo.trainDates
    val schedPaths:Array[(String, String)] = getValidSchedulePaths(dates,
      jobInfo)
    
    //get hourly genre aggregation for duids
    val hourlyAggGenreWTime:RDD[(String, Int, String, Int)] =
      getHourlyAggGenreWTime(schedPaths, jobInfo)

    val hourlyAggGenreFileName:String =
      jobInfo.resourceLoc(TemporalAggJob.ResourceLoc_JobFeature) + "/" + "aggGenreWTimeHourly"

    hourlyAggGenreWTime.map{x =>
      val duid:String  = x._1
      val hr:Int       = x._2
      val genre:String = x._3
      val wTime:Int    = x._4
      ((duid, hr), (genre, wTime))
    }.groupByKey.map{x =>
      val duid:String  = x._1._1
      val hr:Int       = x._1._2
      val iterGenTime:Iterable[(String, Int)] = x._2
      val genreWTimes:String = iterGenTime.map{genreTime =>
        val genre:String = genreTime._1
        val wTime:Int = genreTime._2
        genre + "," + wTime
      }.mkString(",")
      duid + "," + hr + "," + genreWTimes 
    }.saveAsTextFile(hourlyAggGenreFileName)

  }


}
