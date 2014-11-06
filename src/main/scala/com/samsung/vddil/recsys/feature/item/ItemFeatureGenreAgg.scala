package com.samsung.vddil.recsys.feature.item

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
    dates:List[String],
    pathPrefix: String,
    jobInfo:RecJob,
    sc: SparkContext):RDD[(String, String, Double, String)] = {
    
    //for each date 
    dates.map{ date =>
      //get all watchtimes for duids with items
      //TODO: confirm if following is unique
      val wTime:RDD[(String, String, Double)] = sc.textFile(pathPrefix +
        date).map {line =>
          val fields = line.split('\t')
          //duid, item, watchtime
          (fields(0), fields(1), fields(2).toDouble)
        }
      
      //get genres for the items (item, genre)
      val items:RDD[String] = wTime.map(_._2)
      val itemGenre:RDD[(String, String)] = getItemGenre(date, items, jobInfo)
      val itemGroupedGenre:RDD[(String, Iterable[String])] =
        itemGenre.groupByKey()
      
      //get 'duid, genre, watchtime'
      val duidGenres:RDD[(String, String, Double, String)] = wTime.map{ x =>
        //item, (duid, wtime)
        (x._1, (x._2, x._3))
      }.join(
        itemGroupedGenre
      ).map{x => //item, ((duid,wtime), Iterable[Genre])
        
        val duid:String             = x._2._1._1
        val wtime:Double            = x._2._1._2
        val genres:Iterable[String] = x._2._2
        
        (duid, (genres, wTime)) 
      }.flatMapValues{ genreNWtime =>
          val genres:Iterable[String] = genreNWtime._1
          val wtime:Double = genreNWtime._2
          genres.map{genre =>
            (genre, wtime)
          }
      }.map{x =>
        val duid:String = x._1
        val wtime:Double = x._2._2
        val genre:String = x._2._1
        (duid, genre, wtime, date)
      }
      
      duidGenres      
    }.reduce((a,b) => a.union(b))
  }

  //return RDD of item and Genrefor passed date 
  def getItemGenre(date:String, items:RDD[String], 
    jobInfo:RecJob):RDD[(String, String)] = {

    val featSrc:String = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + date 
      + "/program_genre"
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
  

}
