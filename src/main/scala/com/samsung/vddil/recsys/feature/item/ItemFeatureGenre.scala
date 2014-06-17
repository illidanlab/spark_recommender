package com.samsung.vddil.recsys.feature.item

import scala.collection.mutable.HashMap
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString


object ItemFeatureGenre  extends FeatureProcessingUnit{
  
    val ITEM_SUB_GENRE_IND = 2
    val ITEM_GENRE_IND = 3
    val ITEM_ID_IND = 1
    
    val GENRE_ID_IND = 1
    val GENRE_LANG_IND = 2
    val GENRE_DESC_IND = 3
    val GENRE_LANG_FILT = "en" //default language 
      
    val Param_GenreLang = "lang" 
    
    def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
        
        val sc = jobInfo.sc
        
        // 1. Complete default parameters
        //  default parameter for genre: lang filtering. 
        var param_GenreLang:String = GENRE_LANG_FILT
        if(featureParams.isDefinedAt(Param_GenreLang)){
            param_GenreLang = featureParams(Param_GenreLang)
        }
          
        // 2. Generate resource identity using resouceIdentity()
        var resourceIden = resourceIdentity(featureParams)
        
        // 3. Feature generation algorithms (HDFS operations)
        
        //get set of items
        val itemSet = jobInfo.jobStatus.items.toSet
        
        
        //get RDDs of genres only for param_GenreLang if exists
        var fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + jobInfo.trainDates(0) + "/genre*" 
        var genreMap = sc.textFile(fileName).map { line =>
          val fields = line.split('|')
          (fields(GENRE_ID_IND), fields(GENRE_LANG_IND), fields(GENRE_DESC_IND))
        }.filter(x => x._2 == param_GenreLang)        
        
        for (trainDate <- jobInfo.trainDates.tail) {
            fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/genre*"
            val nextGenreMap = sc.textFile(fileName).map { line =>
                val fields = line.split('|')
                (fields(GENRE_ID_IND), fields(GENRE_LANG_IND), fields(GENRE_DESC_IND))
            }.filter(x => x._2 == param_GenreLang)
            genreMap = genreMap.union(nextGenreMap).distinct
        }  
        
        val genresWDesc = genreMap.collect
        val genreKeys = genresWDesc.map(x => x._1)
        val genreDesc = genresWDesc.map(x => x._3)
        val numGenres = genreKeys.length
        
        //create a map: index ->  (genreId or genreKey, description) 
        val genreInd2KeyDesc = ((0 until genreKeys.length) zip (genreKeys zip genreDesc)) toMap
        val genreInd2KeyDescRDD = sc.makeRDD(genreInd2KeyDesc.toList)
        
        //create reverse map for lookup 
        val genre2Ind = (genreKeys zip (0 until genreKeys.length)) toMap
        
        
        //get RDDs of items itemGenreList: itemId, subgenre, Genre        
        fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + jobInfo.trainDates(0) + "/program_genre*"
        var itemGenreList = sc.textFile(fileName).map { line =>
              val fields = line.split('|')
              (fields(ITEM_ID_IND), (genre2Ind(fields(ITEM_SUB_GENRE_IND)), 
                  genre2Ind(fields(ITEM_GENRE_IND))) )             
            }.filter(x => itemSet(x._1) )
        
        //parse program genre file for each date and find genres for items in train
        for (trainDate <- jobInfo.trainDates.tail) {
            fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/program_genre*"
            val nextGenre = sc.textFile(fileName).map { line =>
              val fields = line.split('|')
              (fields(ITEM_ID_IND), (genre2Ind(fields(ITEM_SUB_GENRE_IND)), 
                  genre2Ind(fields(ITEM_GENRE_IND))) )             
            }.filter(x => itemSet(x._1) )
            itemGenreList = itemGenreList.union(nextGenre).distinct
        }  
        
        //generate feature vector for each items    
        val itemGenreInds = itemGenreList.groupByKey().map { x =>
          var itemId = x._1
          var featureVec = Array.fill[Byte](numGenres)(0) //initialize all to 0
          for ((subGenreInd, genreInd) <- x._2) {
              featureVec(subGenreInd) = 1
              featureVec(genreInd) = 1
          }
        }
        
        //save item features as textfile
        var featureFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/"+ resourceIden
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/"+ resourceIden + "Map"
        Logger.logger.info("Dumping feature resource: " + featureFileName)
        itemGenreInds.saveAsTextFile(featureFileName)
        Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)
        genreInd2KeyDescRDD.saveAsTextFile(featureMapFileName)
     
        // 4. Generate and return a FeatureResource that includes all resources.  
        val resourceMap:HashMap[String, Any] = new HashMap()
        resourceMap(FeatureResource.ResourceStr_UserFeature) = featureFileName
        resourceMap(FeatureResource.ResourceStr_UserFeatureMap) = featureMapFileName
        
        new FeatureResource(true, resourceMap, resourceIden)
    }
    
    def resourceIdentity(featureParam:HashMap[String, String]):String = {
        "ItemFeatureGenre_" + HashString.generateHash(featureParam.toString)
    }
}