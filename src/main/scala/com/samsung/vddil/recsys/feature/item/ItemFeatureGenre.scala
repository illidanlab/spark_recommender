package com.samsung.vddil.recsys.feature.item

import scala.collection.mutable.HashMap
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct


object ItemFeatureGenre  extends FeatureProcessingUnit{
  
    val ItemGenreInd = 2
    val ItemIdInd = 1
    
    val GenreIdInd = 1
    val GenreLangInd = 2
    val GenreDescInd = 3
    val GenreLangFilt = "en" //default language 
      
    val Param_GenreLang = "lang" 
    
    def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
    		
    	//get spark context
        val sc = jobInfo.sc
        
        // 1. Complete default parameters
        //  default parameter for genre: lang filtering. 
        var param_GenreLang:String = GenreLangFilt
        if(featureParams.isDefinedAt(Param_GenreLang)){
            param_GenreLang = featureParams(Param_GenreLang)
        }
          
        // 2. Generate resource identity using resouceIdentity()
        val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
        val resourceIden = resourceIdentity(featureParams, dataHashingStr)
        
        val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden + "_Map"
        
        // 3. Feature generation algorithms (HDFS operations)
        
        //get set of items
        val itemSet = jobInfo.jobStatus.items.toSet
        
        
        //get RDDs of genres only for param_GenreLang if exists
        var fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + jobInfo.trainDates(0) + "/genre*" 
        var genreMap = sc.textFile(fileName).map { line =>
          val fields = line.split('|')
          (fields(GenreIdInd), fields(GenreLangInd), fields(GenreDescInd))
        }.filter(x => x._2 == param_GenreLang)        
        
        for (trainDate <- jobInfo.trainDates.tail) {
            fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/genre*"
            val nextGenreMap = sc.textFile(fileName).map { line =>
                val fields = line.split('|')
                (fields(GenreIdInd), fields(GenreLangInd), fields(GenreDescInd))
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
        
        Logger.info("created genres to index map, numGenres: " + numGenres 
                    + " numItems: " + itemSet.size)
        
        
        
        //get RDDs of items itemGenreList: itemId, subgenre, Genre   
        //filter only those for which genre is already found
        fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + jobInfo.trainDates(0) + "/program_genre*"
        var itemGenreList = sc.textFile(fileName).map { line =>
              val fields = line.split('|')
              (fields(ItemIdInd),  fields(ItemGenreInd))             
            }.filter(x => itemSet(x._1) && genre2Ind.contains(x._2)) 
         
        Logger.info("Creating item genrelist")
        
        //parse program genre file for each date and find genres for items in train
        for (trainDate <- jobInfo.trainDates.tail) {
            fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/program_genre*"
            val nextGenre = sc.textFile(fileName).map { line =>
              val fields = line.split('|')
              (fields(ItemIdInd), fields(ItemGenreInd))              
            }.filter(x => itemSet(x._1) && genre2Ind.contains(x._2))
            itemGenreList = itemGenreList.union(nextGenre).distinct
        }  
       
        Logger.info("created itemGenres list")
            
        //generate feature vector for each items    
        val itemGenreInds = itemGenreList.groupByKey().map { x =>
          var itemId = x._1
          var featureVec = Array.fill[Byte](numGenres)(0) //initialize all to 0
          for (genre <- x._2) {
              featureVec(genre2Ind(genre)) = 1
          }
          itemId + "," + featureVec.mkString(",")
        }
        
        Logger.info("created item genre feature vectors")
        
        //save item features as textfile
        if (jobInfo.outputResource(featureFileName)){
        	Logger.logger.info("Dumping feature resource: " + featureFileName)
        	itemGenreInds.saveAsTextFile(featureFileName)
        }
        
        //save genre mapping to indexes
        if (jobInfo.outputResource(featureFileName)){
        	Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)
        	genreInd2KeyDescRDD.saveAsTextFile(featureMapFileName)
        }
        
        val featureStruct:ItemFeatureStruct = 
          	new ItemFeatureStruct(IdenPrefix, resourceIden, featureFileName, featureMapFileName)
        // 4. Generate and return a FeatureResource that includes all resources.  
        val resourceMap:HashMap[String, Any] = new HashMap()
        resourceMap(FeatureResource.ResourceStr_ItemFeature) = featureStruct
        
        Logger.info("Saved item features and feature map")
        
        new FeatureResource(true, Some(resourceMap), resourceIden)
    }
    
    
    val IdenPrefix:String = "ItemFeatureGenre"
}