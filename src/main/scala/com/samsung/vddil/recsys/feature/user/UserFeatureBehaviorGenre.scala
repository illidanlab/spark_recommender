package com.samsung.vddil.recsys.feature.user

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.item.ItemFeatureGenre
import org.apache.spark.SparkContext._


object UserFeatureBehaviorGenre extends FeatureProcessingUnit{
	
	/*
	 * take item genre feature vector and watchtime
	 * will add feature vector weighted by watchtime and divide by sum watchtimes
	 * \sigma (watchtime*genreFeatures)/ \sigma (watchtime)
	 */
	def aggByItemGenres( userGenreWatchtimes : Iterable[(Array[Int], Int)]) : Array[Double] = {
		val first = userGenreWatchtimes.head
		val firstFeats = first._1
		val firstWatchtime = first._2
		val featLen = firstFeats.length
		//weight features of first item by its watchtime
		for (i <- 0 until featLen) {
			firstFeats(i) *= firstWatchtime 
		}
		var sumWatchTime = firstWatchtime
		//accumulate weighted features in firstFeats
		for (userGenreWatchtime <- userGenreWatchtimes.tail) {
			for (i <- 0 until featLen) {
				//accumulate sum of weighted feature vectors
				firstFeats(i) += userGenreWatchtime._1(i)*userGenreWatchtime._2
				//get sum of watch times
				sumWatchTime += userGenreWatchtime._2
			}
		}
		//divide all feature by sumWatchTime
		var weightedGenres = Array.fill[Double](featLen)(0)
		for (i <- 0 until featLen) {
			weightedGenres(i) = firstFeats(i).toDouble/sumWatchTime.toDouble
		}
		weightedGenres
	}  
  
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
		
		//get spark context
		val sc = jobInfo.sc
		
		// 1. Complete default parameters
		
		
	    // 2. Generate resource identity using resouceIdentity()
		var resourceIden = resourceIdentity(featureParams)
		
	    // 3. Feature generation algorithms (HDFS operations)
		
		//get item genres
		
		//parse ItemFeature hash to find genre resources
		var itemGenreFeatureFile = "" 
		var itemGenreFeatureMapFile = ""  
		jobInfo.jobStatus.resourceLocation_ItemFeature.keys.foreach { k =>
			if ( ItemFeatureGenre.checkIdentity(k) ) {
				//got the correct key
			  itemGenreFeatureFile = jobInfo.jobStatus.resourceLocation_ItemFeature(k)
			  itemGenreFeatureMapFile = jobInfo.jobStatus.resourceLocation_ItemFeatureMap(k)
			}
		}
		
		//read item genre features. item -> feature vector array
		val itemGenreFeatures = sc.textFile(itemGenreFeatureFile).map{line =>
		  	val fields = line.split(',')
		  	val item = fields(0)
		  	val genreFeats = fields.slice(1, fields.length).map(s => s.toInt)
		  	(item, genreFeats)
		}.collect.toMap
		
		//get all merged data
		val userGenreFeatures = sc.textFile(jobInfo.jobStatus.resourceLocation_CombineData)
			   .map {line =>
			     	val fields = line.split(',')
			     	val user = fields(0)
			   		val item = fields(1)
			   		val watchTime = fields(2).toInt
			   		(user, item, watchTime)
			    }
			   .filter(x => itemGenreFeatures.contains(x._2) ) //filter out items whose genre information is not available
			   .map { x =>
			   			//user,item, watchTime
			   			val user = x._1
			   			val item = x._2
			   			val watchTime = x._3
			   			(user, (itemGenreFeatures(item), watchTime)) // get the feature vector of genre of item
			   	}
			   .groupByKey() //group by user id
			   .map {x => x._1 + "," + aggByItemGenres(x._2).mkString(",")} //get weighted feature vector
			  
		
		//save user features in text file
		var featureFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + resourceIden
        Logger.logger.info("Dumping feature resource: " + featureFileName)
        userGenreFeatures.saveAsTextFile(featureFileName)
						   
	    // 4. Generate and return a FeatureResource that includes all resources.  
        val resourceMap:HashMap[String, Any] = new HashMap()
		resourceMap(FeatureResource.ResourceStr_UserFeature) = featureFileName
		resourceMap(FeatureResource.ResourceStr_UserFeatureMap) = itemGenreFeatureMapFile
		
		Logger.info("Saved user features and feature map")
		
		new FeatureResource(true, resourceMap, resourceIden)
	}
	
	val IdenPrefix:String = "UserFeatureGenre"
    
}