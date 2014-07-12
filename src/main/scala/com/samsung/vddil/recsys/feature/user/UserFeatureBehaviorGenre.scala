package com.samsung.vddil.recsys.feature.user

import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext._

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.item.ItemFeatureGenre
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.linalg.{Vector,Vectors,SparseVector}


object UserFeatureBehaviorGenre extends FeatureProcessingUnit{
	
	/*
	 * take item genre feature vector and watchtime
	 * will add feature vector weighted by watchtime and divide by sum watchtimes
	 * \sigma (watchtime*genreFeatures)/ \sigma (watchtime)
	 * 
	 * @deprecated
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
	
	/*
	 * this is the vector version of aggByItemGenres
	 * 
	 * This function is tested in the com.samsung.vddil.recsys.linalg.testVector
	 */
	def aggByItemGenres( userGenreWatchtimes: Iterable[(Vector, Double)]) : Vector = {
	    require(userGenreWatchtimes.size > 0)
	    
	    val firstWatchtime = userGenreWatchtimes.head._1
	    val initVector:Vector = Vectors.dense(firstWatchtime.size)
	    
		val (sumVec, sumWt) = 
		  userGenreWatchtimes.foldLeft((initVector, 0.0))( 
		      (gw1, gw2) => (gw1._1 + gw2._1.mapValues(_ *  gw2._2), gw1._2 + gw2._2) )
		
		//only divide non-zero values.
		val result = Vectors.fromBreeze(sumVec.data.mapActiveValues( t => t/sumWt.toDouble)) 
		
		firstWatchtime match {
	      case v:SparseVector => result.toSparse()
	      case _ => result
	    }
	    
	}
  
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		
		//get spark context
		val sc = jobInfo.sc
		
		// 1. Complete default parameters
		
		
	    // 2. Generate resource identity using resouceIdentity()
		val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
		var resourceIden = resourceIdentity(featureParams,dataHashingStr)
		var featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
								    "/" + resourceIden
		var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
								    "/" + resourceIden + "_Map"
		
	    // 3. Feature generation algorithms (HDFS operations)
		
		//get item genres
		
		//parse ItemFeature hash to find genre resources
		var itemGenreFeatureFile:Option[String] = None 
		var itemGenreFeatureMapFile:Option[String] = None  
		jobInfo.jobStatus.resourceLocation_ItemFeature.keys.foreach { k =>
			if ( ItemFeatureGenre.checkIdentity(k) ) {
				//got the correct key
			  itemGenreFeatureFile = Some(jobInfo.jobStatus.resourceLocation_ItemFeature(k).featureFileName)
			  itemGenreFeatureMapFile = Some(jobInfo.jobStatus.resourceLocation_ItemFeature(k).featureMapFileName)
			}			 
		}
		
		if (!itemGenreFeatureFile.isDefined){
		   throw new Exception("ERROR: Dependent item feature not ready")
		   //TODO: if not found we need to generate it!
		}
		
//		//read item genre features. item -> feature vector array
//		val itemGenreFeatures = sc.textFile(itemGenreFeatureFile.get).map{line =>
//		  	val fields = line.split(',')
//		  	val item = fields(0)
//		  	val genreFeats = fields.slice(1, fields.length).map(s => s.toInt)
//		  	(item, genreFeats)
//		}.collect.toMap
		
		//read item genre features. item -> feature vector array
		val itemGenreFeatures = sc.objectFile[(String, Vector)](itemGenreFeatureFile.get).collect.toMap
		
//		//get all merged data
//		val userGenreFeatures = sc.textFile(jobInfo.jobStatus.resourceLocation_CombineData)
//			   .map {line =>
//			     	val fields = line.split(',')
//			     	Rating(fields(0), fields(1), fields(2).toDouble)
//			    }
//			   .filter(rating => itemGenreFeatures.contains(rating.item) ) //filter out items whose genre information is not available
//			   .map { rating =>
//			   			//user,item, watchTime
//			   			(rating.user, (itemGenreFeatures(rating.item), rating.rating.toInt)) // get the feature vector of genre of item
//			   	}
//			   .groupByKey() //group by user id
//			   .map {x => x._1 + "," + aggByItemGenres(x._2).mkString(",")} //get weighted feature vector

		//get all merged data
		//userGenreFeatures := RDD[(String, Vector)]
		val userGenreFeatures = sc.textFile(jobInfo.jobStatus.resourceLocation_CombineData)
			   .map {line =>
			     	val fields = line.split(',')
			     	Rating(fields(0), fields(1), fields(2).toDouble)
			    }
			   .filter(rating => itemGenreFeatures.contains(rating.item) ) //filter out items whose genre information is not available
			   .map { rating =>
			   			//user,item, watchTime
			   			(rating.user, (itemGenreFeatures(rating.item), rating.rating)) // get the feature vector of genre of item
			   	}
			   .groupByKey() //group by user id
			   .map {x => (x._1 , aggByItemGenres(x._2))} //get weighted feature vector		
		
		//save user features in text file
		if(jobInfo.outputResource(featureFileName)){
			Logger.logger.info("Dumping feature resource: " + featureFileName)
			//userGenreFeatures.saveAsTextFile(featureFileName)
			userGenreFeatures.saveAsObjectFile(featureFileName)
		}
        
	    // 4. Generate and return a FeatureResource that includes all resources.
        val featureStruct:UserFeatureStruct = 
          	new UserFeatureStruct(IdenPrefix, resourceIden, featureFileName, itemGenreFeatureMapFile.get)
        val resourceMap:HashMap[String, Any] = new HashMap()
		resourceMap(FeatureResource.ResourceStr_UserFeature) = featureStruct
		
		Logger.info("Saved user features and feature map")
		
		new FeatureResource(true, Some(resourceMap), resourceIden)
	}
	
	val IdenPrefix:String = "UserFeatureGenre"
    
}