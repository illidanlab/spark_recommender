package com.samsung.vddil.recsys.feature.user

import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.{Vector,Vectors,SparseVector}
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.feature.process.FeaturePostProcessor
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import com.samsung.vddil.recsys.feature.process.FeaturePostProcessor
import com.samsung.vddil.recsys.job.JobWithFeature

trait UserFeatureItemWtAgg extends Serializable {
 
  /*
	 * take item genre feature vector and watchtime
	 * will add feature vector weighted by watchtime and divide by sum watchtimes
	 * \sigma (watchtime*genreFeatures)/ \sigma (watchtime)
   */
  def aggByItemFeature(userFeatureWatchtimes: Iterable[(Vector, Double)]) :
  Vector = {
    require(userFeatureWatchtimes.size > 0)
    val firstWatchtime = userFeatureWatchtimes.head._1
    val initVector:Vector = Vectors.dense(firstWatchtime.size)
	    
		val (sumVec, sumWt) = 
		  userFeatureWatchtimes.foldLeft((initVector, 0.0))( 
		      (gw1, gw2) => (gw1._1 + gw2._1.mapValues(_ *  gw2._2), gw1._2 + gw2._2) )
		
		//only divide non-zero values.
		val result = Vectors.fromBreeze(sumVec.data.mapActiveValues( t => t/sumWt.toDouble)) 
	
    //as we don't know the type use pattern match to know type and return in
    //desired form
		firstWatchtime match {
	      case v:SparseVector => result.toSparse()
	      case _ => result
	    }
  }


  /* take all item features and rating data
   * return all aggregated feature by user for all items
   */
  def getAllUserFeatures(itemFeatureFileName:String, ratingDataFileName:String,
    sc:SparkContext): RDD[(Int, Vector)] = {
    
    //read item features in (item, featureVector array)
    val itemFeatures = sc.objectFile[(Int,Vector)](itemFeatureFileName).collect.toMap   
    
    //broad cast the small item map
    val bIFeatMap = sc.broadcast(itemFeatures)    

    //get all user rating data
    val userRatings = sc.textFile(ratingDataFileName
                        ).map{line =>
                          val fields = line.split(',')
                          Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
                        }

    //get ratings after removing extra item
    val userRatingsRemItems = userRatings.filter(rating => bIFeatMap.value.contains(rating.item))

    //get corresponding item feature vector and watchtime
    val userItemFeats = userRatingsRemItems.map(rating =>
        (rating.user, (bIFeatMap.value(rating.item), rating.rating)))

    //group by user, to get all preferred item and features 
    val userGroupedItemFeats = userItemFeats.groupByKey(1000)

    //get weighted aggregation of item features per user
    val aggFeatureFunc:(Iterable[(Vector, Double)]) => Vector = this.aggByItemFeature
    userGroupedItemFeats.map{x => 
      val user:Int = x._1
      val aggFeature:Vector = aggFeatureFunc(x._2)
      (user, aggFeature)
    }
  }

  def generateFeature(
          featureParams:HashMap[String, String], 
          jobInfo:JobWithFeature, 
          checkIdentity: (String) => Boolean, featureFilePath:String,
          idenPrefix:String, 
          resourceIden:String
	):FeatureResource = {
   	
		//get spark context
		val sc = jobInfo.sc
		
		// 1. Complete default parameters
		
		
    // 2. Generate resource identity using resouceIdentity()
		val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
		
    // 3. Feature generation algorithms (HDFS operations)
		
		//get item features 
		
		//parse ItemFeature hash to find desire feature resources
		var itemFeatureFile:Option[String] = None 
		var itemFeatureMapFile:Option[String] = None  
		jobInfo.jobStatus.resourceLocation_ItemFeature.keys.foreach { k =>
			if ( checkIdentity(k) ) {
				//got the correct key
			  itemFeatureFile = Some(jobInfo.jobStatus.resourceLocation_ItemFeature(k).featureFileName)
			  itemFeatureMapFile = Some(jobInfo.jobStatus.resourceLocation_ItemFeature(k).featureMapFileName)
			}			 
		}

		
	//TODO: add feature selection. 
	val	featurePostProcessor:List[FeaturePostProcessor] = List()
	
    itemFeatureFile match {
      case None => 
           Logger.error("ERROR: Dependent item feature not ready")
           return FeatureResource.fail
           
      case Some(itemFeatureFileName) => {
        val ratingDataFileName = jobInfo.jobStatus.resourceLocation_CombinedData_train.get.resourceLoc
        val userFeatures:RDD[(Int, Vector)] =
          getAllUserFeatures(itemFeatureFileName, ratingDataFileName, sc)
        //save generated userFeatures at specified file path
        if(jobInfo.outputResource(featureFilePath)) {
            Logger.info("Dumping feature resource: " + featureFilePath)
            userFeatures.coalesce(jobInfo.partitionNum_unit * 2).saveAsObjectFile(featureFilePath)
        }
      }
    }

    val featureSize:Int = sc.objectFile[(Int,Vector)](itemFeatureFile.get).first._2.size
    
    // 4. Generate and return a FeatureResource that includes all resources.
    val featureStruct:UserFeatureStruct = 
          	new UserFeatureStruct(
          	        idenPrefix, resourceIden, featureFilePath, 
          	        itemFeatureMapFile.get, featureParams, 
          	        featureSize, featureSize, 
          	        featurePostProcessor)
    
    val resourceMap:HashMap[String, Any] = new HashMap()
		resourceMap(FeatureResource.ResourceStr_UserFeature) = featureStruct
		resourceMap(FeatureResource.ResourceStr_FeatureDim)  = featureSize
		Logger.info("Saved user features and feature map")
		new FeatureResource(true, Some(resourceMap), resourceIden)
  } 


}
