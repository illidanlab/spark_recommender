package com.samsung.vddil.recsys.testing

import com.samsung.vddil.recsys.data.DataProcess
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.model.PartializableModel
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.mllib.linalg.{Vectors => SVs, Vector => SV}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.ItemFeatureStruct

object TestResourceRegItemColdHit{
  
  val IdenPrefix = "RegItemColdHit"
  
  /**
   * In case the partial models are used, this parameter 
   * determines how many blocks we divide. A smaller number 
   * can accelerate the batch computing performance, but may 
   * cause memory issues. 
   */
  val partialModelBatchNum = 10


  /**
   * perform test on cold items i.e. for each user predict his preference on new
   * items then compute recall on his actual preference of new items
   * @param jobInfo
   * @param testName
   * @param testParams
   * @param metricParams
   * @param model
   * @return RDD of user, his topN predicted cold items and size of intersection
   * with actual preferred cold items
   */
  def generateResource(jobInfo:RecJob,
    testParams:HashMap[String, String], 
    model: ModelStruct, 
    testResourceDir:String):
    RDD[(Int, (List[String], Int))]  = {

    //get the value of "N" in Top-N from parameters
    val N:Int = testParams.getOrElseUpdate("N", "10").toInt
    
    //get percentage of user sample to predict on as it takes really long to
    //compute on all users
    val userSampleParam:Double = testParams.getOrElseUpdate("UserSampleSize",
                                                  "0.2").toDouble
    Logger.info("User sample parameter: " + userSampleParam)
    
    //seed parameter needed for sampling test users
    val seed = testParams.getOrElseUpdate("seed", "3").toInt


    //get spark context
    val sc = jobInfo.sc
    
    //cache intermediate files, helpful in case of crash  
    val itemFeatObjFile         = testResourceDir + "/" + IdenPrefix + "/itemFeat"   
    val userFeatObjFile         = testResourceDir + "/" + IdenPrefix + "/userFeat" 
    val sampledUserFeatObjFile  = testResourceDir + "/" + IdenPrefix + "/sampledUserFeat" 
    val sampledItemUserFeatFile = testResourceDir + "/" + IdenPrefix + "/sampledUserItemFeat"
    val sampledPredBlockFiles   = testResourceDir + "/" + IdenPrefix + "/sampledPred/BlockFiles"
   
    //get test dates
    val testDates = jobInfo.testDates
    //get test data from test dates
    val testData:RDD[(String, String, Double)] = DataProcess.getDataFromDates(testDates, 
      jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), sc).get
    //get training items
    val trainItems:Set[String] = jobInfo.jobStatus.itemIdMap.keySet
    //get cold items not seen in training
    val coldItems:Set[String] = getColdItems(testData, trainItems, sc)
    Logger.info("Cold items not seen in training: " + coldItems.size) 
   
    //get features for cold items 
         
    //get feature orderings
    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder.map{feature => feature.asInstanceOf[ItemFeatureStruct]}
      
    //get cold item features
    Logger.info("Preparing item features..")
    if (jobInfo.outputResource(itemFeatObjFile)) {
      val coldItemFeatures:RDD[(String, Vector)] = getColdItemFeatures(coldItems,
        jobInfo, itemFeatureOrder, testDates.toList)
      coldItemFeatures.saveAsObjectFile(itemFeatObjFile)
    }
    val coldItemFeatures:RDD[(String, Vector)] = sc.objectFile[(String, Vector)](itemFeatObjFile)

    //cold items with all features
    val finalColdItems:Set[String] = coldItemFeatures.map(_._1).collect.toSet
  
    Logger.info("Number of cold items: " + finalColdItems.size)

    //broadcast cold items
    val bColdItems = sc.broadcast(finalColdItems)

    //users which preferred cold items
    val preferredUsers:RDD[String] = testData.filter(x =>
        bColdItems.value(x._2)).map(_._1)
    
    //get users in training
    val trainUsers:RDD[String] = sc.parallelize(jobInfo.jobStatus.userIdMap.keys.toList)
    
    //filter out training users
    val allColdUsers:RDD[String] = preferredUsers.intersection(trainUsers)
    val allColdUsersCount = allColdUsers.count

    //If userSampleParam is larger than 1 we treat them as real counts
    //or else we treat them as percentage. 
    val userSamplePc:Double = 
        if (userSampleParam > 1)  userSampleParam/allColdUsersCount.toDouble 
        else userSampleParam 
    Logger.info("Adjusted sample ratio: " + userSamplePc)
    
    val withReplacement = false
    val sampledColdUsers:RDD[String] = allColdUsers.sample(withReplacement, userSamplePc, seed)
    Logger.info("The total sampled user number: " + sampledColdUsers.count)


    //replace coldItemUsers from training with corresponding int id
    val userIdMap:Map[String, Int] = jobInfo.jobStatus.userIdMap
    val userMapRDD = sc.parallelize(userIdMap.toList)
    val coldItemUsers:RDD[Int] = sampledColdUsers.map(x =>
        (x,1)).join(userMapRDD).map{_._2._2}
    val coldItemUsersCount = coldItemUsers.count
    

    //replace userId in test with intId and contain only cold items
    val repTestData:RDD[(Int, (String, Double))] = testData.filter{x =>
      val item = x._2
      bColdItems.value(item)
    }.map{x =>
      val user = x._1
      val item = x._2
      val rating = x._3
      (user, (item, rating))
      }.join(sampledColdUsers.map(x => (x,1))).mapValues{v => 
        //above join to filter only users for cold items
        val itemRating = v._1
        itemRating
      }.join(userMapRDD).map{ x =>
      val oldUser:String = x._1
      val item:String = x._2._1._1
      val rating:Double = x._2._1._2
      val newUser:Int = x._2._2
      (newUser, (item, rating))
    }


    //get user features
    Logger.info("Preparing user features...")
    if (jobInfo.outputResource(userFeatObjFile)){
      val userFeatures:RDD[(Int, Vector)] = getOrderedFeatures(coldItemUsers, userFeatureOrder, sc)
      userFeatures.saveAsObjectFile(userFeatObjFile)
    }
    val userFeatures:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)
    Logger.info("No. of users of cold items: " + userFeatures.count)


    val userItemPred:RDD[(Int, (String, Double))] = if (model.isInstanceOf[PartializableModel]){
        //if the model is a partializable model, then we use partial model 
        //and apply the models in batch. 
        
        Logger.info("Generating item enclosed partial models")
        var itemPartialModels = coldItemFeatures.map{x=>
            val itemId:String = x._1
            val partialModel = model.asInstanceOf[PartializableModel].applyItemFeature(x._2)
            (itemId, partialModel)
        }.coalesce(partialModelBatchNum)
        
        val predBlockSize = itemPartialModels.partitions.size
        val blockPredFiles = new Array[String](predBlockSize)
        Logger.info("Item enclosed partial models are divdided into " + predBlockSize + " blocks.")
        
        Logger.info("Preceed with partial models")
        for ((partialModelBlock, blockId) <- itemPartialModels.partitions.zipWithIndex){
            val idx = partialModelBlock.index
            val blockRdd = itemPartialModels.mapPartitionsWithIndex(
                    (ind, x) => if (ind == idx) x else Iterator(), true)
            
            //collect the models by block 
            var blockItemPartialModels = blockRdd.collect()
        	Logger.info("Broadcast block [ " + blockId + ":" + idx + "] with size:" + blockItemPartialModels.size)
        	
        	//block file location. 
        	blockPredFiles(idx) = sampledPredBlockFiles + "_" + idx
        	
        	if (jobInfo.outputResource(blockPredFiles(idx))){
		        val bcItemPartialModels = sc.broadcast(blockItemPartialModels) 
		        //for each user compute the prediction for all items in this block. 
		        userFeatures.flatMap{x=>
		            val userId: Int = x._1
		            val userFeature:Vector = x._2
		            
		            //itemPartialModelMap will be shipped to executors.  
		            bcItemPartialModels.value.map{x =>
		                val itemId:String = x._1
		                (userId, (itemId, x._2(userFeature) ))
		            }
		        }.saveAsObjectFile(blockPredFiles(idx))
        	}
        }
        
        //load all predict blocks and aggregate. 
        Logger.info("Loading and aggregating " + predBlockSize + " blocks.")
        var aggregatedPredBlock:RDD[(Int, (String, Double))] = sc.emptyRDD[(Int, (String, Double))]
        for (idx <- 0 until predBlockSize){
            aggregatedPredBlock = aggregatedPredBlock ++ 
            		sc.objectFile[(Int, (String, Double))](blockPredFiles(idx))
        }
        aggregatedPredBlock.coalesce(Pipeline.getPartitionNum)
        
    }else{
	    //for each user get all possible user item features
	    Logger.info("Generating all possible user-item features")
	
	    if (jobInfo.outputResource(sampledItemUserFeatFile)){
	        val sampledUFIFRDD = userFeatures.cartesian(coldItemFeatures
	            ).map{ x=> //((userID, userFeature), (itemID, itemFeature))
	                val userID:Int = x._1._1
	                val itemID:String = x._2._1
	                val feature:Vector = x._1._2 ++ x._2._2
	                (userID, (itemID, feature))
	            }
	        //NOTE: by rearranging (userID, (itemID, feature)) we want to use
	        //      the partitioner by userID.
	        sampledUFIFRDD.coalesce(1000).saveAsObjectFile(sampledItemUserFeatFile)
	    }
	    val userItemFeat = sc.objectFile[(Int, (String, Vector))](sampledItemUserFeatFile)
	    		
	    //for each user in test get prediction on all train items
	    userItemFeat.mapPartitions{iter =>                 
	              def pred: (Vector) => Double = model.predict
	              //(item, prediction)
	              iter.map( x => (x._1, (x._2._1, pred(x._2._2)))) 
	            }
    }
      
    //use predictions to get recall and hits

    //get cold item sets for each user
    val userColdItems:RDD[(Int, Set[String])] = repTestData.map{x =>
      val user = x._1
      val item = x._2._1
      (user, item)
    }.groupByKey.map{x =>
      val user = x._1
      val itemSet = x._2.toSet
      (user, itemSet)
    }

    //get top-N predicted cold items for each user and size of intersection with
    //actual preferred item sets
    val topNPredColdItems:RDD[(Int, (List[String], Int))] = 
      getTopAllNItems(userItemPred, userColdItems, N)    
    
    //get top-N actual cold items for each user
    /*val topNActualColdItems:RDD[(Int, (List[String], Int))] =
      getTopAllNItems(repTestData, userColdItems, N)*/
    //topNActualColdItems
    
    topNPredColdItems
  }


  /**
   * from the passed user-item-rating triplets will find top-N items for user
   * and intersection with passed item set(actual preferred cold items set)
   * @param userItemRat user, item ratings triplets
   * @param userItemsSet set of items for users, generally actual item sets for
   * user
   * @param N number of top items needed to be found
   * @return RDD of user, his top-N items, size of intersection of top-N items
   * with userItemsSet
   */

  def getTopAllNItems(userItemRat:RDD[(Int, (String, Double))], 
    userItemsSet:RDD[(Int, Set[String])], N:Int):RDD[(Int, (List[String], Int))] = {
    
    //get user ratings on all items
    val userKeyedRatings:RDD[(Int, Iterable[(String, Double)])] = 
      userItemRat.groupByKey
    
    //join user ratings with item set
    val userItemSetNRatings:RDD[(Int, (Iterable[(String,Double)], Set[String]))] = 
      userKeyedRatings.join(userItemsSet)

    userItemSetNRatings.map{x =>
      val user = x._1
      val itemRatings = x._2._1
      //sort in decreasing order of ratings
      val sortedItemRatings = itemRatings.toList.sortBy(-_._2)
      val topNItems = sortedItemRatings.slice(0, N+1).map(_._1)
      val itemSet:Set[String] = x._2._2
      val intersectItemSet:Set[String] = itemSet & topNItems.toSet
      (user, (topNItems, intersectItemSet.size))
    }

  }




}


