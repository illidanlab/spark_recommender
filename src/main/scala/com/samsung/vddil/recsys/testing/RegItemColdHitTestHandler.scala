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

object RegItemColdHitTestHandler extends ColdTestHandler with
LinearRegTestHandler {


  def resourceIdentity(
          testParams:HashMap[String, String], 
          metricParams:HashMap[String, String], 
          modelStr:String
          ):String = {
        IdenPrefix + "_" + 
        		HashString.generateHash(testParams.toString) + "_" + 
        		HashString.generateHash(metricParams.toString)  + "_" +
        		modelStr
  }
  
  val IdenPrefix = "RegItemColdHit"
  
  //  /**
  //   * In case the partial models are used, this parameter 
  //   * determines how many models we compute together. A 
  //   * larger number can accelerate the batch computing 
  //   * performance but may cause memory issue.  
  //   */
  //  val partialModelBatchSize = 400
  
  /**
   * In case the partial models are used, this parameter 
   * determines how many blocks we divide. A smaller number 
   * can accelerate the batch computing performance, but may 
   * cause memory issues. 
   */
  val partialModelBatchNum = 10


  def performTest(jobInfo:RecJob, testName: String,
    testParams:HashMap[String, String], 
    metricParams:HashMap[String, String],
    model: ModelStruct):RDD[(Int, (List[String], Int))]  = {

    //get the value of "N" in Top-N from parameters
    val N:Int = testParams.getOrElseUpdate("N", "10").toInt
 
    //get spark context
    val sc = jobInfo.sc
  
    val resourceIden = resourceIdentity(testParams, metricParams, model.resourceStr)
    
    val testResourceDir = jobInfo.resourceLoc(RecJob.ResourceLoc_JobTest) + "/" + resourceIden 
    
    //cache intermediate files, helpful in case of crash  
    val itemFeatObjFile         = testResourceDir + "/itemFeat"   
    val userFeatObjFile         = testResourceDir + "/userFeat" 
    val sampledUserFeatObjFile  = testResourceDir + "/sampledUserFeat" 
    val sampledItemUserFeatFile = testResourceDir + "/sampledUserItemFeat"
    val sampledPredBlockFiles   = testResourceDir + "/sampledPred/BlockFiles"
   
    //get test dates
    val testDates = jobInfo.testDates
    //get test data from test dates
    val testData:RDD[(String, String, Double)] = DataProcess.getDataFromDates(testDates, 
      jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), sc).get
    //get training items
    val trainItems:Set[String] = jobInfo.jobStatus.itemIdMap.keySet
    //get cold items not seen in training
    val coldItems:Set[String] = getColdItems(testData, trainItems, sc)
    
   
    //get features for cold items 
         
    //get feature orderings
    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder
      
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
   
    //broadcast cold items
    val bColdItems = sc.broadcast(finalColdItems)

    //users which preferred cold items
    val preferredUsers:RDD[String] = testData.filter(x =>
        bColdItems.value(x._2)).map(_._1)
    
    //get users in training
    val trainUsers:RDD[String] = sc.parallelize(jobInfo.jobStatus.userIdMap.keys.toList)
    
    //filter out training users
    val filtColdUsers:RDD[String] = preferredUsers.intersection(trainUsers)
  
    //replace coldItemUsers from training with corresponding int id
    val userIdMap:Map[String, Int] = jobInfo.jobStatus.userIdMap
    val userMapRDD = sc.parallelize(userIdMap.toList)
    val coldItemUsers:RDD[Int] = filtColdUsers.map(x =>
        (x,1)).join(userMapRDD).map{_._2._2}

    //replace userId in test with intId and contain only cold items
    val repTestData:RDD[(Int, (String, Double))] = testData.filter{x =>
      val item = x._2
      bColdItems.value(item)
    }.map{x =>
      val user = x._1
      val item = x._2
      val rating = x._3
      (user, (item, rating))
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
      val userFeatures:RDD[(Int, Vector)] = getOrderedFeatures(coldItemUsers, userFeatureOrder, 
        jobInfo.jobStatus.resourceLocation_UserFeature, sc)
      userFeatures.saveAsObjectFile(userFeatObjFile)
    }
    val userFeatures:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)
    
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

    //get top-N predicted cold items for each user
    val topNPredColdItems:RDD[(Int, (List[String], Int))] = 
      getTopAllNItems(userItemPred, userColdItems, N)    
    
    //get top-N actual cold items for each user
    val topNActualColdItems:RDD[(Int, (List[String], Int))] =
      getTopAllNItems(repTestData, userColdItems, N)

    topNPredColdItems
  }


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


