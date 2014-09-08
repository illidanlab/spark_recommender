package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vectors => SVs, Vector => SV}
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.model.PartializableModel


object TestResourceRegNotColdHit{
  
  val IdenPrefix = "RegNotColdHit"
  
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
  
  /**
   * Performs predictions on all possible items for user and return top predicted
   * items according to model and top items in test along with new items for user
   * which he didn't see in training
   * RDD[(User, ((topPredictedAll, topPredictedNew), (topTestAll, topTestNew)))]
   * 
   * @param jobInfo
   * @param testName
   * @param testParams
   * @param metricParams
   * @param model
   * @return a RDD of hit rate. 
   */
  def generateResource(jobInfo:RecJob, 
              testParams:HashMap[String, String],
              model: ModelStruct,
              testResourceDir:String
              ):RDD[HitSet] = {

    val trainCombData = jobInfo.jobStatus.resourceLocation_CombinedData_train.get
    
    //get the value of "N" in Top-N from parameters
    val N:Int = testParams.getOrElseUpdate("N", "10").toInt
    
    //get percentage of user sample to predict on as it takes really long to
    //compute on all users
    val userSampleParam:Double = testParams.getOrElseUpdate("UserSampleSize",
                                                  "0.2").toDouble
    Logger.info("User sample parameter: " + userSampleParam)
    
    //seed parameter needed for sampling test users
    val seed = testParams.getOrElseUpdate("seed", "3").toInt
    
    //cache intermediate files, helpful in case of crash  
    val itemFeatObjFile         = testResourceDir + "/" + IdenPrefix + "/itemFeat"   
    val userFeatObjFile         = testResourceDir + "/" + IdenPrefix + "/userFeat" 
    val sampledUserFeatObjFile  = testResourceDir + "/" + IdenPrefix + "/sampledUserFeat" 
    val sampledItemUserFeatFile = testResourceDir + "/" + IdenPrefix + "/sampledUserItemFeat"
    val sampledPredBlockFiles   = testResourceDir + "/" + IdenPrefix + "/sampledPred/BlockFiles"
    //get spark context
    val sc = jobInfo.sc
    
    //get test data
    val testData = jobInfo.jobStatus.testWatchTime.get
    val partitionNum = jobInfo.partitionNum_test
    
    //filter test data to remove new users/items
    val filtTestData = filterTestRatingData(testData, trainCombData,
                          sc).map(x => 
                            (x.user, (x.item, x.rating))
                          )
        
    //get test users
    val testUsers = filtTestData.map{ _._1.toInt}.distinct  

    //get sampled test users based on passed sample size
    val withReplacement = false
    
    val totalTestUserNum = testUsers.count
    Logger.info("The total user number: " + totalTestUserNum)
    
    //If userSampleParam is larger than 1 we treat them as real counts
    //or else we treat them as percentage. 
    val userSamplePc:Double = 
        if (userSampleParam > 1)  userSampleParam/totalTestUserNum.toDouble 
        else userSampleParam 
    Logger.info("Adjusted sample ratio: " + userSamplePc)
        
    val sampledTestUsers = testUsers.sample(withReplacement, userSamplePc, seed)
    Logger.info("The total sampled user number: " + sampledTestUsers.count)
    
    //get test data only corresponding to sampled users
    val sampledTestData = filtTestData.join(testUsers.map((_,1)))
                                      .map{x =>
                                        val user = x._1
                                        val item = x._2._1._1
                                        val rating = x._2._1._2
                                        (user, (item, rating))
                                      }

    //get train item indices. 
    val trainItems = trainCombData.getItemMap().map{x => x._2}
    
    //get feature orderings
    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    
    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder
                                        
    //get required user item features     
    Logger.info("Preparing item features...")
    if (jobInfo.outputResource(itemFeatObjFile)) {
      //item features file don't exist, we generate and save
      val iFRDD = getOrderedFeatures(trainItems, itemFeatureOrder, sc)
      iFRDD.saveAsObjectFile(itemFeatObjFile)
    } 
    val itemFeaturesRDD:RDD[(Int, Vector)] =  sc.objectFile[(Int, Vector)](itemFeatObjFile)                    


    Logger.info("Preparing user features...")
    if (jobInfo.outputResource(userFeatObjFile)) {
      //item features file don't exist, we generate and save
      val uFRDD = getOrderedFeatures(testUsers, userFeatureOrder, sc)
      uFRDD.saveAsObjectFile(userFeatObjFile)
    }  
    val userFeaturesRDD:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)                    

    //get features only for sampled test users
    Logger.info("Preparing sampled user features...")
    if (jobInfo.outputResource(sampledUserFeatObjFile)){
    	val sampledUFRDD = userFeaturesRDD.
    		join(sampledTestUsers.map((_,1))).map{ x=>
               val user:Int = x._1
               val features:Vector = x._2._1
               (user, features)
            }
    	sampledUFRDD.saveAsObjectFile(sampledUserFeatObjFile)
    }
    val sampledTestUserFeatures:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](sampledUserFeatObjFile)
    
    //for each user get train/past/old items, require to know new items for user
    //NOTE: This will generate user item set map which can take y
    Logger.info("Get training users item sets")
    val trainUserItem= sc.textFile(trainCombData.resourceLoc).map { x =>
                              val fields = x.split(',')
                              val user = fields(0).toInt
                              val item = fields(1).toInt
                              (user, item)
                            }

    val sampledUserTrainItemsSet = trainUserItem.join(sampledTestUsers.map((_,1)))
                                                    .map{x =>
                                                      val user = x._1
                                                      val item = x._2._1
                                                      (user, item)
                                                    }.groupByKey(
                                                    ).map{x =>
                                                      (x._1, x._2.toSet)
                                                    }                                              
                                                
    val userItemPred:RDD[(Int, (Int, Double))] = if (model.isInstanceOf[PartializableModel]){
        //if the model is a partializable model, then we use partial model 
        //and apply the models in batch. 
        
        Logger.info("Generating item enclosed partial models")
        var itemPartialModels = itemFeaturesRDD.map{x=>
            val itemId:Int = x._1
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
		        sampledTestUserFeatures.flatMap{x=>
		            val userId: Int = x._1
		            val userFeature:Vector = x._2
		            
		            //itemPartialModelMap will be shipped to executors.  
		            bcItemPartialModels.value.map{x =>
		                val itemId:Int = x._1
		                (userId, (itemId, x._2(userFeature) ))
		            }
		        }.saveAsObjectFile(blockPredFiles(idx))
        	}
        }
        
        
        
//        Logger.info("Generating item partial models")
//        var itemPartialModels = itemFeaturesRDD.map{x=>
//            val itemId:Int = x._1
//            val partialModel = model.asInstanceOf[PartializableModel].applyItemFeature(x._2)
//            (itemId, partialModel)
//        }.collect() //these partial models are to be stored. 
//        
//        //break the partial models into blocks for computing. 
//        val itemPartialModelArr: List[Array[(Int, Vector => Double)]] 
//        		= itemPartialModels.grouped(partialModelBatchSize).toList
//        Logger.info("Item enclosed partial models created. Size:" + itemPartialModels.size)
//        
//        val predBlockSize = itemPartialModelArr.size // size of the prediction blocks 
//        Logger.info("Item enclosed partial models are divdided into " + predBlockSize + " blocks.")
//        
//        //compute for each block. 
//        val blockPredFiles = new Array[String](predBlockSize)//place to store intermediate files. 
//        for ((itemPartialModelSet, idx) <- itemPartialModelArr.zipWithIndex){
//        	Logger.info("Broadcast set [" + idx + "] with size:" + itemPartialModelSet.size)
//        	
//        	blockPredFiles(idx) = sampledPredBlockFiles + "_" + idx
//        	
//        	if (jobInfo.outputResource(blockPredFiles(idx))){
//		        val bcItemPartialModels = sc.broadcast(itemPartialModelSet) 
//		        //for each user compute the prediction for all items in this block. 
//		        sampledTestUserFeatures.flatMap{x=>
//		            val userId: Int = x._1
//		            val userFeature:Vector = x._2
//		            
//		            //itemPartialModelMap will be shipped to executors.  
//		            bcItemPartialModels.value.map{x =>
//		                val itemId:Int = x._1
//		                (userId, (itemId, x._2(userFeature) ))
//		            }
//		        }.saveAsObjectFile(blockPredFiles(idx))
//        	}
//        }
        
        //load all predict blocks and aggregate. 
        Logger.info("Loading and aggregating " + predBlockSize + " blocks.")
        var aggregatedPredBlock:RDD[(Int, (Int, Double))] = sc.emptyRDD[(Int, (Int, Double))]
        for (idx <- 0 until predBlockSize){
            aggregatedPredBlock = aggregatedPredBlock ++ 
            		sc.objectFile[(Int, (Int, Double))](blockPredFiles(idx))
        }
        aggregatedPredBlock.coalesce(partitionNum)
        
    }else{
	    //for each user get all possible user item features
	    Logger.info("Generating all possible user-item features")
	
	    if (jobInfo.outputResource(sampledItemUserFeatFile)){
	        val sampledUFIFRDD = sampledTestUserFeatures.cartesian(itemFeaturesRDD
	            ).map{ x=> //((userID, userFeature), (itemID, itemFeature))
	                val userID:Int = x._1._1
	                val itemID:Int = x._2._1
	                val feature:Vector = x._1._2 ++ x._2._2
	                (userID, (itemID, feature))
	            }
	        //NOTE: by rearranging (userID, (itemID, feature)) we want to use
	        //      the partitioner by userID.
	        sampledUFIFRDD.coalesce(1000).saveAsObjectFile(sampledItemUserFeatFile)
	    }
	    val userItemFeat = sc.objectFile[(Int, (Int, Vector))](sampledItemUserFeatFile)
	    		
	    //for each user in test get prediction on all train items
	    userItemFeat.mapPartitions{iter =>                 
	              def pred: (Vector) => Double = model.predict
	              //(item, prediction)
	              iter.map( x => (x._1, (x._2._1, pred(x._2._2)))) 
	            }
    }

    
    
    //get top N predicted items for user
    val topPredictedItems = getTopAllNNewItems(userItemPred, sampledUserTrainItemsSet, N)
    Logger.info("DEBUG:: topPredictedItems " + topPredictedItems.count)

    //for each user in test, get his actual Top-N overall viewed items
    val topTestItems = getTopAllNNewItems(sampledTestData, sampledUserTrainItemsSet, N)
    Logger.info("DEBUG:: topTestItems " + topTestItems.count)
    
    //join predicted and test ranking by user key   
    val topPredNTestItems = topPredictedItems.join(topTestItems)
    Logger.info("DEBUG:: topPredNTestItems " + topPredNTestItems.count)
    
    //RDD[(user, ((topPredictedAll, topPredictedNew), (topTestAll, topTestNew)))]
    topPredNTestItems.map(x => HitSet(x._1, //user
                                       x._2._1._1, x._2._1._2, //top predicted all, top predicted new  
                                       x._2._2._1, x._2._2._2, N))//top test all, top test new
  }

  
  /**
   * Returns top-N items both including and excluding passed item set
   * 
   * @param userItemRat RDD of rating of users on items
   * @param userItemsSet set of items which you want to exclude while
   * calculating Top-N generally its training items of user
   * @param N  number of top items to find per user
   * @return RDD caontaining for each user list of Top-N items  including passed
   * itemset and excluding passed item set
   */
  def getTopAllNNewItems(userItemRat:RDD[(Int, (Int, Double))], 
                      userItemsSet:RDD[(Int, Set[Int])], 
                      N: Int): RDD[(Int, (List[Int], List[Int]))] = {
    
    //get user ratings on all items
    val userKeyedRatings:RDD[(Int, Iterable[(Int, Double)])] = userItemRat.groupByKey

    //join user rating with already itemsSet
    val userItemSetNRatings:RDD[(Int, (Iterable[(Int,Double)], Set[Int]))] = userKeyedRatings.join(userItemsSet)

    userItemSetNRatings.map {x =>      
      val user = x._1
      val itemRatings = x._2._1
      //sort in decreasing order of ratings
      val sortedItemRatings = itemRatings.toList.sortBy(-_._2)
      val itemSet:Set[Int] = x._2._2
      val topNAllItems = sortedItemRatings.slice(0, N+1).map(_._1)
      val topNNewItems = sortedItemRatings.filterNot(x => itemSet(x._1))
                                          .slice(0, N+1).map(_._1)   
      (user, (topNAllItems, topNNewItems))    
    }

  }
  
}
