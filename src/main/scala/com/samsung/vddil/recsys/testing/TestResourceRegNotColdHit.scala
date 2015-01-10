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
import com.samsung.vddil.recsys.job.RecMatrixFactJob
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import com.samsung.vddil.recsys.prediction._


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
    def generateResource(jobInfo:RecMatrixFactJob, 
          testParams:HashMap[String, String], 
          model: MatrixFactModel, 
          testResourceDir:String):RDD[HitSet] = {
      
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
      
       val testParamStr = HashString.generateHash(testParams.toString)
       
       //cache intermediate files, helpful in case of crash  
   	   val itemFeatObjFile         = testResourceDir + "/" + IdenPrefix + "_" + testParamStr + "/itemFeat"   
	   val userFeatObjFile         = testResourceDir + "/" + IdenPrefix + "_" + testParamStr + "/userFeat" 
	   val sampledUserFeatObjFile  = testResourceDir + "/" + IdenPrefix + "_" + testParamStr + "/sampledUserFeat" 
	   val debugFile               = testResourceDir + "/" + IdenPrefix + "_" + testParamStr + "/debug" 
	   val sampledItemUserFeatFile = testResourceDir + "/" + IdenPrefix + "_" + testParamStr + "/sampledUserItemFeat"
	   val sampledPredBlockFiles   = testResourceDir + "/" + IdenPrefix + "_" + testParamStr + "/sampledPred/BlockFiles"
	   val filterRatingDataFile    = testResourceDir + "/" + IdenPrefix + "_" + testParamStr + "/filterTestRatingData"
		
		//get spark context
		val sc = jobInfo.sc
		val partitionNum = jobInfo.partitionNum_test
		
		//filter test data to remove new users/items
        if(jobInfo.outputResource(filterRatingDataFile)){
	        // cache the filtered rating data
	        val filtTestData:RDD[(Int, (Int, Double))] = 
	            filterTestRatingData(
	                jobInfo.jobStatus.resourceLocation_CombinedData_test.get,  //get test data
	                jobInfo.jobStatus.resourceLocation_CombinedData_train.get,
	                sc).map(x => 
	                    (x.user, (x.item, x.rating))
	                )
	        filtTestData.saveAsObjectFile(filterRatingDataFile)
        }
		val filtTestData:RDD[(Int, (Int, Double))] = sc.objectFile(filterRatingDataFile)
		
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
		
	    ////STEP: get required user item features			        
	    ////get feature orderings
	    //val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
        //                                .userFeatureOrder
    
        //val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
        //                                .itemFeatureOrder
	         
//	    Logger.info("Preparing item features...")
//	    if (jobInfo.outputResource(itemFeatObjFile)) {
//	      //item features file don't exist, we generate and save
//	      val iFRDD = getOrderedFeatures(trainItems, itemFeatureOrder, sc)
//	      iFRDD.saveAsObjectFile(itemFeatObjFile)
//	    } 
//	    val itemFeaturesRDD:RDD[(Int, Vector)] =  sc.objectFile[(Int, Vector)](itemFeatObjFile)                    
	    val itemFeaturesRDD:Option[RDD[(String, Vector)]] = None
	
	    
//	    Logger.info("Preparing user features...")
//	    if (jobInfo.outputResource(userFeatObjFile)) {
//	      //item features file don't exist, we generate and save
//	      val uFRDD = getOrderedFeatures(testUsers, userFeatureOrder, sc)
//	      uFRDD.saveAsObjectFile(userFeatObjFile)
//	    }  
//	    val userFeaturesRDD:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)
	    val userFeaturesRDD:Option[RDD[(String, Vector)]] = None
    
	    
	    
	    ////STEP: ground truth
	    val trainUserItem = trainCombData.getDataRDD().map{ x=> 
	        val userId = x._1
	        val itemId = x._2
	        (userId, itemId)
	    }
	    
	    //user-item pair in test user.  
	    val sampledUserTrainItemsSet = trainUserItem.join(sampledTestUsers.map((_,1)))
                                                    .map{x =>
                                                      val user = x._1
                                                      val item = x._2._1
                                                      (user, item)
                                                    }.groupByKey(
                                                    ).map{x =>
                                                      (x._1, x._2.toSet)
                                                    }  
	    
	    ////STEP: compute prediction            
        val userIdMap = trainCombData.getUserMap() //translate testUsers
        val itemIdMap = trainCombData.getItemMap() //translate trainItems
        
	    val userStrIdList: RDD[String] = translateIdInt2Str(userIdMap, testUsers)
	    val itemStrIdList: RDD[String] = translateIdInt2Str(itemIdMap, trainItems)
	    
	    val userItemPredStr:RDD[(String, (String, Double))] 
	    			= computePrediction(model, userStrIdList, itemStrIdList, userFeaturesRDD, userFeaturesRDD,
	    			        (resLoc: String) => jobInfo.outputResource(resLoc), sc)
	    
	    //join and translate back for evaluation. 
        //translate Int to String back and forth may be not efficient, 
	    //but it makes the model pure :-)
	    val userItemPred:RDD[(Int, (Int, Double))] = translateIdStr2Int(userItemPredStr, userIdMap, itemIdMap)
	    
	    ////debug use. 
	    userItemPred.map{x => (x._1, List(x._2))}.reduceByKey{(x1, x2)=> x1++x2}.saveAsTextFile(debugFile)
	    
	    ////STEP: process results. 
	    
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
    val filterRatingDataFile    = testResourceDir + "/" + IdenPrefix + "/filterTestRatingData"
    
    //get spark context
    val sc = jobInfo.sc
    
    val partitionNum = jobInfo.partitionNum_test
    
    //filter test data to remove new users/items
    if(jobInfo.outputResource(filterRatingDataFile)){
        // cache the filtered rating data
        val filtTestData:RDD[(Int, (Int, Double))] = 
            filterTestRatingData(
                jobInfo.jobStatus.testWatchTime.get,  //get test data
                jobInfo.jobStatus.resourceLocation_CombinedData_train.get,
                sc).map(x => 
                    (x.user, (x.item, x.rating))
                )
        filtTestData.saveAsObjectFile(filterRatingDataFile)
    }
    
    val filtTestData:RDD[(Int, (Int, Double))] = sc.objectFile(filterRatingDataFile)
        
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
    
    val userItemPred:RDD[(Int, (Int, Double))] = computePrediction (
            	model, sampledTestUserFeatures, itemFeaturesRDD,
            	(resLoc: String) => jobInfo.outputResource(resLoc), 
            	sampledPredBlockFiles, sampledItemUserFeatFile,
            	sc, partitionNum, partialModelBatchNum
    		)                                             
    
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
