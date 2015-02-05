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
import com.samsung.vddil.recsys.prediction._
import com.samsung.vddil.recsys.job.RecMatrixFactJob
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import scala.reflect.ClassTag
import com.samsung.vddil.recsys.feature.ItemFactorizationFeatureStruct

object TestResourceRegItemColdHit{
  
  val IdenPrefix = "RegItemColdHit"
  
  /**
   * In case the partial models are used, this parameter 
   * determines how many blocks we divide. A smaller number 
   * can accelerate the batch computing performance, but may 
   * cause memory issues. 
   */
  val partialModelBatchNum = 10


  def generateResource(jobInfo:RecMatrixFactJob, 
          testParams:HashMap[String, String], 
          model: MatrixFactModel,
          testResourceDir:String): RDD[(Int, (List[String], Int))] = {
      
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
		val partitionNum = jobInfo.partitionNum_test
		
		//get test data from test dates
		val testData:RDD[(String, String, Double)] = DataProcess.getDataFromDates(testDates, 
		  jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), sc, partitionNum).get
		//get training items
		val trainItems:Set[String] = trainCombData.getItemList().collect.toSet
		//get cold items not seen in training
		val coldItems:Set[String] = getColdItems(testData, trainItems, sc)
		Logger.info("Cold items not seen in training: " + coldItems.size) 
		   
		//get features for cold items 
		 
		//get feature orderings
		//val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
		//                                    .userFeatureOrder
		//val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
		//                                    .itemFeatureOrder.map{feature => feature.asInstanceOf[ItemFeatureStruct]}
		  
		//get cold item features
		//Logger.info("Preparing item features..")
		//if (jobInfo.outputResource(itemFeatObjFile)) {
		//    val coldItemFeatures:RDD[(String, Vector)] = getColdItemFeatures(coldItems,
		//        jobInfo, itemFeatureOrder, testDates.toList)
		//    coldItemFeatures.saveAsObjectFile(itemFeatObjFile)
		//}
		//val coldItemFeatures:RDD[(String, Vector)] = sc.objectFile[(String, Vector)](itemFeatObjFile)
		
		//cold items with all features
		//val finalColdItems:Set[String] = coldItemFeatures.map(_._1).collect.toSet
		
		val finalColdItems:Set[String] = coldItems  
		Logger.info("Number of cold items: " + finalColdItems.size) //TODO: this could be zero.
		
		//broadcast cold items
		val bColdItems = sc.broadcast(finalColdItems)
		
		//users which preferred cold items
		val preferredUsers:RDD[String] = testData.filter(x =>
        bColdItems.value(x._2)).map(_._1)
        Logger.info("The number of preferred users with cold item: " + preferredUsers.count) //TODO: this could be zero. 
        
		//get users in training
        val trainUsers:RDD[String] = trainCombData.getUserList()
        
        //filter out training users
        val allColdUsers:RDD[String] = preferredUsers.intersection(trainUsers)
        val allColdUsersCount = allColdUsers.count
		
        //If userSampleParam is larger than 1 we treat them as real counts
	    //or else we treat them as percentage. 
	    //TODO: preventive treatment for allColdUsersCount.toDouble is zero. 
	    val userSamplePc:Double = 
	        if (userSampleParam > 1)  userSampleParam/allColdUsersCount.toDouble 
	        else userSampleParam 
	    Logger.info("Adjusted sample ratio: " + userSamplePc)
	    
	    val withReplacement = false
	    val sampledColdUsers:RDD[String] = allColdUsers.sample(withReplacement, userSamplePc, seed)
	    Logger.info("The total sampled user number: " + sampledColdUsers.count)
        
	    
	    
	    val userMapRDD:RDD[(String, Int)] = trainCombData.getUserMap()
	    
	    val coldItemTestData = testData.filter{x => 
	        val itemId = x._2
	        bColdItems.value(itemId)
	    }
        
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
        
	    
//	    //get user features
//	    Logger.info("Preparing user features...")
//	    if (jobInfo.outputResource(userFeatObjFile)){
//	      val userFeatures:RDD[(Int, Vector)] = getOrderedFeatures(coldItemUsers, userFeatureOrder, sc)
//	      userFeatures.saveAsObjectFile(userFeatObjFile)
//	    }
//	    val userFeatures:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)
//	    Logger.info("No. of users of cold items: " + userFeatures.count)
	    
        val userFeaturesRDDOption: Option[RDD[(String, Vector)]] = None
        val itemFeaturesRDDOption: Option[RDD[(String, Vector)]] = None
	    
	    val userItemPredStr:RDD[(String, (String, Double))] = computePrediction(
            model:MatrixFactModel,
            allColdUsers, sc.parallelize(finalColdItems.toList),
            userFeaturesRDDOption, itemFeaturesRDDOption,
            (resLoc: String) => jobInfo.outputResource(resLoc), sc)      
	    
        val userItemPred:RDD[(Int, (String, Double))] = 
            userItemPredStr.join(trainCombData.getUserMap().map{x=>(x._1, x._2)}).map{x=>
	            val userIdInt:Int    = x._2._2
	            val itemIdStr:String = x._2._1._1
	            val rating:Double    = x._2._1._2
	            (userIdInt, (itemIdStr, rating))
	        }
	    
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
	      
	    topNPredColdItems
    }
  
  
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


    //get spark context
    val sc = jobInfo.sc
    
    //cache intermediate files, helpful in case of crash  
    val itemFeatObjFile         		= testResourceDir + "/" + IdenPrefix + "/itemFeatWithFactFeature"   
    val itemWithoutFactFeatureObjFile   = testResourceDir + "/" + IdenPrefix + "/itemFeatWithoutFactFeature"   
    val userFeatObjFile         		= testResourceDir + "/" + IdenPrefix + "/userFeat" 
    val sampledUserFeatObjFile  		= testResourceDir + "/" + IdenPrefix + "/sampledUserFeat" 
    val sampledItemUserFeatFile 		= testResourceDir + "/" + IdenPrefix + "/sampledUserItemFeat"
    val sampledPredBlockFiles   		= testResourceDir + "/" + IdenPrefix + "/sampledPred/BlockFiles"
   
    //get test dates
    val testDates = jobInfo.testDates
    val partitionNum = jobInfo.partitionNum_test
    
    //get test data from test dates
    val testData:RDD[(String, String, Double)] = DataProcess.getDataFromDates(testDates, 
      jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), sc, partitionNum).get
    //get training items
    val trainItems:Set[String] = trainCombData.getItemList().collect.toSet
    //get cold items not seen in training
    val coldItems:Set[String] = getColdItems(testData, trainItems, sc)
    Logger.info("Cold items not seen in training: " + coldItems.size) 
   
    //get features for cold items 
         
    //get feature orderings
    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder.map{feature => feature.asInstanceOf[ItemFeatureStruct]}
    
    ///initialize profile generators.
    //get item features that are not factorization features 
    val itemFeatureWithoutFactorization = itemFeatureOrder.filter{x =>
        !x.isInstanceOf[ItemFactorizationFeatureStruct]
    }
    Logger.info("## struct list size is " + itemFeatureWithoutFactorization.size)
    
    //get factorization feature struct
    val itemFeaturewithFactorization = itemFeatureOrder.find{x =>
    	x.isInstanceOf[ItemFactorizationFeatureStruct]
    }
    
    
    // get cold item features
    // we current encounter empty collection probelm after calling function getColdItemFeatures function
    Logger.info("Preparing item features..")
    var itemAllFeatureObjFile = itemWithoutFactFeatureObjFile
    val coldItemFeaturesWithoutFactFeature:RDD[(String, Vector)] = getColdItemFeatures(coldItems,
        jobInfo, itemFeatureWithoutFactorization, testDates.toList)
        
    //save cold item features without factorization features. 
    if (jobInfo.outputResource(itemWithoutFactFeatureObjFile)) {
      coldItemFeaturesWithoutFactFeature.saveAsObjectFile(itemWithoutFactFeatureObjFile)
    }            
    Logger.info("##1No. of Cold items : " + coldItemFeaturesWithoutFactFeature.count) 
    Logger.info("##1Dim. of Cold items : " + coldItemFeaturesWithoutFactFeature.first._2.size) 
    if (itemFeaturewithFactorization.isDefined) {
	    val allColdItemsFeatures:RDD[(String, Vector)] = includeColdItemFactorizationFeatures(coldItemFeaturesWithoutFactFeature, itemFeatureWithoutFactorization, coldItems,
	        jobInfo, itemFeaturewithFactorization.get.asInstanceOf[ItemFactorizationFeatureStruct])
	                
	    if (jobInfo.outputResource(itemFeatObjFile)) {
	      allColdItemsFeatures.saveAsObjectFile(itemFeatObjFile)
	    }
	    itemAllFeatureObjFile = itemFeatObjFile
    } 

    

    val coldItemFeatures:RDD[(String, Vector)] = sc.objectFile[(String, Vector)](itemAllFeatureObjFile)

    //cold items with all features
    val finalColdItems:Set[String] = coldItemFeatures.map(_._1).collect.toSet
  
    //Logger.info("##2Number of cold items: " + finalColdItems.size) //TODO: this could be zero. 

    //broadcast cold items
    val bColdItems = sc.broadcast(finalColdItems)

    //users which preferred cold items
    val preferredUsers:RDD[String] = testData.filter(x =>
        bColdItems.value(x._2)).map(_._1)
    Logger.info("The number of preferred users with cold item: " + preferredUsers.count) //TODO: this could be zero. 
        
    //get users in training
    val trainUsers:RDD[String] = trainCombData.getUserList() 
    
    //filter out training users
    val allColdUsers:RDD[String] = preferredUsers.intersection(trainUsers)
    val allColdUsersCount = allColdUsers.count

    //If userSampleParam is larger than 1 we treat them as real counts
    //or else we treat them as percentage. 
    //TODO: preventive treatment for allColdUsersCount.toDouble is zero. 
    val userSamplePc:Double = 
        if (userSampleParam > 1)  userSampleParam/allColdUsersCount.toDouble 
        else userSampleParam 
    Logger.info("Adjusted sample ratio: " + userSamplePc)
    
    val withReplacement = false
    val sampledColdUsers:RDD[String] = allColdUsers.sample(withReplacement, userSamplePc, seed)
    Logger.info("The total sampled user number: " + sampledColdUsers.count)


    //replace coldItemUsers from training with corresponding int id
    //val userIdMap:Map[String, Int] = trainCombData.userMap.mapObj
    //val userMapRDD:RDD[(String, Int)] = sc.parallelize(userIdMap.toList)
    val userMapRDD:RDD[(String, Int)] = trainCombData.getUserMap()
    
    val coldItemUsers:RDD[Int] = sampledColdUsers.map(x =>
        (x,1)).join(userMapRDD).map{_._2._2}
    val coldItemUsersCount = coldItemUsers.count
    Logger.info(s"Cold-item user count: $coldItemUsersCount")
    
    

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

    //use predictions to get recall and hits
    val userItemPred:RDD[(Int, (String, Double))] = computePrediction (
            	model,  userFeatures, coldItemFeatures,
            	(resLoc: String) => jobInfo.outputResource(resLoc),
            	sampledPredBlockFiles, sampledItemUserFeatFile,
            	sc, partitionNum, partialModelBatchNum
    		)    

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

    def getTopAllNItems[UserIDType:ClassTag](
            userItemRat:RDD[(UserIDType, (String, Double))], 
    		userItemsSet:RDD[(UserIDType, Set[String])], N:Int):
    		RDD[(UserIDType, (List[String], Int))] = {
	    //get user ratings on all items
		//val userKeyedRatings:RDD[(UserIDType, Iterable[(String, Double)])] =
		  
	    val userKeyedRatings = userItemRat.map{x => (x._1, List(x._2))}.reduceByKey(_ ++ _)
		val userItemSetNRatings = userKeyedRatings.join(userItemsSet)
		
		userItemSetNRatings.map{x=>
	        val userId:UserIDType = x._1
	        val itemRatings:List[(String, Double)] = x._2._1
	        //sort in decreasing order of ratings
	        val sortedItemRatings = itemRatings.toList.sortBy(-_._2)
	        val topNItems = sortedItemRatings.slice(0, N+1).map(_._1)
	        val itemSet:Set[String] = x._2._2
	        val intersectItemSet:Set[String] = itemSet & topNItems.toSet
	        (userId, (topNItems, intersectItemSet.size))
	    }
    }


}


