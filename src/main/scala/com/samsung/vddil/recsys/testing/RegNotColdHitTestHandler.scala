package com.samsung.vddil.recsys.testing

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.LinearRegressionModelStruct
import com.samsung.vddil.recsys.job.Rating



case class HitSet(user: String, topNPredAllItem:Array[String], 
		           topNPredNewItems:Array[String], topNTestAllItems:Array[String],
		           topNTestNewItems:Array[String], N:Int)


object RegNotColdHitTestHandler extends NotColdTestHandler 
                                with LinearRegTestHandler {
	
	/*
	 * perform predictions on all possible items for user and return top predicted
	 * items according to model and top items in test along with new items for user
	 * which he didn't see in training
	 * RDD[(User, ((topPredictedAll, topPredictedNew), (topTestAll, topTestNew)))]
	 */
	def performTest(jobInfo:RecJob, testName: String,
			        testParams:HashMap[String, String],
			        model: LinearRegressionModelStruct): 
			            RDD[HitSet] = {
	    
		//get the value of "N" in Top-N from parameters
		val N:Int = testParams.getOrElse("N", "10").toInt
		
		//get spark context
		val sc = jobInfo.sc
		
		//get test data
		var testData = jobInfo.jobStatus.testWatchTime.get
		
		//filter test data to remove new users/items
		testData = filterTestRatingData(testData, jobInfo.jobStatus, sc)
        testData.persist
        
        
		//get train data
		
		
		//get test users
        val testUsers = testData.map{ _.user}
                                .distinct
                                .collect
                                .toSet
		
		//get train items
		val trainItems = jobInfo.jobStatus.items.toSet
		
		
		//get feature orderings
        val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                            .userFeatureOrder
        
        val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                            .itemFeatureOrder
		
                                            
        //get required user item features     
        val userFeaturesRDD = getOrderedFeatures(testUsers, userFeatureOrder, 
                            jobInfo.jobStatus.resourceLocation_UserFeature, sc)
            
        val itemFeaturesRDD = getOrderedFeatures(trainItems, itemFeatureOrder, 
                            jobInfo.jobStatus.resourceLocation_ItemFeature, sc)

                            
        //for each user get train/past/old items, require to know new items for user
        //NOTE: This will generate user item set map which can take memory
        val trainUserItemsSet = sc.textFile(jobInfo.jobStatus.resourceLocation_CombineData).map { x =>
                                	val fields = x.split(',')
                                	val user = fields(0)
                                	val item = fields(1)
                                	(user, item)
                                }.groupByKey().map { x=> 
                                	(x._1, x._2.toSet) //[user, itemsSet]
                                }.collectAsMap 
                                
        //for each user get all possible user item features
        val userItemFeat = concateUserWAllItemFeat(userFeaturesRDD, itemFeaturesRDD)

        //for each user in test get prediction on all train items
		val userItemPred = userItemFeat.map{ x =>
			                                //(user, (item, prediction))
                                	        (x._1, x._2, model.model.predict(x._3) ) 
                                       }.map(x => Rating(x._1, x._2, x._3.toDouble))
        
        //get top N predicted items for user
        val topPredictedItems = getTopAllNNewItems(userItemPred, trainUserItemsSet, N)
		
		//for each user in test, get his actual Top-N overall viewed items
        val topTestItems = getTopAllNNewItems(testData, trainUserItemsSet, N)

        //join predicted and test ranking by user key
        val topPredNTestItems = topPredictedItems.join(topTestItems)
       
       //RDD[(user, ((topPredictedAll, topPredictedNew), (topTestAll, topTestNew)))]
       topPredNTestItems.map(x => HitSet(x._1, //user
      		                             x._2._1._1, x._2._1._2, //top predicted all, top predicted new  
      		                             x._2._2._1, x._2._2._2, N))//top test all, top test new
	}
	
	
	/*
	 *will return top-N items both including and excluding passed item set 
	 */
	def getTopAllNNewItems(userItemRat:RDD[Rating], 
			                userItemsSet:scala.collection.Map[String, Set[String]], 
			                N: Int): RDD[(String, (Array[String], Array[String]))] = {
		userItemRat.map{x => (x.user, (x.item, x.rating))}
                .groupByKey()
                .map{userItemRat =>
                    val user = userItemRat._1
                    //sort item in descending order
                    //TODO: can be made more efficient by not sorting complete list
                    val itemScorePairs = userItemRat._2.toArray.sortBy(-_._2)
                    val topNAllItems = itemScorePairs.slice(0, N+1).map(_._1)
                    //get only new items i.e. not present in training
                    val topNNewItems = itemScorePairs.filterNot(x => userItemsSet(user)(x._1))
                                                     .slice(0, N+1).map(_._1)   
                    (user, (topNAllItems, topNNewItems))
                }
	}
	
}