package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.model.ModelStruct

case class HitSet(user: Int, topNPredAllItem:List[Int], 
               topNPredNewItems:List[Int], topNTestAllItems:List[Int],
               topNTestNewItems:List[Int], N:Int)


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
              model: ModelStruct): 
                  RDD[HitSet] = {
    //hash string to cache intermediate files, helpful in case of crash    
    val dataHashStr =  HashString.generateHash(testName + "RegNotColdHit")

    //get the value of "N" in Top-N from parameters
    val N:Int = testParams.getOrElse("N", "10").toInt
   
    //get percentage of user sample to predict on as it takes really long to
    //compute on all users
    val userSamplePc:Double = testParams.getOrElse("UserSampleSize",
                                                  "0.2").toDouble

    //seed parameter needed for sampling test users
    val seed = testParams.getOrElse("seed", "3").toInt

    //get spark context
    val sc = jobInfo.sc
    
    //get test data
    val testData = jobInfo.jobStatus.testWatchTime.get
    
    //filter test data to remove new users/items
    val filtTestData = filterTestRatingData(testData, jobInfo.jobStatus,
                          sc).map(x => 
                            (x.user, (x.item, x.rating))
                          )
        
    //get test users
    val testUsers = filtTestData.map{ _._1.toInt}.distinct  

    //get sampled test users based on passed sample size
    val withReplacement = false
    val sampledTestUsers = testUsers.sample(withReplacement, userSamplePc, seed)

    //get test data only corresponding to sampled users
    val sampledTestData = filtTestData.join(testUsers.map((_,1)))
                                      .map{x =>
                                        val user = x._1
                                        val item = x._2._1._1
                                        val rating = x._2._1._2
                                        (user, (item, rating))
                                      }

    //get train items
    val trainItems = sc.parallelize(jobInfo.jobStatus.itemIdMap.values.toList)
    
    //get feature orderings
    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    
    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder
                                        
    //get required user item features     
    Logger.info("Preparing item features...")
    val itemFeatObjFile = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/itemFeatObj" + dataHashStr 
    if (jobInfo.outputResource(itemFeatObjFile)) {
      //item features file don't exist
      //generate and save
      val iFRDD = getOrderedFeatures(trainItems, itemFeatureOrder, 
                    jobInfo.jobStatus.resourceLocation_ItemFeature, sc)
      iFRDD.saveAsObjectFile(itemFeatObjFile)
    } 
    val itemFeaturesRDD:RDD[(Int, Vector)] =  sc.objectFile[(Int, Vector)](itemFeatObjFile)                    


    Logger.info("Preparing user features...")
    val userFeatObjFile = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/userFeatObj" + dataHashStr 
    if (jobInfo.outputResource(userFeatObjFile)) {
      //item features file don't exist
      //generate and save
      val uFRDD = getOrderedFeatures(testUsers, userFeatureOrder, 
                    jobInfo.jobStatus.resourceLocation_UserFeature, sc)
      uFRDD.saveAsObjectFile(userFeatObjFile)
    }  
    val userFeaturesRDD:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)                    

    //get features only for sampled test users
    val sampledTestUserFeatures = userFeaturesRDD.join(sampledTestUsers.map((_,1)))
                                                  .map{ x=>
                                                    val user = x._1
                                                    val features = x._2._1
                                                    (user, features)
                                                  }


    //for each user get train/past/old items, require to know new items for user
    //NOTE: This will generate user item set map which can take y
    Logger.info("Get training users item sets")
    val trainUserItem= sc.textFile(jobInfo.jobStatus.resourceLocation_CombineData).map { x =>
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


    //for each user get all possible user item features
    Logger.info("Generating all possible user-item features")

    //TODO: remove hard coded partition count, Pipeline.getPartitionNum not
    //working correctly? investigate and fix
    val userItemFeat = concateUserWAllItemFeat(sampledTestUserFeatures, itemFeaturesRDD
                                              ).map(x =>
                                                //(user, (item, feature))
                                                (x._1, (x._2, x._3))
                                              ).partitionBy(Pipeline.getHashPartitioner())
                
    //for each user in test get prediction on all train items
    val userItemPred:RDD[(Int, (Int, Double))] = userItemFeat.mapPartitions{iter =>                 
              def pred: (org.apache.spark.mllib.linalg.Vector) => Double = model.predict
              //(item, prediction)
              iter.map( x => (x._1, (x._2._1, pred(x._2._2)))) 
            }
    
    //get top N predicted items for user
    val topPredictedItems = getTopAllNNewItems(userItemPred, sampledUserTrainItemsSet, N)

    //for each user in test, get his actual Top-N overall viewed items
    val topTestItems = getTopAllNNewItems(sampledTestData, sampledUserTrainItemsSet, N)

    //join predicted and test ranking by user keyi
    val topPredNTestItems = topPredictedItems.join(topTestItems)
   
    //RDD[(user, ((topPredictedAll, topPredictedNew), (topTestAll, topTestNew)))]
    topPredNTestItems.map(x => HitSet(x._1, //user
                                       x._2._1._1, x._2._1._2, //top predicted all, top predicted new  
                                       x._2._2._1, x._2._2._2, N))//top test all, top test new
  }

  
  /*
   *will return top-N items both including and excluding passed item set
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
      val sortedItemRatings = itemRatings.toList.sortBy(-_._2)
      val itemSet:Set[Int] = x._2._2
      val topNAllItems = sortedItemRatings.slice(0, N+1).map(_._1)
      val topNNewItems = sortedItemRatings.filterNot(x => itemSet(x._1))
                                          .slice(0, N+1).map(_._1)   
      (user, (topNAllItems, topNNewItems))    
    }

  }
  
}
