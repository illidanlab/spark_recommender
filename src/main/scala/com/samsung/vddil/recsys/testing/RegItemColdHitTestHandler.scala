package com.samsung.vddil.recsys.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vectors => SVs, Vector => SV}
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.linalg.SparseVector
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.model.ModelStruct
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.data.DataProcess


object RegItemColdHitTestHandler extends ColdTestHandler with
LinearRegTestHandler {


  def performTest(jobInfo:RecJob, testName: String,
    testParams:HashMap[String, String], 
    metricParams:HashMap[String, String], model: ModelStruct) = {
    
    //get the value of "N" in Top-N from parameters
    val N:Int = testParams.getOrElseUpdate("N", "10").toInt
 
    //get spark context
    val sc = jobInfo.sc
    
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
    //TODO: convert this to Vector 
    val coldItemFeatures:RDD[(String, SparseVector)] = getColdItemFeatures(coldItems,
      jobInfo, itemFeatureOrder, testDates.toList)

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
    val coldUsers:RDD[Int] = filtColdUsers.map(x =>
        (x,1)).join(userMapRDD).map{_._2._2}

    //get cold user features
    val userFeatures:RDD[(Int, Vector)] = getOrderedFeatures(coldUsers, userFeatureOrder, 
      jobInfo.jobStatus.resourceLocation_UserFeature, sc)
  
     
    
  }

}


