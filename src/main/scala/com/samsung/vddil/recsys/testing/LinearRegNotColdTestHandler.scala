package com.samsung.vddil.recsys.testing

import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.model.LinearRegressionModelStruct

object LinearRegNotColdTestHandler extends NotColdTestHandler 
                                    with LinearRegTestHandler{
	
	
	/**
	 * perform predictions on test data and return result as
	 * (user, item, actual rating, predicted rating)
	 */
	def performTest(jobInfo:RecJob, testName: String, 
			            testParams: HashMap[String, String],
			            model: LinearRegressionModelStruct
			             ): RDD[(Int, Int, Double, Double)] = {
    //get test data
		var testData = jobInfo.jobStatus.testWatchTime.get
		
		//get spark context
		val sc = jobInfo.sc
	    
		//process test data
		testData = filterTestRatingData(testData, jobInfo.jobStatus, sc)
		
    val testItems = testData.map{ _.item}
                            .distinct

    val testUsers = testData.map{ _.user}
		                        .distinct
		
    //get feature orderings
    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    
    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder
    
    //get required item n user features 
    Logger.info("Preparing item features...")
		val itemFeaturesRDD:RDD[(Int, Vector)] = 
		    	getOrderedFeatures(testItems, itemFeatureOrder, 
	                        		  jobInfo.jobStatus.resourceLocation_ItemFeature, sc)
    
    val itemFeatObjFile = "hdfs://gnosis-01-01-01.crl.samsung.com:8020/user/m3.sharma/iFeat.obj"
    if (jobInfo.outputResource(itemFeatObjFile)) {
      itemFeaturesRDD.saveAsObjectFile(itemFeatObjFile)
    }
    val itemFeaturesRDD2 = sc.objectFile[(Int, Vector)](itemFeatObjFile)

    Logger.info("Preparing user features...")
    val userFeaturesRDD:RDD[(Int, Vector)] = 
		    	getOrderedFeatures(testUsers, userFeatureOrder, 
				            		  jobInfo.jobStatus.resourceLocation_UserFeature, sc, true)			

    val userFeatObjFile = "hdfs://gnosis-01-01-01.crl.samsung.com:8020/user/m3.sharma/uFeat.obj"
    if (jobInfo.outputResource(userFeatObjFile)) {
      userFeaturesRDD.saveAsObjectFile(userFeatObjFile)
    }
    val userFeaturesRDD2 = sc.objectFile[(Int, Vector)](userFeatObjFile)

    Logger.info("Concatenating user and item features in test")
    
    //get user item features
    //NOTE: this will also do filtering of test data in case feature not found 
    //owing to coverage criteria of training data
    val userItemFeatWRating = concatUserTestFeatures(userFeaturesRDD2, itemFeaturesRDD2, testData)

    val userItemFeatObjFile = "hdfs://gnosis-01-01-01.crl.samsung.com:8020/user/m3.sharma/uiFeat.obj"
    if (jobInfo.outputResource(userItemFeatObjFile)) {
      userItemFeatWRating.saveAsObjectFile(userItemFeatObjFile)
    }
    val userItemFeatWRating2 = sc.objectFile[(Int, Int, Vector, Double)](userItemFeatObjFile)

    //get prediction on test data
    //conv to label points
    Logger.info("Converting to testlabel point")
    val testLabelPoints = convToLabeledPoint(userItemFeatWRating2)
    
    //NOTE: user-item pair in test can appear more than once
    Logger.info("Getting prediction on test label points")
    val testLabelNPred = testLabelPoints.map { point =>
                              (point._1, //user
                               point._2, //item
                                point._3.label, //actual label
                               model.model.predict(point._3.features))
                            }
    val labelObjFile = "hdfs://gnosis-01-01-01.crl.samsung.com:8020/user/m3.sharma/testLabelPred.obj"
    if (jobInfo.outputResource(labelObjFile)) {
      testLabelNPred.saveAsObjectFile(labelObjFile)
    }
    val testLabelNPred2 = sc.objectFile[(Int, Int, Double, Double)](labelObjFile)
    testLabelNPred2
	}
	
}
