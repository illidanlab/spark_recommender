package com.samsung.vddil.recsys.testing

import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.LinearRegressionModelStruct

object LinearRegNotColdTestHandler extends NotColdTestHandler 
                                    with LinearRegTestHandler{
	
	
	/*
	 * perform predictions on test data nd return result as
	 * (user, item, actual rating, predicted rating)
	 */
	def performTest(jobInfo:RecJob, testName: String, 
			            testParams: HashMap[String, String],
			            model: LinearRegressionModelStruct
			             ): RDD[(String,String,Double, Double)] = {
	    //get test data
		var testData = jobInfo.jobStatus.testWatchTime.get
		
		//get spark context
		val sc = jobInfo.sc
	    
		//process test data
		testData = filterTestRatingData(testData, jobInfo.jobStatus, sc)
		testData.persist
		
		
		val testUsers = testData.map{ _.user}
		                        .distinct
		                        .collect
		                        .toSet
		
    val testItems = testData.map{ _.item}
                            .distinct
                            .collect
                            .toSet
                   
		//get feature orderings
    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    
    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder

    //get required user item features     
    val userFeaturesRDD = getOrderedFeatures(testUsers, userFeatureOrder, 
                    jobInfo.jobStatus.resourceLocation_UserFeature, sc)
      
    val itemFeaturesRDD = getOrderedFeatures(testItems, itemFeatureOrder, 
                            jobInfo.jobStatus.resourceLocation_ItemFeature, sc)
        
    //get user item features
    //NOTE: this will also do filtering of test data in case feature not found 
    //owing to coverage criteria of training data
    val userItemFeatWRating = concatUserTestFeatures(userFeaturesRDD, itemFeaturesRDD, testData, sc)

    //get prediction on test data
    //conv to label points
    val testLabelPoints = convToLabeledPoint(userItemFeatWRating)
    
    //NOTE: user-item pair in test can apeear more than once
    val testLabelNPred = testLabelPoints.map { point =>
                              (point._1, //user
                               point._2, //item
                                point._3.label, //actual label
                               model.model.predict(point._3.features))
                            } 
    testLabelNPred
        
	}
	
}
