package com.samsung.vddil.recsys.testing

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.model.LinearRegressionModelStruct
import com.samsung.vddil.recsys.utils.HashString
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

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
    
    //hash string to cache intermediate files, helpful in case of crash    
    val dataHashStr =  HashString.generateHash(testName + "LinearRegNotCold")

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
    val itemFeatObjFile = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/itemFeatObj" + dataHashStr
    if (jobInfo.outputResource(itemFeatObjFile)) {
      //item features file don't exist
      //generate and save
      val iFRDD = getOrderedFeatures(testItems, itemFeatureOrder, 
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
    
    
    Logger.info("Concatenating user and item features in test")
    
    //get user item features
    //NOTE: this will also do filtering of test data in case feature not found 
    val userItemFeatObjFile = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/uiFeat.obj" + dataHashStr
    if (jobInfo.outputResource(userItemFeatObjFile)) {
      val uIFeatWRating = concatUserTestFeatures(userFeaturesRDD, itemFeaturesRDD, testData) 
      uIFeatWRating.saveAsObjectFile(userItemFeatObjFile)
    }
    val userItemFeatWRating = sc.objectFile[(Int, Int, Vector, Double)](userItemFeatObjFile)

    //get prediction on test data
    //conv to label points
    Logger.info("Converting to testlabel point")
    val testLabelPoints = convToLabeledPoint(userItemFeatWRating)
    
    //NOTE: user-item pair in test can appear more than once
    Logger.info("Getting prediction on test label points")
    val testLabelNPred = testLabelPoints.map { point =>
                              (point._1, //user
                               point._2, //item
                                point._3.label, //actual label
                               model.model.predict(point._3.features))
                            }
    /*
    val labelObjFile = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/testLabelPred.obj" + dataHashStr 
    if (jobInfo.outputResource(labelObjFile)) {
      testLabelNPred.saveAsObjectFile(labelObjFile)
    }
    val testLabelNPred2 = sc.objectFile[(Int, Int, Double, Double)](labelObjFile)
    */
    testLabelNPred
	}
	
}
