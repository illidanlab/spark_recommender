package com.samsung.vddil.recsys.testing

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.utils.HashString
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import com.samsung.vddil.recsys.job.RecMatrixFactJob

object TestResourceLinearRegNotCold {
  
    val IdenPrefix = "LinearRegNotCold"
	
        
    def generateResource(jobInfo:RecMatrixFactJob, 
			            testParams: HashMap[String, String],
			            model: MatrixFactModel,
			            testResourceDir:String
			             ): RDD[(Int, Int, Double, Double)] = {
    	//hash string to cache intermediate files, helpful in case of crash    
        val itemFeatObjFile      = testResourceDir + "/" + IdenPrefix + "/itemFeat"
	    val userFeatObjFile      = testResourceDir + "/" + IdenPrefix + "/userFeat"
	    val userItemFeatObjFile  = testResourceDir + "/" + IdenPrefix + "/userItemFeat" 
	    val filterRatingDataFile = testResourceDir + "/" + IdenPrefix + "/filterTestRatingData"
	    
	    
		//get spark context
		val sc = jobInfo.sc
		
		//process test data
		if (jobInfo.outputResource(filterRatingDataFile)){
		    // cache the filtered rating data
		    // 
			val testData = filterTestRatingData(
			        jobInfo.jobStatus.resourceLocation_CombinedData_test.get, 
			        jobInfo.jobStatus.resourceLocation_CombinedData_train.get, 
			        sc).map(x => 
	                    (x.user, (x.item, x.rating))
	                )
			testData.saveAsObjectFile(filterRatingDataFile)
		}
        val filtTestData:RDD[(Int, (Int, Double))] = sc.objectFile(filterRatingDataFile)
        
        val testData = filtTestData
        
        val testItems = testData.map{ _._2._1}.distinct

        val testUsers = testData.map{ _._1}.distinct
        
		//get feature orderings
//        val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
//                                        .userFeatureOrder
//    
//        val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
//                                        .itemFeatureOrder
    
	    //get required item n user features 
//	    Logger.info("Preparing item features...")
//	    if (jobInfo.outputResource(itemFeatObjFile)) {
//	      //item features file don't exist
//	      //generate and save
//	      val iFRDD = getOrderedFeatures(testItems, itemFeatureOrder, sc)
//	      iFRDD.saveAsObjectFile(itemFeatObjFile)
//	    } 
//	    val itemFeaturesRDD:RDD[(Int, Vector)] =  sc.objectFile[(Int, Vector)](itemFeatObjFile)                    
	    val itemFeaturesRDD:RDD[(String, Vector)] = null;
	    
//	    Logger.info("Preparing user features...")
//	    if (jobInfo.outputResource(userFeatObjFile)) {
//	      //item features file don't exist
//	      //generate and save
//	      val uFRDD = getOrderedFeatures(testUsers, userFeatureOrder, sc)
//	      uFRDD.saveAsObjectFile(userFeatObjFile)
//	    }  
//	    val userFeaturesRDD:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)           
        val userFeaturesRDD:RDD[(String, Vector)] = null;
        
        Logger.info("Concatenating user and item features in test")
        
        val predictData:RDD[(String, String, Double)] = null
        
        val pred = model.predict(predictData, userFeaturesRDD, itemFeaturesRDD)
        
        
        
        //val  
        val testLabelNPred: RDD[(Int, Int, Double, Double)] = null
        
        throw new NotImplementedError()
        
        testLabelNPred
    }
        
	/**
	 * perform predictions on test data and return result as
	 * (user, item, actual rating, predicted rating)
	 * 
	 * TODO: use subsample. 
	 */
	def generateResource(jobInfo:RecJob, 
			            testParams: HashMap[String, String],
			            model: ModelStruct,
			            testResourceDir:String
			             ): RDD[(Int, Int, Double, Double)] = {
        
        
		//hash string to cache intermediate files, helpful in case of crash    
        val itemFeatObjFile      = testResourceDir + "/" + IdenPrefix + "/itemFeat"
	    val userFeatObjFile      = testResourceDir + "/" + IdenPrefix + "/userFeat"
	    val userItemFeatObjFile  = testResourceDir + "/" + IdenPrefix + "/userItemFeat" 
	    val filterRatingDataFile = testResourceDir + "/" + IdenPrefix + "/filterTestRatingData"
		
		//get spark context
		val sc = jobInfo.sc
	    
		//process test data
		if (jobInfo.outputResource(filterRatingDataFile)){
		    // cache the filtered rating data
		    // 
			val testData = filterTestRatingData(
			        jobInfo.jobStatus.testWatchTime.get, 
			        jobInfo.jobStatus.resourceLocation_CombinedData_train.get, 
			        sc)
			testData.saveAsObjectFile(filterRatingDataFile)
		}
        
        val testData:RDD[com.samsung.vddil.recsys.job.Rating] = sc.objectFile(filterRatingDataFile)
		
		val testItems = testData.map{ _.item}.distinct

        val testUsers = testData.map{ _.user}.distinct
		
		//get feature orderings
        val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
    
        val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .itemFeatureOrder
    
	    //get required item n user features 
	    Logger.info("Preparing item features...")
	    if (jobInfo.outputResource(itemFeatObjFile)) {
	      //item features file don't exist
	      //generate and save
	      val iFRDD = getOrderedFeatures(testItems, itemFeatureOrder, sc)
	      iFRDD.saveAsObjectFile(itemFeatObjFile)
	    } 
	    val itemFeaturesRDD:RDD[(Int, Vector)] =  sc.objectFile[(Int, Vector)](itemFeatObjFile)                    
	
	    
	    Logger.info("Preparing user features...")

	    if (jobInfo.outputResource(userFeatObjFile)) {
	      //item features file don't exist
	      //generate and save
	      val uFRDD = getOrderedFeatures(testUsers, userFeatureOrder, sc)
	      uFRDD.saveAsObjectFile(userFeatObjFile)
	    }  
	    val userFeaturesRDD:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)                    
	    
	    
	    Logger.info("Concatenating user and item features in test")
	    
	    //get user item features
	    //NOTE: this will also do filtering of test data in case feature not found 
	    
	    if (jobInfo.outputResource(userItemFeatObjFile)) {
	      val uIFeatWRating = concatUserTestFeatures(userFeaturesRDD, itemFeaturesRDD, testData) 
	      uIFeatWRating.saveAsObjectFile(userItemFeatObjFile)
	    }
	    val userItemFeatWRating = sc.objectFile[(Int, Int, Vector, Double)](userItemFeatObjFile)
	
	    //get prediction on test data
	    //convert to label points
	    //Logger.info("Converting to testlabel point")
	    //val testLabelPoints = convToLabeledPoint(userItemFeatWRating)
	    
	    //NOTE: user-item pair in test can appear more than once
	    Logger.info("Getting prediction on test label points")
	    
	    val testLabelNPred = userItemFeatWRating.map{tuple =>
	        	val userID:Int      = tuple._1
	        	val itemID:Int      = tuple._2
	        	val features:Vector = tuple._3
	        	val label:Double    = tuple._4
	        	(userID, itemID, label, model.predict(features))
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
