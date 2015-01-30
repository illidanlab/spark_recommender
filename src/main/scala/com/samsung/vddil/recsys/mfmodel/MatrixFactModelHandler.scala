package com.samsung.vddil.recsys.mfmodel

import com.samsung.vddil.recsys.data.CombinedDataSet
import com.samsung.vddil.recsys.job.RecMatrixFactJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.data.DataAssemble
import scala.collection.mutable.HashSet
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD

object MatrixFactModelHandler {
	val MatrixFactModelTypeNMF:String = "nmf_model"
	val MatrixFactModelTypePMF:String = "pmf_model"
	    
	val ParamUserProfileGenerator = "UserProfileGenerator"
	val ParamItemProfileGenerator = "ItemProfileGenerator"
	val ParamVal_ProfileGeneratorRidge = "Ridge"
	val ParamVal_ProfileGeneratorLasso = "Lasso"	    
	val ParamVal_DefaultUserGenerator = ParamVal_ProfileGeneratorRidge
	val ParamVal_DefaultItemGenerator = ParamVal_ProfileGeneratorRidge
	    
	def buildModel(
		modelName:String,
		modelParams:HashMap[String, String],
		ratingData:CombinedDataSet,
		jobInfo:RecMatrixFactJob
	): Option[MatrixFactModel] = {
	    
	      
	    val paramUserProfileGeneratorType:String = 
	        modelParams.getOrElseUpdate(ParamUserProfileGenerator, ParamVal_DefaultUserGenerator)
	    val paramItemProfileGeneratorType:String = 
	        modelParams.getOrElseUpdate(ParamItemProfileGenerator, ParamVal_DefaultItemGenerator)
	    
	    //TODO: extract parameters about feature extraction.
	    val minIFCoverage:Double = 0.1 
	    val minUFCoverage:Double = 0.1
	    val sc = jobInfo.sc
	    
	    /////// Get features. 
	    val combData:CombinedDataSet = jobInfo.jobStatus.resourceLocation_CombinedData_train.get
	    val itemFeatureList = jobInfo.jobStatus.resourceLocation_ItemFeature
	    val userFeatureList = jobInfo.jobStatus.resourceLocation_UserFeature
	    
        //get the number of users and items
        val numUsers = combData.userNum
        //get the number of items
        val numItems = combData.itemNum	    
        
        //set to keep keys of item feature having desired coverage
        val usedItemFeature:HashSet[FeatureStruct] = DataAssemble.filterFeatures(
                      itemFeatureList, minIFCoverage, sc, numItems)

        //set to keep keys of user feature having desired coverage
        val usedUserFeature:HashSet[FeatureStruct] = DataAssemble.filterFeatures(
                      userFeatureList, minUFCoverage, sc, numItems) 
        
	    // perform an intersection on selected user features, generate <intersectUF>
        val userIntersectIds = DataAssemble.getIntersectIds(usedUserFeature, sc)
        //parse eligible features and extract only those with IDs present in userIntersectIds
        var (userFeaturesRDD, userFeatureOrder) = 
              DataAssemble.getCombinedFeatures(userIntersectIds, usedUserFeature, sc)
              
	    // perform an intersection on selected item features, generate <intersectIF>              
	    val itemIntersectIds = DataAssemble.getIntersectIds(usedItemFeature, sc)
	    //parse eligible features and extract only those with ids present in itemIntersectIds                  
        var (itemFeaturesRDD, itemFeatureOrder) =  
              DataAssemble.getCombinedFeatures(itemIntersectIds, usedItemFeature, sc)
	    
	    
        /////// Construct profile generator.
        val userProfileGenFunc: RDD[(Int, Vector)] => ColdStartProfileGenerator =
	        if (paramUserProfileGeneratorType.compareTo(ParamVal_ProfileGeneratorRidge) == 0){
	        	(profileRDD: RDD[(Int, Vector)]) =>
        			RidgeRegressionProfileGenerator(profileRDD, userFeaturesRDD)
	        } else if (paramUserProfileGeneratorType.compareTo(ParamVal_ProfileGeneratorLasso) == 0){
	        	(profileRDD: RDD[(Int, Vector)]) =>
        			LassoRegressionProfileGenerator(profileRDD, userFeaturesRDD)	            
	        }
	        else{
	            (profileRDD: RDD[(Int, Vector)]) =>
	            	AverageProfileGenerator(profileRDD.map{x => x._2})
	        }
	    
        val itemProfileGenFunc: RDD[(Int, Vector)] => ColdStartProfileGenerator =
            if (paramItemProfileGeneratorType.compareTo(ParamVal_ProfileGeneratorRidge) == 0){
            	(profileRDD: RDD[(Int, Vector)]) =>
        			RidgeRegressionProfileGenerator(profileRDD, itemFeaturesRDD)
            } else if (paramItemProfileGeneratorType.compareTo(ParamVal_ProfileGeneratorLasso) == 0) {
            	(profileRDD: RDD[(Int, Vector)]) =>
        			LassoRegressionProfileGenerator(profileRDD, itemFeaturesRDD)  
        	}
            else{
                (profileRDD: RDD[(Int, Vector)]) =>
	            	AverageProfileGenerator(profileRDD.map{x => x._2})
            }
        	
	    // build model
	    if(modelName.compareTo(MatrixFactModelTypePMF) == 0){
	        val modelGenerator = MatrixFactModelPMF(modelParams, userFeatureOrder, itemFeatureOrder)
	        modelGenerator.train(ratingData, userProfileGenFunc, itemProfileGenFunc, jobInfo)
	    }else{
	    	None
	    }
	    
	    
	}
}