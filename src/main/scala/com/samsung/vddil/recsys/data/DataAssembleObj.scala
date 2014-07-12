package com.samsung.vddil.recsys.data

import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import org.apache.spark.HashPartitioner
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJobStatus
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.linalg.Vector

/*
 * This is the object version of data assemble 
 */
object DataAssembleObj {
   
   /**
   * return join of features of specified Ids and ordering of features
   * 
   * @param idSet
   * @param usedFeature
   * @param featureResourceMap
   * @param sc SparkContext
   */
   def getCombinedFeatures(
		   idSet: RDD[String], 
		   usedFeatures: HashSet[String], 
           featureResourceMap: HashMap[String, FeatureStruct], 
           sc: SparkContext
       ): (RDD[(String, Vector)], List[String]) = 
   {
       val usedFeaturesList = usedFeatures.toList
       
       val idSetRDD = idSet.map(x => (x,1))
       
       //join all features RDD
       ///
       var featureJoin = sc.objectFile[(String, Vector)](featureResourceMap(usedFeaturesList.head).featureFileName).
       					 join(idSetRDD).map{
    	   					//x._1 => id, x._2._1 => feature vector, x._2._2 => 1
    	   					x=> (x._1, x._2._1)
       					 }
       ///remaining
	   for (usedFeature <- usedFeaturesList.tail){
		   featureJoin = featureJoin.join(
				sc.objectFile[(String, Vector)](featureResourceMap(usedFeature).featureFileName)
		   ).map{ x=>
		      (x._1, x._2._1 ++ x._2._2)
		   }
	   }
	   (featureJoin, usedFeaturesList)
   }
  
   /**
   * will return intersection of IDs for which features exist
   * 
   * @param usedFeatures features for which we want intersection of ids
   * @param featureResourceMap contains mapping of features to actual files
   * @param sc SparkContext
   */
  def getIntersectIds(usedFeatures: HashSet[String], 
            featureResourceMap: HashMap[String, FeatureStruct], 
            sc: SparkContext):  RDD[String] = {
      
      val intersectIds = usedFeatures.map{feature =>
        sc.objectFile[(String, Vector)](featureResourceMap(feature).featureFileName)
          .map(_._1) //the first field is always id
      }.reduce((idSetA, idSetB) => idSetA.intersection(idSetB)) // reduce to get intersection of all sets
          
      intersectIds
  }
    
   /**
   * will return only those features which satisfy minimum coverage criteria
   * 
   * @param featureResourceMap contains map of features and location
   * @param minCoverage minimum coverage i.e. no. of features found should be greater than this pc
   * @param sc spark context
   * @param total number of items or users
   */
  def filterFeatures(featureResourceMap: HashMap[String, FeatureStruct], 
      minCoverage: Double, sc: SparkContext, total: Int) :HashSet[String] = {
    //set to keep keys of item feature having desired coverage
        var usedFeatures:HashSet[String] = new HashSet()

        //check each feature against minCoverage
        featureResourceMap foreach {
            case (k, v) =>
                {
                    val numFeatures = sc.objectFile[(String, Vector)](v.featureFileName).count
                    if ( (numFeatures.toDouble/total)  > minCoverage) {
                      //coverage satisfy by feature add it to used set
                        usedFeatures += k
                    }
                }
        }
        usedFeatures
   }
  
   def assembleContinuousDataIden(
      dataIdentifier:String,
      userFeature:HashSet[String], 
      itemFeature:HashSet[String]):String = {
    return "ContAggData_" + dataIdentifier+ 
          HashString.generateHash(userFeature.toString) + "_" + 
          HashString.generateHash(itemFeature.toString) 
   }
	
  def assembleBinaryData(jobInfo:RecJob, minIFCoverage:Double, minUFCoverage:Double):String = {
      //see assembleContinuousData
     throw new NotImplementedError("This function is yet to be implemented. ")
  }
  
  def assembleBinaryDataIden(
      dataIdentifier:String,
      userFeature:HashSet[String], 
      itemFeature:HashSet[String]):String = {
    return "BinAggData_" + dataIdentifier + 
          HashString.generateHash(userFeature.toString) + "_" + 
          HashString.generateHash(itemFeature.toString)
  }
}