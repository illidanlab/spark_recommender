package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import com.samsung.vddil.recsys.feature.process.FeaturePostProcessor
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.job.JobWithFeature


trait ItemFeatureExtractor {
  
  
    
  /**
   * Extracts features from feature locations for passed items
   * @param items set of items for which features will be extracted
   * @param featureSources location of files from which features will be
   * extracted
   * @return RDD of item and sparse feature vector
   */
  protected def extractFeature(
          items:Set[String], featureSources:List[String],
          featureParams:HashMap[String, String], featureMapFileName:String,
          sc:SparkContext): RDD[(String, Vector)]

  
  
  /**
   * Uses the extractFeature function to construct original feature vectors, and apply 
   * featurePostProcessors.   
   */
  def extract(
          items:Set[String], 
          featureSources:List[String],
          featureStruct:ItemFeatureStruct,
          sc:SparkContext): RDD[(String, Vector)] = {
      
      //first we look for the base feature structure
      //which contains the original feature map to construct raw features.
      
      //we need to retrieve the original extractor
      //because the feature map contains the original
      //feature mapping.
      var baseFeatureStruct = featureStruct
      while(baseFeatureStruct.originalItemFeatureStruct.isDefined){
          baseFeatureStruct = baseFeatureStruct.originalItemFeatureStruct.get
      }
      
      extract(
          items:Set[String], 
          featureSources:List[String],
          baseFeatureStruct.featureParams, 
          baseFeatureStruct.featureMapFileName,
          featureStruct.featurePostProcessor,
          sc:SparkContext)
  }
  
  
  /**
   * Uses the extractFeature function to construct original feature vectors, and apply 
   * featurePostProcessors.   
   */
  private def extract(
          items:Set[String], 
          featureSources:List[String],
          featureParams:HashMap[String, String], 
          featureMapFileName:String,
          featurePostProcessors: List[FeaturePostProcessor],
          sc:SparkContext): RDD[(String, Vector)] = {
      
      var featureRDD: RDD[(String, Vector)] 
    		  = this.extractFeature(
    		           items, featureSources, 
    		           featureParams, featureMapFileName, sc)
      
      //post processing. 
      featurePostProcessors.foreach{processor =>
          featureRDD = processor.processFeatureVector(featureRDD)
      }
      
      featureRDD
  }
  
  
  /**
   * get files from which feature extraction should be done
   * @param dates dates for which feature should be extracted
   * @param jobInfo contains location of data
   * @return list of file paths of feature sources
   */
  def getFeatureSources(dates:List[String], jobInfo:JobWithFeature):List[String]

  //feature parameters in train 
  //var trFeatureParams:HashMap[String, String]
}


trait FactorizationItemFeatureExtractor {
    
    def itemFeatureFileName:String
    
    def itemMapLoc:String
    
    def debugMode = false
    
    protected def extractFeature(
          items:Set[String], featureSources:List[String],
          featureParams:HashMap[String, String], featureMapFileName:String,
          sc:SparkContext): RDD[(String, Vector)] = {
        
        //construct hash table from itemID to item feature, for all the training items. 
        val trainItemID2IntMap:RDD[(String, Int)] = sc.objectFile[(String, Int)](itemMapLoc)
        val trainInt2ItemIDMap:RDD[(Int, String)] = trainItemID2IntMap.map(x => (x._2,x._1))
        
        val trainItemInt2Features:RDD[(Int,Vector)] = sc.objectFile[(Int,Vector)](itemFeatureFileName)
        val trainItemID2Features = trainInt2ItemIDMap
        						   .join(trainItemInt2Features)
        						   .values
        						   
        val itemList = items.toList.map(x => (x, x))			
        val itemListRDD = sc.parallelize(itemList)
        val itemFeaturesRDD:RDD[(String, Vector)] = itemListRDD.join(trainItemID2Features).values						   
        
        if (debugMode) {
            itemFeaturesRDD.saveAsTextFile(itemFeatureFileName + "colditemtext")
        }
        
		itemFeaturesRDD
    }
    
    def getFeatureSources(dates:List[String], jobInfo:JobWithFeature):List[String] = {
    	dates.map{date =>
      		jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime) + date + "/*"
    	}.toList
    }
}
