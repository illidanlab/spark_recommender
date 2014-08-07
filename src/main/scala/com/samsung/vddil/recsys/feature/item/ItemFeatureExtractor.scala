package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap

trait ItemFeatureExtractor {
  
  /**
   * will extract features from feature locations for passed items
   * @param items set of items for which features will be extracted
   * @param featureSources location of files from which features will be
   * extracted
   * @return RDD of item and sparse feature vector
   */
  def extractFeature(items:Set[String], featureSources:List[String],
    featureParams:HashMap[String, String], featureMapFileName:String, 
    sc:SparkContext): RDD[(String, Vector)]
  
  /**
   * get files from which feature extraction should be done
   * @param dates dates for which feature should be extracted
   * @param jobInfo contains location of data
   * @return list of file paths of feature sources
   */
  def getFeatureSources(dates:List[String], jobInfo:RecJob):List[String]

  //feature parameters in train 
  var trFeatureParams:HashMap[String, String]
}
