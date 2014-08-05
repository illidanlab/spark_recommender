package com.samsung.vddil.recsys.feature.item

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.samsung.vddil.recsys.linalg.SparseVector
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
    sc:SparkContext): RDD[(String, SparseVector)]


}
