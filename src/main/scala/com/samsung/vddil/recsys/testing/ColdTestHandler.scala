package com.samsung.vddil.recsys.testing

import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor
import com.samsung.vddil.recsys.feature.ItemFeatureHandler
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import com.samsung.vddil.recsys.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap

trait ColdTestHandler {
  
  /**
   * get new items not seen during training from test
   * @param testData RDD of ratings in test data
   * @param trainItems contains set of train items
   * @param sc spark context
   * @return set of new items not appeared in training
   */
  def getColdItems(testData:RDD[(String, String, Double)], trainItems:Set[String], 
    sc:SparkContext): Set[String] = {
  
    //broadcast trainItems
    val bTrItems = sc.broadcast(trainItems)

    //get test items
    val testItems:Set[String] = testData.map(_._2 //item string id
                                            ).filter(
                                              item => !(bTrItems.value(item))
                                            ).collect.toSet
    testItems
  }

  /**
   * return features for passed items  
   * @param items set of items for which we need to generate feature
   * @param jobInfo
   * @param featureOrder
   * @param featureSources
   */
  def getColdItemFeatures(items:Set[String], jobInfo:RecJob,
    featureOrder:List[String], dates:List[String]
    ):RDD[(String, SparseVector)] = {
    
    //get feature resource location map
    val featureResourceMap = jobInfo.jobStatus.resourceLocation_ItemFeature  
    
    //get spark context
    val sc = jobInfo.sc

    val itemFeatures:List[RDD[(String, SparseVector)]] = featureOrder.map{featureResStr =>
      val itemFeatureExtractor:ItemFeatureExtractor =
        ItemFeatureHandler.revItemFeatureMap(featureResStr)
      val featMapFileName:String =
        featureResourceMap(featureResStr).featureMapFileName
      val featParams = itemFeatureExtractor.trFeatureParams
      val featureSources = itemFeatureExtractor.getFeatureSources(dates, jobInfo)
      itemFeatureExtractor.extractFeature(items, featureSources, featParams,
        featMapFileName, sc)
    }

    //combine feature in order
    val headItemFeatures:RDD[(String, SparseVector)] = itemFeatures.head 
    val combItemFeatures:RDD[(String, SparseVector)] =
      itemFeatures.tail.foldLeft(headItemFeatures){ (itemFeat1, itemFeat2) =>
        val joinedItemFeat:RDD[(String, (SparseVector, SparseVector))] = itemFeat1.join(itemFeat2)
        joinedItemFeat.mapValues{featVecs =>
          (featVecs._1 ++ featVecs._2).toSparse
        }
      }

    combItemFeatures
  }


}
