package com.samsung.vddil.recsys.utils

import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.linalg.Vector
import java.io._
import org.apache.spark.mllib.linalg.{Vector => SV, DenseVector => SDV, SparseVector => SSV}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

/**
 *This object facilitates selction of desired features for items and write them
 *into single file on disk
 */
object FeatureWriter {

  /**
   * find features for given item sets and then join these features. It will
   * return features for only those items for which all the features exists
   * @param itemSets set of items for which we need to find feature
   * @param featureObjFileNames list of feature filenames
   * @return RDD of item and its combined features
   */
  def getJoinedFeatures(itemSets:Set[Int], featureObjFileNames:List[String], 
    sc:SparkContext):RDD[(Int, Vector)] = {

    val arrFeaturesRDD:List[RDD[(Int, Vector)]] =
      featureObjFileNames.map{featureFileName =>
        val itemFeatures:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](featureFileName)
        itemFeatures.filter(x => itemSets(x._1))     
    }
    
    //joint the features in the order they are read 
    val headFeature:RDD[(Int, Vector)] = arrFeaturesRDD.head
    val tailFeatures:List[RDD[(Int, Vector)]] = arrFeaturesRDD.tail
    val combFeature:RDD[(Int, Vector)] = tailFeatures.foldLeft(headFeature){(feature1, feature2) => 
      feature1.join(feature2).map {x =>
        val id = x._1
        val feature = x._2._1 ++ x._2._2
        (id, feature)
      }
    }
    combFeature
  }


  def convItemFeaturesToStr(features: RDD[(Int, Vector)], sep:String = ","):RDD[(Int, Int, String)] = {
    //convert sparse feature vector to string of form
    //itemId, vector size, "featureInd1 featureVal1 featureInd2 featureVal2"
    val featuresStr:RDD[(Int, Int, String)] = features.map{x =>
      val id = x._1
      val sparseVec:SSV = x._2.toMLLib match {
        case spVec:SSV => spVec
        case _ => throw new ClassCastException
      }
      val vecSize:Int = sparseVec.size
      val inds:Array[Int] = sparseVec.indices
      val values:Array[Double] = sparseVec.values
      val featureString = inds.zip(values).map{
        case (ind, value) => ind + sep + value
      }.mkString(sep)
      (id, vecSize, featureString)
    }
    featuresStr 
  }


  /**
   * will convert item sparse feature vector to string "itemId, size,
   * featureInd1 featureVal1 ..."
   * @param features RDD of item and feature vector
   * @param featureFileName fileName string
   */
  def saveFeatureToHDFSFile(features: RDD[(Int, Vector)], featureFileName:String, 
                            sep:String = ",") = {
    //convert sparse feature vector to string of form
    //itemId, vector size, "featureInd1 featureVal1 featureInd2 featureVal2"
    val featuresStr:RDD[String] = convItemFeaturesToStr(features).map{x =>
      val id = x._1
      val vecSize = x._2
      val featureStr = x._3
      id + sep + vecSize + sep + featureStr
    }
    featuresStr.coalesce(1).saveAsTextFile(featureFileName)
  }


  /**
   * will convert item sparse feature vector to CSR matrix where rows are
   * ordered by item Ids  and its assumed all vectors are of same size
   * @param features RDD of item and feature vector
   * @param featureFileName fileName string
   */
  def saveFeatureToCSRFile(features:RDD[(Int, Vector)], featureFileName:String) = {
    //convert sparse feature vector to RDD of form
    //itemId, "featureInd1 featureVal1 featureInd2 featureVal2"
    val featuresStr:RDD[(Int, String)] = convItemFeaturesToStr(features).map{x =>
      val id = x._1
      val feat = x._3
      (id, feat)
    }
 
    //collect item features and sort them by id
    val itemFeatures:Array[(Int, String)] = featuresStr.collect.sortBy(_._1)  
    
    val writer = new PrintWriter(new File(featureFileName))
    itemFeatures.map(x => writer.write(x._2 + "\n"))
    writer.close
  }


}
