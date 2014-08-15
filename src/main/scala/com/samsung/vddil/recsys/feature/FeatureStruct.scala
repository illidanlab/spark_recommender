package com.samsung.vddil.recsys.feature

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor

/**
 * This data structure stores the information of feature
 */
trait FeatureStruct {
	def featureIden:String 
	def resrouceStr:String
	def featureFileName:String
	def featureMapFileName:String
	def featureParams:HashMap[String, String]
}

case class UserFeatureStruct(
				val featureIden:String, 
				val resrouceStr:String,
				val featureFileName:String, 
				val featureMapFileName:String,
				val featureParams:HashMap[String, String]
			) extends FeatureStruct {
}

case class ItemFeatureStruct(
				val featureIden:String,
				val resrouceStr:String,
				val featureFileName:String, 
				val featureMapFileName:String,
				val featureParams:HashMap[String, String],
				val extractor:ItemFeatureExtractor
			) extends FeatureStruct{
}
