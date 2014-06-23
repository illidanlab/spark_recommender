package com.samsung.vddil.recsys.feature

import scala.collection.mutable.HashMap

/**
 * This data structure stores the information of feature
 */
trait FeatureStruct {
	def featureIden:String 
	def resrouceStr:String
	def featureFileName:String
	def featureMapFileName:String
}

case class UserFeatureStruct(
				val featureIden:String, 
				val resrouceStr:String,
				val featureFileName:String, 
				val featureMapFileName:String,
				val featureParams:HashMap[String, String] = new HashMap()
			) extends FeatureStruct {
}

case class ItemFeatureStruct(
				val featureIden:String,
				val resrouceStr:String,
				val featureFileName:String, 
				val featureMapFileName:String,
				val featureParams:HashMap[String, String] = new HashMap()
			) extends FeatureStruct{
}
