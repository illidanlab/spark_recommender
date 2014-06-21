package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap

/*
 * 
 */
trait Model{
	var modelName:String
	var resourceStr:String
	var resourceLoc:String
	var modelParams:HashMap[String, String]
}

case class RegressionModel(modelType:String, resourceStr:String, resourceLoc:String){
    
  
}
case class ClassificationModel(modelType:String, resourceStr:String, resourceLoc:String){
  
  
}



