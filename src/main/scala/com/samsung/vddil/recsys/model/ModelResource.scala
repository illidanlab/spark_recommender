package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap

/**
 * This is the results returned by a model processing unit. 
 * 
 * success:Boolean stores if this feature is successful
 * 
 * resourceMap: stores a (key, resource) pair to be handled by ModelHandler. 
 * 
 * resourceIden: a unique identifier for this model (to be used in HashMap purpose).
 * 
 * 
 */
class ModelResource(var success:Boolean, var resourceMap:HashMap[String, Any] = new HashMap, var resourceIden:String) {
    
}

object ModelResource{
   val ResourceStr_RegressModel  = "regModel"
   val ResourceStr_RegressPerf   = "regPerf"
   val ResourceStr_ClassifyModel = "clsModel"
   val ResourceStr_ClassifyPerf  = "clsPerf"
    
   /**
    * Provides a shortcut with an empty ModelResource indicating a failed status.
    */
   def fail:ModelResource = {
      new ModelResource(false, null, "")
   }
}