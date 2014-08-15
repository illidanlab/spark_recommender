package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.feature.FeatureStruct

/**
 * This is the data structure for data
 */
trait DataStruct {

}

/**
 * This is the data used to store an assembled feature, which includes 
 * features and their orders used in assembling them.  
 * 
 * @param location Resource location
 * @param userFeatureOrder Order of user feature, each element is the resource identity of a specific user feature
 * @param itemFeatureOrder Order of item feature, each element is the resource identity of a specific user feature
 */
class AssembledDataSet(
    val location: String, 
    val userFeatureOrder: List[FeatureStruct],
    val itemFeatureOrder: List[FeatureStruct]
)

class SplittedAssembledDataSet(
        override val location:String,
        override val userFeatureOrder: List[FeatureStruct],
        override val itemFeatureOrder: List[FeatureStruct]
        ) extends AssembledDataSet(location, userFeatureOrder, itemFeatureOrder){
    
}