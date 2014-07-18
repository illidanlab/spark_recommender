package com.samsung.vddil.recsys.data

/**
 * This is the data used to store an assembled feature, which includes 
 * features and their orders used in assembling them.  
 */
case class AssembledDataSet(
    /** Resource location */
    location: String, 
    
    /** Order of user feature, each element is the resource identity of a specific user feature */ 
    userFeatureOrder: List[String],
    
    /** Order of item feature, each element is the resource identity of a specific user feature */
    itemFeatureOrder: List[String]
)