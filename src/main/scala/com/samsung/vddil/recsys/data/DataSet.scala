package com.samsung.vddil.recsys.data

case class DataSet(
    location: String, 
    userFeatureOrder: List[String],
    itemFeatureOrder: List[String]
)