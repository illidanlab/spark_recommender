package com.samsung.vddil.recsys

/**Provides objects and classes for data processing 
 * 
 * ==Overview==
 * There are several data processing components in the recommendation pipeline. 
 * The [[com.samsung.vddil.recsys.data.DataProcess]] read watch time data from 
 * HDFS, and construct the user-item matrix, as well as a list of items and a 
 * list of users. 
 * 
 * The object of [[com.samsung.vddil.recsys.data.DataAssemble]] provides functions 
 * to join item features, user features with the user-item matrix, to produce 
 * training (testing) data for model. The data structure [[com.samsung.vddil.recsys.data.AssembledDataSet]] is used to 
 * store an assembled feature, which includes features and their orders used in assembling them.  
 * 
 * The object of [[com.samsung.vddil.recsys.data.DataProcess]] provides functions to 
 * split a data set into training, testing and validation parts.  
 * 
 * 
 */
package object data 