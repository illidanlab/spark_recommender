package com.samsung.vddil.recsys.testing

/**
 * @param user id of user for which top-N items has been collected
 * @param topNPredAllItem top N items predicted using all items 
 * @param topNPredNewItems top N items predicted excluding items in training set
 * @param topNTestAllItems actual top N items from test
 * @param topNTestNewItems actual top N items excluding items in training
 * @param N
 */
case class HitSet(user: Int, topNPredAllItem:List[Int], 
               topNPredNewItems:List[Int], topNTestAllItems:List[Int],
               topNTestNewItems:List[Int], N:Int)

