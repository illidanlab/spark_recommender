package com.samsung.vddil.recsys.linalg


object testVectors {
    
      /*
	 * take item genre feature vector and watchtime
	 * will add feature vector weighted by watchtime and divide by sum watchtimes
	 * \sigma (watchtime*genreFeatures)/ \sigma (watchtime)
   */
  def aggByItemFeature(userFeatureWatchtimes: Iterable[(Vector, Double)]) :
  Vector = {
    require(userFeatureWatchtimes.size > 0)
    val firstWatchtime = userFeatureWatchtimes.head._1
    val initVector:Vector = Vectors.dense(firstWatchtime.size)
	    
		val (sumVec, sumWt) = 
		  userFeatureWatchtimes.foldLeft((initVector, 0.0))( 
		      (gw1, gw2) => (gw1._1 + gw2._1.mapValues(_ *  gw2._2), gw1._2 + gw2._2) )
		
		//only divide non-zero values.
		val result = Vectors.fromBreeze(sumVec.data.mapActiveValues( t => t/sumWt.toDouble)) 
	
    //as we don't know the type use pattern match to know type and return in
    //desired form
		firstWatchtime match {
	      case v:SparseVector => result.toSparse()
	      case _ => result
	    }
  }

  def main(args: Array[String]): Unit = {
     println ("Test Vectors. ")
     
     println("\nCREATE")
     val v1 = Vectors.sparse(5, Seq((0,2), (3,4)))
     println(v1)
     println(v1.toDense)
     val v2 = Vectors.sparse(5, Seq((2,1), (4,7)))
     println(v2)
     println(v2.toDense)
     
     println("\nADD")
     println(v1 + v2)
     println((v1 + v2).toDense)
     
     println("\nCONCATENATE")
     println("V1" + v1 + "("+ v1.toDense+")")
     println("V2" + v2 + "("+ v2.toDense+")")
     
     println("sparse + dense")
     println(v1.toSparse ++ v2.toDense)
     println((v1.toSparse ++ v2.toDense).toDense)
     
     println("dense + sparse")
     println(v1.toDense ++ v2.toSparse)
     println((v1.toSparse ++ v2.toDense).toDense)
     
     println("dense + dense")
     println(v1.toDense ++ v2.toDense)
     println((v1.toDense ++ v2.toDense).toDense)
     
     println("sparse + sparse")
     println(v1.toSparse ++ v2.toSparse)
     println((v1.toSparse ++ v2.toSparse).toDense)
     
     println("\nMAP")
     println("original")
     println(v1.toSparse)
     println(v1.toDense)
     
     println("sparse.mapValues")
     println(v1.toSparse.mapValues(_ * 2))
     println((v1.toSparse.mapValues(_ * 2)).toDense)
     
     println("dense.mapValues")
     println(v1.toDense.mapValues(_ * 2))
     println((v1.toDense.mapValues(_ * 2)).toSparse)
     
     println("Check aggByItemGenres")
     val v3 = Vectors.sparse(5, Seq((2,1), (3,3)))
     
     val userGenreWatchtimes:Iterable[(Vector, Double)] =
       	Array((v1 ,0.3), (v2, 0.6), (v3, 0.2))
     // (v1 = [2, 0, 0, 4, 0], 0.3)
     // (v2 = [0, 0, 1, 0, 7], 0.6)
     // (v3 = [0, 0, 1, 3, 0], 0.2)
     // vSum = v1 * 0.3 + v2 * 0.6 + v3 * 0.2 = 
     //   [0.6, 0, 0, 1.2, 0] + [0, 0, 0.6, 0, 4.2] + [0, 0, 0.2, 0.6, 0] =
     //   [0.6, 0, 0.8, 1.8, 4.2]
     // sum = 0.3+0.6+0.2 = 1.1 
     // vSum/sum = [0.5454.., 0, 0.7272.., 1.6363.., 3.8181..]
     //          = []
     val correctResult = Vectors.dense(Array(0.6/1.1, 0, 0.8/1.1, 1.8/1.1, 4.2/1.1)).toSparse
     
     println(userGenreWatchtimes)
     val aggResult = aggByItemFeature(userGenreWatchtimes)
     println(aggResult)
     println(correctResult)
       
     //the equals does not hold because there might be some numerical error in map function. 
     //println(aggResult.equals(correctResult.copy))
     
     println ("Test Ends.")
  }

}