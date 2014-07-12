package com.samsung.vddil.recsys.linalg




object testVectors {

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
     println(v1 ++ v2)
     
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
     
     println ("Test Ends.")
  }

}