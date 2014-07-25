package com.samsung.vddil.recsys.linalg


import java.lang.{Iterable => JavaIterable, Integer => JavaInteger, Double => JavaDouble}
import java.util.Arrays


import breeze.linalg.{Vector => BV, DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.mllib.linalg.{Vector => SV, DenseVector => SDV, SparseVector => SSV}
import org.apache.spark.mllib.linalg.{Vectors => MLLibVectors}

/**
 * A numeric vector, whose index type is Int and value type is Double 
 * The internal data structure holding the data is breeze.linalg.Vector.
 * This class can be efficiently converted into a org.apache.spark.mllib.linalg.Vector
 */
trait Vector extends Serializable {
	
   /**
    * Size of the vector
    */
   def size: Int
   
   /**
    * The internal data structure
    */
   def data: BV[Double]
   
   /**
    * Converts the instance to a double array.
    */
   def toArray: Array[Double]
   
   override def equals(other: Any): Boolean = {
    other match {
      case v: Vector =>
        Arrays.equals(this.toArray, v.toArray)
      case _ => false
    }
   }
   
   override def hashCode(): Int = Arrays.hashCode(this.toArray)
   
   /**
    * Convert the instance to a mllib vector. 
    */
   def toMLLib: SV
   
   /**
    * Gets the value of the ith element
    * @param i index
    */
   def apply(i:Int): Double = data(i)
   
   /**
    * Concatenate vectors.
    * 
    *  Sparse ++ Dense returns Sparse 
    *  Sparse ++ Sparse returns Sparse 
    *  Dense ++ Dense returns Dense
    */
   def ++(that: Vector):Vector
   
   /**
    * Add values. The return type is based on the + operation in the wrapped Breeze vector.
    * 
    * Sparse + Dense  returns Dense 
    * Sparse + Sparse returns Sparse
    * Dense  + Dense  returns Dense
    */
   def +(that: Vector):Vector
   
   /**
    * A copy of current vector into sparse form 
    */
   def toSparse():SparseVector
  
   /**
    * A copy of current vector into dense form 
    */
   def toDense():DenseVector
   
   /**
    * Map transformation
    */
   def mapValues(f:Double=>Double):Vector
   
   /**
    * copy 
    */
   def copy():Vector
}


/**
 * Factory Methods, that creates SparseVector and DenseVectors, as well as 
 * many fundamental operations.
 */
object Vectors{
   /**
    * Creates a vector from Breeze data structure
    */
   def fromBreeze(data:BV[Double]): Vector = {
       data match {
         case v:BSV[Double] => new SparseVector(v)
         case v:BDV[Double] => new DenseVector(v)
         case v:BV[_] => 
           sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
       }
   } 
  
   /**
   * Creates a dense vector from a double array.
   */
   def dense(values:Array[Double]): DenseVector = new DenseVector(values)
   
   /**
    * Creates an empty dense vector
    * 
    *  @param size vector size. 
    */
   def dense(size:Int): DenseVector = new DenseVector(BV.zeros[Double](size).toDenseVector)
 

   /**
    *Creates a sparse vector from dense array
    *@param values full array containing values
    */
    def sparse(values:Array[Double]): SparseVector = {
      val denseVec:DenseVector = dense(values) 
      denseVec.toSparse
    }


   /**
   * Creates a sparse vector providing its index array and value array.
   *
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
   def sparse(size:Int, indices: Array[Int], values:Array[Double]): SparseVector = 
     	new SparseVector(size, indices, values)
   
   /**
   * Creates a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def sparse(size: Int, elements: Seq[(Int, Double)]): SparseVector = {
    require(size > 0)

    val (indices, values) = elements.sortBy(_._1).unzip
    var prev = -1
    indices.foreach { i =>
      require(prev < i, s"Found duplicate indices: $i.")
      prev = i
    }
    require(prev < size)

    new SparseVector(size, indices.toArray, values.toArray)
  }

   def sparse(size:Int): SparseVector = {
      require(size > 0)
      
      new SparseVector(size, Array(), Array())
   }
   
//   /**
//    * Create a MLLib vector instance from our RecSys vector instance.
//    */
//   def toMLLib(recsysVector:Vector): SV = {
//       breezeToMLLib(recsysVector.data)
//   }
   
   /**
   * Creates a MLLib vector instance from a breeze vector.
   */
  private[recsys] def breezeToMLLib(breezeVector: BDV[Double]): SDV = {

        if (breezeVector.offset == 0 && breezeVector.stride == 1) {
          new SDV(breezeVector.data)
        } else {
          new SDV(breezeVector.toArray)  // Can't use underlying array directly, so make a new one
        }
  }
  
  /**
   * Creates a MLLib vector instance from a breeze vector.
   */
  private[recsys] def breezeToMLLib(breezeVector: BSV[Double]): SSV = {
 
        if (breezeVector.index.length == breezeVector.used) {
          new SSV(breezeVector.length, breezeVector.index, breezeVector.data)
        } else {
          new SSV(breezeVector.length, breezeVector.index.slice(0, breezeVector.used), breezeVector.data.slice(0, breezeVector.used))
        }
      
  }
  
  /**
   * Transform a dense breeze vector to a sparse breeze vector
   */
  def breezeDenseToSparse(breezeDenseVector: BDV[Double]): BSV[Double] = {
      
      val (indices, values) = 
    		  (0 to breezeDenseVector.size).zip(breezeDenseVector.data).
    		  filter(p => p._2 != 0).
    		  unzip
    
	  //val indices = (0 to breezeDenseVector.size).toArray
	  //val values  = breezeDenseVector.toArray
      new BSV[Double](indices.toArray, values.toArray, breezeDenseVector.size)
  }
  
  /**
   * Concatenate two sparse vectors into a new sparse vector 
   */
  def concatSparseVector(sparseVector1:SparseVector, sparseVector2:SparseVector): SparseVector = {
      new SparseVector(concatBreezeSparseVector(sparseVector1.data, sparseVector2.data))
  }
  
  /**
   * Concatenate two breeze sparse vectors
   */
  private[recsys] def concatBreezeSparseVector(sparseVector1:BSV[Double], sparseVector2:BSV[Double]): BSV[Double] = {
      val size:Int = sparseVector1.size + sparseVector2.size
      val values   = sparseVector1.data.slice(0, sparseVector1.activeSize)  ++ 
      				 sparseVector2.data.slice(0, sparseVector2.activeSize) 
      val indices  = sparseVector1.index.slice(0, sparseVector1.activeSize) ++ 
      				 sparseVector2.index.slice(0, sparseVector2.activeSize).map(_+sparseVector1.size)
      return new BSV[Double](indices, values, size)
  }
  
  /**
   * Concatenate two dense vectors into a new dense vector.  
   */
  def concatDenseVector(denseVector1:DenseVector, denseVector2: DenseVector): DenseVector = {
      new DenseVector(new BDV[Double](denseVector1.data.data ++ denseVector2.data.data))
  }
  
  /**
   * Adding a Breeze sparse vector to Breeze dense vector, and returns a Breeze dense vector
   */
  def addBreezeSparseDenseVector(sparseVector:BSV[Double], denseVector:BDV[Double]): BDV[Double] = {
      require(sparseVector.size == denseVector.size)
      
      val resultVector = denseVector.copy
      
      for ( (index, value) <-  sparseVector.activeIterator){
    	  resultVector(index) += value
      }
      
      resultVector
  }
}



/**
 * A dense vector represented by a value array.
 */
class DenseVector(val data:BDV[Double]) extends Vector {
  
  def this(values: Array[Double]) = this(BDV[Double](values))
  
  override def size: Int = data.length

  override def toString: String = data.data.mkString("RecSysDenseVector[", ",", "]")

  override def toArray: Array[Double] = data.toArray

  def toMLLib:SDV = {
     Vectors.breezeToMLLib(this.data)
  }
  
  override def apply(i: Int) = data(i)
  
  def ++(that:Vector):Vector = {
     that match{
       case v: SparseVector => Vectors.concatSparseVector(this.toSparse, v)
       case v: DenseVector  => Vectors.concatDenseVector (this,          v)
       case v => 
          sys.error("Unsupported vector type: " + v.getClass.getName)
     }
  }
  
  
  def +(that: Vector):Vector = {
     require(this.size == that.size)
     Vectors.fromBreeze(this.data + that.data)
  }
  
  def toSparse():SparseVector = {
     new SparseVector(Vectors.breezeDenseToSparse(this.data))
  }
  
  def toDense():DenseVector = {
     new DenseVector(this.data.copy)
  }
  
  def mapValues(f:Double => Double): DenseVector = {
     new DenseVector(this.data.map(f))
  }
  
  def copy():DenseVector = {
     new DenseVector(this.data.copy)
  }
}

/**
 * A sparse vector represented by an index array and an value array.
 *
 * @param size size of the vector.
 * @param indices index array, **assume to be strictly increasing**.
 * @param values value array, must have the same length as the index array.
 */
class SparseVector(val data:BSV[Double]) extends Vector{
  
	def this(size:Int, indices: Array[Int], values: Array[Double]) = this(new BSV[Double](indices, values, size))

    override val size: Int = data.length 
    
    override def toString: String = {
        "RecSysSparseVector(" + size + "," + data.index.zip(data.data).mkString("[", "," ,"]") + ")"
    }

    override def toArray: Array[Double] = data.toArray
    
    def toMLLib:SSV = {
        Vectors.breezeToMLLib(this.data)
    }
    
    def ++(that:Vector):Vector = {
       that match{
         case v: SparseVector => Vectors.concatSparseVector(this, v)
         case v: DenseVector  => Vectors.concatSparseVector(this, v.toSparse)
         case v => 
           sys.error("Unsupported vector type: " + v.getClass.getName)
       }
    }
    
    def +(that: Vector):Vector = {
       require(this.size == that.size)
       Vectors.fromBreeze(this.data + that.data)
    }
    
    def toSparse(): SparseVector = {
       new SparseVector(this.data.copy)
    }
    
    def toDense():DenseVector = {
       new DenseVector(this.data.toDenseVector)
    }
    
    def mapValues(f: Double => Double):SparseVector = {
       new SparseVector(this.data.mapActiveValues(f))
    }
    
    def copy():SparseVector = {
       new SparseVector(this.data.copy)
    }
}
