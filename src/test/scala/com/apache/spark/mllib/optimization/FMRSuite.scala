package com.apache.spark.mllib.optimization

import scala.util.Random
import scala.collection.JavaConversions._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.mllib.optimization.FactorizationMachineRegressionModel
import org.apache.spark.mllib.optimization.GradientDescent
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.optimization.LeastSquaresGradient


object FactorizationMachineRegressionSuite extends {
    
	def generateLogisticInputAsList(
			offset: Double,
			scale: Double,
			nPoints: Int,
			dim:Int,
			seed: Int): java.util.List[LabeledPoint] = {
	    
		seqAsJavaList(generateGDInput(offset, scale, nPoints, dim, seed))
	}
    /**
     * This label is random. 
     */
	def generateGDInput(
			offset: Double,
			scale: Double,
			nPoints: Int,
			dim:Int,
			seed: Int): Seq[LabeledPoint]  = {
		val rnd = new Random(seed)
		
		val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())
		
		val unifRand = new scala.util.Random(45)
		val rLogis = (0 until nPoints).map { i =>
			val u = unifRand.nextDouble()
			math.log(u) - math.log(1.0-u)
		}

		val y: Seq[Int] = (0 until nPoints).map { i =>
			val yVal = offset + scale * x1(i) + rLogis(i)
			if (yVal > 0) 1 else 0
		}

		(0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(Array.fill[Double](dim)(rnd.nextGaussian()))))
	}
	
	def main(args:Array[String]):Unit = {
	    var sc: SparkContext = new SparkContext("local", "test")
	    val rnd = new Random(52)
	    
	    
	    
	    
	    val nPoints = 10000
	    val dimension = 55
	    val latentDim = 5
	    val modelSize = dimension * (latentDim + 1) + 1
	    val A = 2.0
	    val B = -1.5
	    
	    
	    val gradient = new FactorizationMachineRegressionModel.FactorizationMachineGradient(latentDim)
	    val initialWeights = Vectors.dense(Array.fill[Double](modelSize)(rnd.nextGaussian()))
	    	    
	    
	    //val updater  = new SimpleUpdater()
	    val updater  = new FactorizationMachineRegressionModel.FactorizationMachineL2Update(latentDim)
	    
	    // Add a extra variable consisting of all 1.0's for the intercept.
	    val testData = FactorizationMachineRegressionSuite.generateGDInput(A, B, nPoints, dimension, 42)
	    val data = testData.map { case LabeledPoint(label, features) =>
	      label -> Vectors.dense(features.toArray)
	    }
	    
	    println("Sample size: "+ data.size)
	    println("Dimension:   "+ data.head._2.size)
	    
	    
	    
	    

	    
	    
	    val stepSize = 0.0001//1.0
	    val numIterations = 10
	    val regParam = 0
	    val miniBatchFrac = 1.0
	
	    
	
	    val dataRDD = sc.parallelize(data, 2).cache()

	    val (_, loss) = GradientDescent.runMiniBatchSGD(
	    		dataRDD,
	    		gradient,
	    		updater,
	    		stepSize,
	    		numIterations,
	    		regParam,
	    		miniBatchFrac,
	    		initialWeights)
	    		
	    sc.stop()
	    System.clearProperty("spark.driver.port")
	}
}