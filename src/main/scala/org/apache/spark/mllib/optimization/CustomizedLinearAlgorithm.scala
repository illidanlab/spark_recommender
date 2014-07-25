package org.apache.spark.mllib.optimization

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Logging, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import scala.collection.immutable.HashMap
import org.apache.spark.mllib.regression.LabeledPoint


abstract class CustomizedLinearModel(
        val weights:Vector, 
        val intercept: Double, 
        val param:HashMap[String, Any])
	extends Serializable{
    
    /**
     * Predict the result given a data point and the weights learned.
     *
     * @param dataMatrix Row vector containing the features for this data point
     * @param weightMatrix Column vector containing the weights of the model
     * @param intercept Intercept of the model.
     */
    protected def predictPoint(dataMatrix: Vector, weightMatrix: Vector, intercept: Double): Double

    /**
     * Predict values for the given data set using the model trained.
     *
     * @param testData RDD representing data points to be predicted
     * @return RDD[Double] where each entry contains the corresponding prediction
     */
    def predict(testData: RDD[Vector]): RDD[Double] = {
        // A small optimization to avoid serializing the entire model. Only the weightsMatrix
        // and intercept is needed.
        val localWeights = weights
        val localIntercept = intercept

        testData.map(v => predictPoint(v, localWeights, localIntercept))
    }

    /**
     * Predict values for a single data point using the model trained.
     *
     * @param testData array representing a single data point
     * @return Double prediction from the trained model
     */
    def predict(testData: Vector): Double = {
        predictPoint(testData, weights, intercept)
    }
    
    
    
}



abstract class CustomizedLinearAlgorithm [M <: CustomizedLinearModel]
	extends Logging with Serializable{
	
    protected val validators: Seq[RDD[LabeledPoint] => Boolean] = List()

    /** The optimizer to solve the problem. */
    def optimizer: Optimizer

    /** Whether to add intercept (default: false). */
    protected var addIntercept: Boolean = false

    protected var validateData: Boolean = true

    /**
     * Create a model given the weights and intercept
     */
    protected def createModel(weights: Vector, intercept: Double, param:HashMap[String, Any]): M

    /**
     * Set if the algorithm should add an intercept. Default false.
     * We set the default to false because adding the intercept will cause memory allocation.
     */
    def setIntercept(addIntercept: Boolean): this.type = {
        this.addIntercept = addIntercept
        this
    }

    /**
     * Set if the algorithm should validate data before training. Default true.
     */
    def setValidateData(validateData: Boolean): this.type = {
        this.validateData = validateData
        this
    }

    /** Prepends one to the input vector. */
    private def prependOne(vector: Vector): Vector = {
      val vector1 = vector.toBreeze match {
        case dv: BDV[Double] => BDV.vertcat(BDV.ones[Double](1), dv)
        case sv: BSV[Double] => BSV.vertcat(new BSV[Double](Array(0), Array(1.0), 1), sv)
        case v: Any => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
      Vectors.fromBreeze(vector1)
    }

  /**
   * Run the algorithm with the configured parameters on an input RDD
   * of LabeledPoint entries starting from the initial weights provided.
   */
  def run(input: RDD[LabeledPoint], initialWeights: Vector, param:HashMap[String, Any]): M = {

    // Check the data properties before running the optimizer
    if (validateData && !validators.forall(func => func(input))) {
      throw new SparkException("Input validation failed.")
    }

    // Prepend an extra variable consisting of all 1.0's for the intercept.
    val data = if (addIntercept) {
      input.map(labeledPoint => (labeledPoint.label, prependOne(labeledPoint.features)))
    } else {
      input.map(labeledPoint => (labeledPoint.label, labeledPoint.features))
    }

    val initialWeightsWithIntercept = if (addIntercept) {
      prependOne(initialWeights)
    } else {
      initialWeights
    }

    val weightsWithIntercept = optimizer.optimize(data, initialWeightsWithIntercept)

    val intercept = if (addIntercept) weightsWithIntercept(0) else 0.0
    val weights =
      if (addIntercept) {
        Vectors.dense(weightsWithIntercept.toArray.slice(1, weightsWithIntercept.size))
      } else {
        weightsWithIntercept
      }

    createModel(weights, intercept, param)
  }
    
}