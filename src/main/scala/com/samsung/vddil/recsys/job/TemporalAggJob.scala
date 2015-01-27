package com.samsung.vddil.recsys.job

import com.samsung.vddil.recsys.feature.item.{ItemFeatureGenreAgg, ItemFeatureGenreHourlyAgg}
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import scala.xml._

object TemporalAggJob {
 	val ResourceLoc_RoviHQ     = "roviHq"
	val ResourceLoc_WatchTime  = "watchTime"
	val ResourceLoc_Workspace  = "workspace"
	val ResourceLoc_JobFeature = "jobFeature"
	val ResourceLoc_JobData    = "jobData"
	val ResourceLoc_JobModel   = "jobModel"
	val ResourceLoc_JobTest    = "jobTest"
	val ResourceLoc_JobDir     = "job"
  val ResourceLoc_SchedWTime = "schedWTime"
}

case class TemporalAggJob (jobName:String, 
                            jobDesc:String, jobNode:Node) extends JobWithResource {
  val jobType = JobType.TemporalAgg
  val jobStatus:TemporalAggJobStatus = new TemporalAggJobStatus(this)
  
  val dateParser = new SimpleDateFormat("yyyyMMdd") // a parser/formatter for date. 
  
  /** a list of dates used to generate training data/features */
  val trainDates:Array[String] = populateTrainDates()
  
  
  def run():Unit = {
    //ItemFeatureGenreHourlyAgg.saveAggGenreHourly(this)
    ItemFeatureGenreAgg.saveAggGenreWeekly(this)
  }
  
  def getStatus():TemporalAggJobStatus = {
    return this.jobStatus
  }
    
  def generateXML():Option[Elem] = {
    None
  }
    
  /**
   * Populates training dates.
   * 
   * The dates are used to construct resource locations. The dates will be unique and sorted.
   * 
   * @return a list of date strings  
   */
  def populateTrainDates():Array[String] = {
    
    var dateList:Array[String] = Array[String]()
    
    //the element by element. 
    var nodeList = jobNode \ JobTag.RecJobTrainDateList
    if (nodeList.size == 0){
      Logger.warn("No training dates given!")
      return dateList.toArray
    }
    
    dateList = (nodeList(0) \ JobTag.RecJobTrainDateUnit).map(_.text).
          flatMap(expandDateList(_, dateParser)).  //expand the lists
          toSet.toArray.sorted                     //remove duplication and sort.
          
    Logger.info("Training dates: " + dateList.toArray.deep.toString 
        + " hash("+ HashString.generateHash(dateList.toArray.deep.toString) +")")
        
    return dateList
  }

 

}


case class TemporalAggJobStatus(jobInfo:TemporalAggJob) extends JobStatus {
  var status:HashMap[Any, Boolean] = new HashMap()
  this.status(TemporalAggJobStatus.COMPLETE_AGG) = false

  def allCompleted():Boolean = {
    this.status(TemporalAggJobStatus.COMPLETE_AGG)
  }

  def showStatus():Unit = {
    Logger.info("The status of current job [completed? %s]".format(allCompleted().toString))
  }

}


object TemporalAggJobStatus {
  val COMPLETE_AGG = "complete_aggregation"
}


