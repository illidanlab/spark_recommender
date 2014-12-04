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
                            jobDesc:String, jobNode:Node) extends Job {
  val jobType = JobType.TemporalAgg
  val jobStatus:TemporalAggJobStatus = new TemporalAggJobStatus(this)
  
  /** an instance of SparkContext created according to user specification */
  val sc:SparkContext = Pipeline.instance.get.sc
  
  //populate resources
  val resourceLoc:HashMap[String, String] = populateResourceLoc() 
  
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
   * Populates special resource locations.
   * 
   * @return a map whose keys are given by 
   *    [[TemporalAggJob.ResourceLoc_RoviHQ]],
   *    [[TemporalAggJob.ResourceLoc_WatchTime]],
   *    [[TemporalAggJob.ResourceLoc_Workspace]],
   *    [[TemporalAggJob.ResourceLoc_JobData]],
   *    [[TemporalAggJob.ResourceLoc_JobFeature]],
   *    [[TemporalAggJob.ResourceLoc_JobModel]],
   *    and values are double.  
   */
  def populateResourceLoc():HashMap[String, String] = {
     var resourceLoc:HashMap[String, String] = new HashMap()
     
     var nodeList = jobNode \ JobTag.RecJobResourceLocation
     if (nodeList.size == 0){
        Logger.error("Resource locations are not given. ")
        return resourceLoc
     }
     
     
     if ((nodeList(0) \ JobTag.RecJobResourceLocationRoviHQ).size > 0) 
       resourceLoc(TemporalAggJob.ResourceLoc_RoviHQ)     = (nodeList(0) \ JobTag.RecJobResourceLocationRoviHQ).text
     
     if ((nodeList(0) \ JobTag.RecJobResourceLocationWatchTime).size > 0) 
       resourceLoc(TemporalAggJob.ResourceLoc_WatchTime)  = (nodeList(0) \ JobTag.RecJobResourceLocationWatchTime).text
     
     if ((nodeList(0) \ JobTag.RecJobResourceSchedLocation).size > 0) 
       resourceLoc(TemporalAggJob.ResourceLoc_SchedWTime)  = (nodeList(0) \ JobTag.RecJobResourceSchedLocation).text
     
     if ((nodeList(0) \ JobTag.RecJobResourceLocationWorkspace).size > 0) { 
       resourceLoc(TemporalAggJob.ResourceLoc_Workspace)  = (nodeList(0) \ JobTag.RecJobResourceLocationWorkspace).text
       //derivative
       resourceLoc(TemporalAggJob.ResourceLoc_JobDir)     = resourceLoc(TemporalAggJob.ResourceLoc_Workspace) + "/"  + jobName
       resourceLoc(TemporalAggJob.ResourceLoc_JobData)    = resourceLoc(TemporalAggJob.ResourceLoc_JobDir) + "/data"
       resourceLoc(TemporalAggJob.ResourceLoc_JobFeature) = resourceLoc(TemporalAggJob.ResourceLoc_JobDir) + "/feature"
       resourceLoc(TemporalAggJob.ResourceLoc_JobModel)   = resourceLoc(TemporalAggJob.ResourceLoc_JobDir) + "/model"
       resourceLoc(TemporalAggJob.ResourceLoc_JobTest)    = resourceLoc(TemporalAggJob.ResourceLoc_JobDir) + "/test"
     }
     
     Logger.info("Resource WATCHTIME:   " + resourceLoc(TemporalAggJob.ResourceLoc_WatchTime))
     Logger.info("Resource ROVI:        " + resourceLoc(TemporalAggJob.ResourceLoc_RoviHQ))
     Logger.info("Resource Job Data:    " + resourceLoc(TemporalAggJob.ResourceLoc_JobData))
     Logger.info("Resource Job Feature: " + resourceLoc(TemporalAggJob.ResourceLoc_JobFeature))
     Logger.info("Resource Job Model:   " + resourceLoc(TemporalAggJob.ResourceLoc_JobModel))
     resourceLoc
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


