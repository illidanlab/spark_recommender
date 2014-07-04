package com.samsung.vddil.recsys


import com.samsung.vddil.recsys._
import com.samsung.vddil.recsys.job._
import org.apache.log4j.PropertyConfigurator


/**
 * This is the main entrance of the program. Due to historical reasons. 
 * 
 * @author jiayu.zhou
 */
object TestObj {

  def main(args: Array[String]): Unit = {
		PropertyConfigurator.configure("log4j.properties")
		
		val logger = Logger.logger
		
		//Read config file
		var cfgFileStr:String = "local_cfg.xml"
		if (args.size > 1){
		   cfgFileStr = args(1)
		   logger.info("Config file specified: " + cfgFileStr)
		}else{
		  logger.warn("No config file specified. Used default job file: " + cfgFileStr)
		}
		
		Pipeline.config()
		
		// only proceed to jobs if pipeline is properly configured. 
		if (Pipeline.instance.isDefined){
		
			//Read job file
			var jobFileStr:String = "test_job.xml" 
			if (args.size > 0){
			  jobFileStr = args(0)
			  logger.info("Job file specified: " + jobFileStr)
			}else{
			  logger.warn("No job file specified. Used default job file: " + jobFileStr)
			}
			
			//Process job file
			val jobList : List[Job] = Job.readFromXMLFile(jobFileStr,
                                                    Pipeline.instance.get.sc)
			
			for (job:Job <- jobList){
			   logger.info("=== Running job: " + job + " ===")
			   job.run()
			   job.getStatus().showStatus()
			   logger.info("=== Job Done: " + job + " ===")
			}
			
			logger.info("All jobs are completed.")
		}else{
			logger.info("Pipeline exited as configuration fails.")
		}
		
  }

}
