recsys-spark
============

A recommender system built upon Apache Spark. 


Setup Development Environment
----------
Currently the development environment is Eclipse. To use Eclipse as an IDE, 
follow these steps:

1. Download and install [Scala](http://www.scala-lang.org/download/) to computer. The version of the Scala should be compatible with the Spark to be installed in the next step. For example, Spark 1.0.0 and Spark 0.91 use [Scala 2.10](http://www.scala-lang.org/download/2.10.4.html)
2. Download and install [Spark](http://spark.apache.org/). The latest Spark version up-to-date is [1.0.0](http://spark.apache.org/releases/spark-release-1-0-0.html). 
3. Download and install [Eclipse](https://www.eclipse.org/downloads/). The current latest one is [Kepler 4.3](https://www.eclipse.org/downloads/).
4. Install the following plugins to your Eclipse
  * Install [Scala IDE](http://scala-ide.org/download/current.html). Recommended installation is via Update Site in the Eclipse (`Help` > `Install New Software...` > `Add...`). Note that the version of Scala IDE should be consistent with the Scala version to be worked with Spark. For Scala 2.10.4, the Scala IED 3.03/3.04 update site is 
  ```
  http://download.scala-ide.org/sdk/helium/e38/scala210/stable/site
  ```
  <!--- * Install [Maven for Eclipse plugin]() --->
  * Install [m2eclipse](http://eclipse.org/m2e/download/). The Spark project is managed by the [Maven build system](http://maven.apache.org/pom.html). Recommended installation is dragging the installiation icon
  <a href='http://marketplace.eclipse.org/marketplace-client-intro?mpc_install=252' title='Drag and drop into a running Eclipse Indigo workspace to install Maven Integration for Eclipse'> 
	<img src='http://marketplace.eclipse.org/misc/installbutton.png'/>
    </a>in the Eclipse workspace, and the installation dialogue will pop up.  
5. If you are creating a Spark project from scratch in GitHub and want to use Eclipse, the correct way to do this is as follows:
  1.  Create a GitHub repository in GitHub
  2.  Clone the repository using git client on local machine
  3.  Create a plain Scala (or Python) project using the folder cloned from GitHub. The Eclipse should be able to recognize the folder is related to a repository (additional cylinder in the icon).  
  4.  Add a Maven dependency on `spark-core` of corresponding version. To do this, right click the project, and choose `Configure` > `Convert to Maven Project` and follow the instructions to add dependencies. To use Spark 1.0.0, the Maven information is `groupId = org.apache.spark`, `artifactId = spark-core_2.10`, `version = 1.0.0`. 
    

