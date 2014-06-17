recsys-spark
============

A learning-to-rank recommender system built upon Apache Spark. This page shows how to set up development environment and [a wiki](https://github.com/vddil/recsys-spark/wiki) is associated with this project, showing technical details. 


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
  * Install [m2eclipse-scala](https://github.com/sonatype/m2eclipse-scala). This allows you to work with Maven in Scala project. The installation can be done by cloning `https://github.com/sonatype/m2eclipse-scala.git` to your Eclipse `dropins` folder.

### Create Projects from Scrach
If you are creating a Spark project from scratch in GitHub and want to use Eclipse, the **most efficient way** (hold before you finish reading the entire section) to do this is as follows:
  1.  Create a GitHub repository in GitHub
  2.  Clone the repository using git client on local machine
  3.  Create a plain Scala (or Python) project using the folder cloned from GitHub. The Eclipse should be able to recognize the folder is related to a repository (additional cylinder in the icon).  
  4.  Add a Maven dependency on `spark-core` of corresponding version. To do this, right click the project, and choose `Configure` > `Convert to Maven Project` and follow the instructions to add dependencies. To use Spark 1.0.0, the Maven information is `groupId = org.apache.spark`, `artifactId = spark-core_2.10`, `version = 1.0.0`. 

**However**, this may impose some problems for deployment because it may not compile a jar file (according to Yuan Zhang). Therefore an alternative is to use Maven project:
  1.  Create a GitHub repository in GitHub
  2.  Clone the repository using git client on local machine
  3.  Create a Maven project, use the folder name cloned from GitHub as the artifact name. The Eclipse should be able to recognize the folder is related to a repository (additional cylinder in the icon).  
  4.  Add Scala nature by `Configure` > `Add Scala Nature` to enable Scala. To this end, you should be able to compile Scala files.  
  5.  To use Spark 1.0.0, add dependency `groupId = org.apache.spark`, `artifactId = spark-core_2.10`, `version = 1.0.0`. Since the `spark-core_2.10` dependency already includes Scala, we may remove the Scala Library added by Eclipse in Step (4) from `Java Build Path` in the project `Properties`. 
  6.  More often than not the default Maven JVM setting `J2SE-1.5` may not have the same compatible level as the system, and there may be a disturbing warning. Follow [this page](http://stackoverflow.com/questions/14804945/maven-build-path-specifies-execution-environment-j2se-1-5-even-though-i-chang) to adjust the `compiler compliance level` in `Java Compiler` in the project `Properties`. 
  7.  The the `pom.xml` file must be set-up according to project requirement. One reference is the one in this project. This might be the most time consuming part, and in case it is not working, copy and paste the one in this project with minor changes (group ID, artifact ID and etc). 



