recsys-spark
============

A learning-to-rank recommender system built upon Apache Spark. This page shows how to set up development environment and [a wiki site](https://github.com/vddil/recsys-spark/wiki) is associated with this project, showing technical details. 

Technical Requirements
----------
In this project we use [Scala](http://www.scala-lang.org/download/) as the main development language, in which the [Spark](http://spark.apache.org/) is based on. To collaborate with the team to work on the project, you will also need to set up [Git](https://help.github.com/articles/set-up-git) in the local machine (to run this program in Yarn, Git should also be set up in the gateway machine in order to clone and pull code from GitHub). 

Getting Started
----------
The following are some installations that will help to learn Scala and Spark. Note that the two steps are not required to develop and run the project, and the Scala and Spark used in the project will be downloaded (separately)

1. Download and install [Scala](http://www.scala-lang.org/download/) to computer. The version of the Scala should be compatible with the Spark to be installed in the next step. For example, Spark 1.0.0 and Spark 0.91 use [Scala 2.10](http://www.scala-lang.org/download/2.10.4.html). After installation, type `scala` in command line should take you to the Scala interative shell, which is the best way to learn Scala. 
2. Download and install [Spark](http://spark.apache.org/). The latest Spark version up-to-date is [1.0.0](http://spark.apache.org/releases/spark-release-1-0-0.html). The Spark provides a Spark shell that includes an instance of `val sc:SparkContext`, which is the main entrance of Spark. The Spark shell is the best way to learn Spark. 



Setup Development Environment
----------
Currently the development environment is Eclipse. To use Eclipse as an IDE, 
follow these steps:

1. Download and install [Eclipse](https://www.eclipse.org/downloads/). The current latest one is [Kepler 4.3](https://www.eclipse.org/downloads/).
2. Install the following plugins to your Eclipse
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
3. Verify Git works fine with Eclipse (right click on any project and Git operations can be accessed in the `Team` context menu). Since we are using GitHub, it provides a nice native client for both [Windows](https://windows.github.com/) and [Mac](https://mac.github.com/). 
4. Check out the project to your workspace (e.g., `$HOME\workspace`)
5. In Eclipse, import the checked out project folder as an existing project (`File` > `Import` > `General` > `Existing Projects into Workspace`). 
6. If this is the first time Maven is used, then there are probably many missing libraries, which is normal because Maven does not automatically download necessary libraries for you. To fix download the missing libraries, simply build the project (Right click on the project folder > `Run As` > `Maven Build`. In the window poped up, type "install" into the `Goals`, and click `Run`). The maven should start to build and download all required libraries from Internet. 
7. In rare case the Eclipse Maven will fail due to some downloading issues. In such case, one solution to build outside the Maven. To do this, install Maven in command line, remove the corrupted maven downloads (`rm -r $HOME\.m2`) and navigate to the project folder, and run `mvn install`, and it should build.  

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



