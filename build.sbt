 import AssemblyKeys._
 
 assemblySettings

 jarName in assembly := "samsung-recsys-assembly.jar"

 name := "Samsung VD Recommender System"

 version := "1.0"

 scalaVersion := "2.10.4"

 libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.1"

 libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.1"

 libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"

 libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

 resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

 resolvers ++= Seq(
            // other resolvers here
            // if you want to use snapshot builds (currently 0.8-SNAPSHOT), use this.
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
 )

 mergeStrategy in assembly := {
  case n if n.startsWith("com/esotericsoftware/minlog") => MergeStrategy.first
  case n if n.startsWith("javax/activation") => MergeStrategy.first
  case n if n.startsWith("javax/servlet/") => MergeStrategy.first
  case n if n.startsWith("org/apache/commons/") => MergeStrategy.first
  case n if n.startsWith("org/apache/hadoop/") => MergeStrategy.first
  case n if n.startsWith("plugin.properties") => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.first
  case n if n.startsWith("rootdoc.txt") => MergeStrategy.first
  case n if n.startsWith("scala/reflect/api/Liftable") => MergeStrategy.first
  case n if n.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.discard
  case n if n.startsWith("META-INF/ECLIPSEF.SF") => MergeStrategy.discard
  case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
  case n if n.startsWith("META-INF/NOTICE.txt") => MergeStrategy.discard
  case n if n.startsWith("META-INF/NOTICE") => MergeStrategy.discard
  case n if n.startsWith("META-INF/LICENSE.txt") => MergeStrategy.discard
  case n if n.startsWith("META-INF/LICENSE") => MergeStrategy.discard
  case n if n.startsWith("META-INF/DEPENDENCIES") => MergeStrategy.discard 
  case n if n.startsWith("META-INF/INDEX.LIST") => MergeStrategy.discard  
  case n if n.startsWith("META-INF/mailcap") => MergeStrategy.discard    
  case n if n.startsWith("META-INF/mimetypes.default") => MergeStrategy.discard   
  case n if n.startsWith("META-INF/services") => MergeStrategy.discard   
  case n if n.startsWith("readme.html") => MergeStrategy.discard
  case n if n.startsWith("readme.txt") => MergeStrategy.discard
  case n if n.startsWith("library.properties") => MergeStrategy.discard
  case n if n.startsWith("license.html") => MergeStrategy.discard
  case n if n.startsWith("about.html") => MergeStrategy.discard
  case n if n.endsWith("LICENSE.txt") => MergeStrategy.first
  case n if n.endsWith(".txt") => MergeStrategy.first
  case _ => MergeStrategy.deduplicate
}
