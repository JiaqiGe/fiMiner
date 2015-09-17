
name := "Apriori"

version := "1.1"


scalaVersion:="2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating"

//libraryDependencies += "org.apache.jmeter" %% "jorphan" % "2.11"
//resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/"
)


// assemblySettings
//
// excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
//   cp filter { jar =>
//     jar.data.getName.matches("commons-logging.*")
//   }
// }




//lazy val buildSettings = Seq(
//  version := "0.1-SNAPSHOT",
//  organization := "io.tranwarp",
//  scalaVersion := "2.10.4"
//)
//
//val app = (project in file("app")).
//  settings(buildSettings: _*).
//  settings(assemblySettings: _*).
//  settings(
//    // your settings here
//  )

