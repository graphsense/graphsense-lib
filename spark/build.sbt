import scala.io.Source._

val packagename = "graphsense-spark"
// used for local builds
val defaultVersion = fromFile("Makefile")
                      .getLines
                      .filter(_.startsWith("RELEASE"))
                      .toList
                      .headOption
                      .getOrElse("=Unknown")
                      .replaceAll("RELEASE := ", "")
                      .replaceAll("'", "")

// taken from https://alterationx10.com/2022/05/26/publish-to-github/
val tagWithQualifier: String => String => String =
  qualifier =>
    tagVersion => s"%s.%s.%s-${qualifier}%s".format(tagVersion.split("\\."): _*)

val tagAlpha: String => String = tagWithQualifier("a")
val tagBeta: String => String = tagWithQualifier("b")
val tagMilestone: String => String = tagWithQualifier("m")
val tagRC: String => String = tagWithQualifier("rc")

val versionFromTag: String = sys.env
  .get("GITHUB_REF_TYPE")
  .filter(_ == "tag")
  .flatMap(_ => sys.env.get("GITHUB_REF_NAME"))
  .flatMap { t =>
    t.headOption.map {
      case 'a' => tagAlpha(t.tail) // Alpha build, a1.2.3.4
      case 'b' => tagBeta(t.tail) // Beta build, b1.2.3.4
      case 'm' => tagMilestone(t.tail) // Milestone build, m1.2.3.4
      case 'r' => tagRC(t.tail) // RC build, r1.2.3.4
      case 'v' => t.tail // Production build, should be v1.2.3
      case _ => defaultVersion
    }
  }
  .getOrElse(defaultVersion)

ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "org.graphsense"
ThisBuild / version := versionFromTag
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / publishTo := Some(
  "GitHub Package Registry" at "https://maven.pkg.github.com/graphsense/" + packagename
)
ThisBuild / credentials += Credentials(
  "GitHub Package Registry", // realm
  "maven.pkg.github.com", // host
  "graphsense", // user
  sys.env.getOrElse("GITHUB_TOKEN", "thisisnottherealpassword") // password
)
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalafixOnCompile := true
ThisBuild / scalafmtOnCompile := true

lazy val root = (project in file(".")).
  settings(
    name := packagename,
    fork := true,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    scalacOptions ++= List(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint:_",
      "-Ywarn-adapted-args",
      "-Ywarn-dead-code",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-unused-import",
      "-Ywarn-value-discard"),
    resolvers += "SparkPackages" at "https://repos.spark-packages.org/",
    // Scope rationale (matters for `sbt assembly`):
    //   - The application's runtime deps below are plain (compile) scope so the
    //     assembly (fat) jar bundles them. This lets a fat-jar consumer run the
    //     job without passing them via spark-submit --packages, and in
    //     particular bundles graphframes (which lives on the spark-packages
    //     repo, not Maven Central) so consumers need no extra resolver.
    //   - Spark itself (spark-sql, spark-graphx) stays Provided: the cluster
    //     supplies it and it must NOT be bundled.
    //   - cassandra-analytics-core (optional Sidecar bulk-write path) stays
    //     Provided: it is large, only used with --writer sidecar, and is added
    //     via --packages when needed.
    // `sbt package` / `sbt publish` never bundle dependencies regardless of
    // scope, so the slim jar and the existing prod spark-submit flow (slim jar
    // + --packages) are unchanged by this.
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "com.github.mrpowers" % "spark-fast-tests_2.12" % "1.0.0" % Test,
      "org.rogach" %% "scallop" % "4.1.0",
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
      "joda-time" % "joda-time" % "2.10.10",
      "org.web3j" % "core" % "4.8.7",
      "org.web3j" % "abi" % "4.8.7",
      "org.apache.spark" %% "spark-sql" % "3.5.8" % Provided,
      "org.apache.spark" %% "spark-graphx" % "3.5.8" % Provided,
      "graphframes" % "graphframes" % "0.8.3-spark3.5-s_2.12",
      "org.apache.cassandra" % "cassandra-analytics-core_spark3_2.12" % "0.3.0" % Provided),
    // Fat-jar (assembly) configuration. Scala is provided by the Spark cluster,
    // so it is excluded to keep the jar smaller and avoid classpath shadowing.
    assembly / assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")      => MergeStrategy.discard
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", "native", _ @ _*)   => MergeStrategy.first
      case "reference.conf"                         => MergeStrategy.concat
      case "application.conf"                        => MergeStrategy.concat
      case x if x.endsWith("module-info.class")     => MergeStrategy.discard
      case x =>
        val previous = (assembly / assemblyMergeStrategy).value
        previous(x)
    },
    javaOptions ++= Seq(
      "-Xms8g",
      "-Xmx8g",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-exports=java.base/sun.misc=ALL-UNNAMED"
    )
  )
