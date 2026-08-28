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
      // Compile against the Java 8 API, whatever JDK is running the build.
      // Scala 2.12 always emits Java 8 bytecode, so the target never varied —
      // but without this the source is checked against the builder's class
      // library, and a Java 9+ stdlib call would compile on a 17 runner and
      // throw NoSuchMethodError on the Java 11 executors. The source is
      // already Java 8 API-clean, so this costs nothing and turns the property
      // into something the compiler enforces rather than something the CI
      // environment happens to provide.
      "-release:8",
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
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "com.github.mrpowers" % "spark-fast-tests_2.12" % "1.0.0" % Test,
      "org.rogach" %% "scallop" % "4.1.0",
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
      "joda-time" % "joda-time" % "2.10.10",
      // Only org.web3j.abi is used (Tokens.scala: EventEncoder,
      // FunctionReturnDecoder, TypeReference, datatypes.Event). `core` was
      // declared but never imported, and it is what dragged okhttp, okio,
      // Java-WebSocket, kotlin-stdlib and bouncycastle into the assembly jar —
      // all carrying open advisories with nothing here calling them. Do not
      // re-add it without an import to justify it.
      //
      // Pinned at 4.8.7 deliberately: web3j moved to Java 17 bytecode at
      // 4.10.0 and Java 21 at 4.14.0, and this builds on Scala 2.12.17 for a
      // Java 11 cluster. 4.14.0 fails at `typer` with "bad constant pool
      // index: 0" and would not load on the executors either. 4.9.8 is the
      // last Java 8 build, but abi's only dependency is org.web3j:utils at
      // every version, so a bump buys no advisory coverage.
      "org.web3j" % "abi" % "4.8.7" exclude ("org.bouncycastle", "bcprov-jdk15on"),
      // web3j:utils pins bcprov-jdk15on 1.68, and that coordinate ends at 1.70:
      // BouncyCastle renamed the artifact to bcprov-jdk18on at 1.71, which is
      // where the fix for GHSA advisories on >= 1.61, < 1.78 lives. Bumping
      // web3j cannot reach it (4.9.8, the last Java 8 build, still pins 1.70),
      // so the old coordinate is excluded and the new one added explicitly.
      // Same org.bouncycastle.* packages, so it is a drop-in; it must stay on
      // the classpath because EventEncoder hashes the event signature through
      // org.web3j.crypto.Hash, which calls BouncyCastle's Keccak digest.
      "org.bouncycastle" % "bcprov-jdk18on" % "1.80",
      // Pulled transitively at 3.10 by spark-cassandra-connector-driver, inside
      // the advisory range >= 3.0, < 3.18.0. Declared directly rather than as a
      // dependencyOverride so that sbt's latest-revision conflict manager picks
      // 3.18.0 *and* the coordinate reaches config.py's package list — an
      // override is invisible to scripts/check_spark_packages.py, which would
      // leave the slim --packages path resolving 3.10 while the assembly jar
      // carried 3.18.0. Nothing here imports it; 3.20.0 exists and is equally
      // Java 8, but 3.18.0 is the smallest step that clears the advisory.
      "org.apache.commons" % "commons-lang3" % "3.18.0",
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
