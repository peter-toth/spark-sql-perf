package com.cloudera.spark.perf

import java.nio.file.Paths

import scala.util.{Random, Try}

import scopt.OptionParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import com.databricks.spark.sql.perf.ExecutionMode.{CollectResults, ForeachResults, WriteParquet}
import com.databricks.spark.sql.perf.{Benchmark, ExecutionMode, Query, Variation}
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

case class TPCDSQueryBenchmarkConfig(
    database: String = null,
    location: String = null,
    dropDatabase: Boolean = false,
    iterations: Int = 1,
    resultLocation: String = null,
    randomizeQueries: Boolean = false,
    timeout: Int = 1,
    analyzeTables: Boolean = false,
    queryFilter: Seq[String] = Seq.empty,
    runDefaultQueries: Boolean = true,
    additionalQueries: Option[String] = None,
    measureRuleTimes: Boolean = false,
    executionMode: ExecutionMode = CollectResults,
    saveToParquetPath: String = "",
    configVariations: Option[Map[String, Seq[String]]] = None) {
  def realExecutionMode = if (executionMode == TPCDSQueryBenchmark.dummyWriteParquet) WriteParquet(saveToParquetPath) else executionMode
}

object TPCDSQueryBenchmark {
  val dummyWriteParquet = WriteParquet("")

  def main(args: Array[String]): Unit = {
    val executionModes = Seq(CollectResults, ForeachResults, dummyWriteParquet).map(em => em.toString -> em).toMap

    val parser = new OptionParser[TPCDSQueryBenchmarkConfig]("TPCDSQueryBenchmark") {
      head("TPCDSQueryBenchmark", "0.1.0")
      opt[String]('d', "database")
        .action((x, c) => c.copy(database = x))
        .text("the database prefix to use")
        .required()
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = x))
        .text("the location of the input data")
        .required()
      opt[String]('r', "resultLocation")
        .action((x, c) => c.copy(resultLocation = x))
        .text("the location of the benchmark results")
        .required()
      opt[Boolean]("dropDatabase")
        .action((x, c) => c.copy(dropDatabase = x))
        .text("set to true if benchmark database should be dropped and rebuilt before running queries, default false")
      opt[Int]("iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to use, default 1")
      opt[Boolean]("randomizeQueries")
        .action((x, c) => c.copy(randomizeQueries = x))
        .text("set to true if queries should be randomized, default false")
      opt[Int]("timeout")
        .action((x, c) => c.copy(timeout = x))
        .text("the benchmark timeout in hours, default 1")
      opt[Boolean]("analyzeTables")
        .action((x, c) => c.copy(analyzeTables = x))
        .text("set to true if tables should be analized (CBO needs it) before running queries, default false")
      opt[Seq[String]]("queryFilter")
        .action((x, c) => c.copy(queryFilter = x))
        .text("the list of query names separated with comma to filter queries, default empty")
      opt[Boolean]("runDefaultQueries")
        .action((x, c) => c.copy(runDefaultQueries = x))
        .text("set to false if default TPCDS 2.4 queries shouldn't run, default true")
      opt[String]("additionalQueries")
        .action((x, c) => c.copy(additionalQueries = Some(x)))
        .text("a file or a directory that contains additional queries to run, default empty")
      opt[Boolean]("measureRuleTimes")
        .action((x, c) => c.copy(measureRuleTimes = x))
        .text("set to true if rule times should be measured, default false")
      opt[String]("executionMode")
        .validate(x => if (executionModes.contains(x)) success else failure(s"executionMode should be one of ${executionModes.keys.mkString("\"", ", ", "\"")}"))
        .action((x, c) => c.copy(executionMode = executionModes(x)))
        .text(s"the execution mode to use, default ${CollectResults.toString}")
      opt[String]("saveToParquetPath")
        .action((x, c) => c.copy(saveToParquetPath = x))
        .text(s"the path where $dummyWriteParquet execution mode should store the results")
      opt[Map[String, String]]("configVariations")
        .action((x, c) => c.copy(configVariations = Some(x.mapValues(_.split('|')))))
        .text(s"the config variations to run the queries with, default empty")
      checkConfig(c => if (c.executionMode == dummyWriteParquet && c.saveToParquetPath == null) failure(s"writeParquetPath should be defined when executionMode is $dummyWriteParquet") else success)
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, TPCDSQueryBenchmarkConfig()) match {
      case Some(config) => run(config)
      case None => System.exit(1)
    }
  }

  def run(config: TPCDSQueryBenchmarkConfig): Unit = {
    val spark = SparkSession.builder
      .appName("TPCDSQueryBenchmark")
      .enableHiveSupport()
      .getOrCreate()

    import spark._

    try {
      val tpcds = new TPCDS(sqlContext)
      val queries = {
        val queries = if (config.runDefaultQueries) {
          tpcds.tpcds2_4Queries(config.realExecutionMode, config.measureRuleTimes)
        } else {
          Seq.empty
        } ++ config.additionalQueries.toSeq.flatMap { aq =>
          sparkContext.wholeTextFiles(aq).collect().map { case (file, sql) =>
            val queryName = Paths.get(file).getFileName().toString()
            new Query(queryName, sqlContext.sql(sql), description = s"Additional query - $file", Some(sql), config.realExecutionMode, config.measureRuleTimes)
          }
        }
        val filteredQueries = if (config.queryFilter.nonEmpty) {
          queries.filter(q => config.queryFilter.contains(q.name))
        } else {
          queries
        }
        if (config.randomizeQueries) {
          Random.shuffle(filteredQueries)
        } else {
          filteredQueries
        }
      }
      if (queries.nonEmpty) {
        val database = config.database

        val databaseExists = if (config.dropDatabase) {
          sql(s"DROP DATABASE IF EXISTS $database CASCADE")
          false
        } else {
          Try {
            sql(s"USE $database")
            true
          }.getOrElse(false)
        }

        if (!databaseExists) {
          sql(s"CREATE DATABASE $database")

          val tables = new TPCDSTables(sqlContext,
            dsdgenDir = "/tmp/tpcds-kit/tools", // dummy dsdgenDir
            scaleFactor = "5") // dummy scaleFactor

          // Create metastore tables in a specified database for your data.
          // Once tables are created, the current database will be switched to the specified database.
          tables.createExternalTables(config.location, "parquet", database, overwrite = true, discoverPartitions = true)
          // Or, if you want to create temporary tables
          // tables.createTemporaryTables(location, format)

          // For CBO only, gather statistics on all columns:
          if (config.analyzeTables) {
            tables.analyzeTables(database, analyzeColumns = true)
          }
        }

        sql(s"USE $database")

        val experiment = tpcds.runExperiment(
          queries,
          iterations = config.iterations,
          variations = config.configVariations.map(_.toSeq.map { case (key, values) =>
            Variation(s"ConfigVariation $key", values) { case value => sqlContext.setConf(key, value) }
          }).getOrElse(Benchmark.standardRun),
          tags = Map(("database" -> database) +:
            config.getClass.getDeclaredFields.map(_.getName).zip(config.productIterator.map(_.toString).to): _*),
          resultLocation = config.resultLocation)

        println(experiment.toString)

        experiment.waitForFinish(config.timeout * 60 * 60)

        experiment.getCurrentRuns
          .drop("results")
          .show(1000, false)

        experiment.getCurrentResults
          .drop("queryExecution")
          .drop("ruleTimeSpent")
          .show(1000, false)

        experiment.getCurrentResults
          .withColumn("runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
          .select("name", "parameters", "runtime")
          .show(1000, false)
      } else {
        println("No queries to run.")
      }
    } catch { case e =>
      e.printStackTrace()
    } finally {
      spark.stop()
      System.exit(0)
    }
  }
}