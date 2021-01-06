package com.cloudera.spark.perf

import scopt.OptionParser

import com.databricks.spark.sql.perf.ExecutionMode

object QueryBenchmark {
  val dummyWriteParquet = ExecutionMode.WriteParquet("")
}

case class QueryBenchmarkConfig(
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
    executionMode: ExecutionMode = ExecutionMode.CollectResults,
    saveToParquetPath: String = "",
    configVariations: Option[Map[String, Seq[String]]] = None) {
  import QueryBenchmark._

  def realExecutionMode = if (executionMode == dummyWriteParquet) ExecutionMode.WriteParquet(saveToParquetPath) else executionMode
}

abstract class QueryBenchmark {
  import QueryBenchmark._

  def name: String

  def ver: String

  def main(args: Array[String]): Unit = {
    val executionModes = Seq(ExecutionMode.CollectResults, ExecutionMode.ForeachResults, dummyWriteParquet).map(em => em.toString -> em).toMap

    val parser = new OptionParser[QueryBenchmarkConfig](name) {
      head(name, ver)
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
        .text(s"the execution mode to use, default ${ExecutionMode.CollectResults.toString}")
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

    parser.parse(args, QueryBenchmarkConfig()) match {
      case Some(config) => run(config)
      case None => System.exit(1)
    }
  }

  def run(config: QueryBenchmarkConfig): Unit
}