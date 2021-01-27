package com.cloudera.spark.perf

import java.nio.file.Paths

import scala.util.{Random, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import com.databricks.spark.sql.perf.{Benchmark, Query, Variation}
import com.databricks.spark.sql.perf.tpch.TPCH
import com.databricks.spark.sql.perf.tpch.TPCHTables

object TPCHQueryBenchmark extends QueryBenchmark {
  override def name: String = "TPCHQueryBenchmark"

  override def ver: String = "0.1.0"

  def run(config: QueryBenchmarkConfig): Unit = {
    val spark = SparkSession.builder
      .appName("TPCHQueryBenchmark")
      .enableHiveSupport()
      .getOrCreate()

    import spark._

    try {
      val tpch = new TPCH(sqlContext)
      val queries = {
        val defaultQueries = if (config.runDefaultQueries) {
          tpch.queries(config.realExecutionMode, config.measureRuleTimes)
        } else {
          Seq.empty
        }
        val additionalQueries = config.additionalQueries.toSeq.flatMap { aq =>
          sparkContext.wholeTextFiles(aq).collect().map { case (file, sql) =>
            val queryName = Paths.get(file).getFileName().toString()
            new Query(queryName, sqlContext.sql(sql), description = s"Additional query - $file", Some(sql), config.realExecutionMode, config.measureRuleTimes)
          }
        }
        val allQueries = defaultQueries ++ additionalQueries
        val filteredQueries = if (config.queryFilter.nonEmpty) {
          allQueries.filter(q => config.queryFilter.contains(q.name))
        } else {
          allQueries
        }
        val orderedQueries = if (config.queryOrder.nonEmpty) {
          config.queryOrder.flatMap(name => filteredQueries.find(_.name == name))
        } else {
          filteredQueries
        }
        if (config.randomizeQueries) {
          Random.shuffle(orderedQueries)
        } else {
          orderedQueries
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

          val tables = new TPCHTables(sqlContext,
            dbgenDir = "/tmp/tpch-kit/tools", // dummy dbdgenDir
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

        val experiment = tpch.runExperiment(
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