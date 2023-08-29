package indexeddataframe

import indexeddataframe.execution.IndexedOperatorExec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import indexeddataframe.logical.ConvertToIndexedOperators
import indexeddataframe.implicits._

object TestOnTPCDS {
  def triggerExecutionIndexedDF(df: DataFrame) = {
    val plan = df.queryExecution.executedPlan
      .asInstanceOf[IndexedOperatorExec]
      .executeIndexed()
    plan.foreachPartition(p => println("indexed part size = " + p.length))
  }

  def createIndexAndCache(df: DataFrame, colNo: Int): DataFrame = {
    val indexed = df.createIndex(colNo).cache()
    triggerExecutionIndexedDF(indexed)
    indexed
  }

  val sqlText =
    """
      |select i_item_desc
      |       ,w_warehouse_name
      |       ,d1.d_week_seq
      |       ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      |       ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      |       ,count(*) total_cnt
      | from catalog_sales
      | join inventory on (cs_item_sk = inv_item_sk)
      | join warehouse on (w_warehouse_sk = inv_warehouse_sk)
      | join item on (i_item_sk = cs_item_sk)
      | join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
      | join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
      | join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
      | join date_dim d2 on (inv_date_sk = d2.d_date_sk)
      | join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
      | left outer join promotion on (cs_promo_sk = p_promo_sk)
      | left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
      | where d1.d_week_seq = d2.d_week_seq
      |   and inv_quantity_on_hand < cs_quantity
      |   and d3.d_date > (cast(d1.d_date AS DATE) + interval '5' day)
      |   and hd_buy_potential = '>10000'
      |   and d1.d_year = 1999
      |   and cd_marital_status = 'D'
      | group by i_item_desc,w_warehouse_name,d1.d_week_seq
      | order by total_cnt desc, i_item_desc, w_warehouse_name, d1.d_week_seq
      | limit 100
      |""".stripMargin

  // val sqlText =
  //   """
  //   |select * from store_sales
  //   |join date_dim on (ss_sold_date_sk = d_date_sk)
  //   |order by ss_quantity
  //   |limit 100
  //   """.stripMargin

  private def measureTimeInMS[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf
      .set("spark.master", "yarn-client")
      .set("spark.hadoop.fs.defaultFS", "hdfs://s44:8020")
      .set("spark.hadoop.yarn.resourcemanager.address", "s44")
      .set("spark.yarn.jars", "hdfs://s44:8020/spark-2.4/jars/*")
      .set("spark.app.name", "TestSQL-Index-Inventory-0-Catagory-Returns-1")
      .set("spark.driver.memory", "2G")
      .set("spark.driver.maxResultSize", "4G")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memory", "4G")
      .set("spark.executor.instances", "10")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "hdfs://s44:8020/spark-history-log")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")

    val session = SparkSession.builder
      .config(sparkContext.getConf)
      .config("spark.shuffle.reduceLocality.enabled", "false")
      .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
      .config(
        "spark.executor.extraJavaOptions",
        "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
      )
      .config("spark.locality.wait", "200")
      .enableHiveSupport()
      .getOrCreate()

    session.experimental.extraStrategies =
      (Seq(IndexedOperators) ++ session.experimental.extraStrategies)
    session.experimental.extraOptimizations = (Seq(
      ConvertToIndexedOperators
    ) ++ session.experimental.extraOptimizations)

    val sqlContext = session.sqlContext
    val tables = TPCDSTables.tables
    val dataLocation = "hdfs://s44:8020/tpcds-data-50"
    val databaseName = "tpcds50"

    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

    var createIndex: Double = 0

    tables.foreach { table =>
      val df = table.createExternalTable(
        session.catalog,
        dataLocation,
        databaseName,
        "parquet"
      )
      if (table.name == "inventory") {
        createIndex += measureTimeInMS {
          val indexed = createIndexAndCache(df.get, 0)
          indexed.createOrReplaceTempView(table.name)
        }
      }
      if (table.name == "catalog_returns") {
        createIndex += measureTimeInMS {
          val indexed = createIndexAndCache(df.get, 1)
          indexed.createOrReplaceTempView(table.name)
        }
      }
    }
    session.sql(s"USE $databaseName")

    val parsingTime = measureTimeInMS {
      val _ = session.sessionState.sqlParser.parsePlan(sqlText)
    }

    val df = session.sql(sqlText)
    df.explain(true)
    val queryExecution = df.queryExecution
    val analysisTime = measureTimeInMS {
      queryExecution.analyzed
    }
    val optimizationTime = measureTimeInMS {
      queryExecution.optimizedPlan
    }
    val planningTime = measureTimeInMS {
      queryExecution.executedPlan
    }
    val executionTime = measureTimeInMS {
      df.collect()
    }
    println(
      s"indexing: $createIndex parsing: $parsingTime analysis: $analysisTime optimization: $optimizationTime planning: $planningTime execution: $executionTime"
    )
  }
}
