
ТЗ:

Необходимо реализовать месячный транзакционный агрегат и обогатить его epk_id на основе витрины связок.
Описание необходимых таблиц и их объемы + партиционирование представлено ниже:

Детальные транзакции (объем 1Тб):
CREATE EXTERNAL TABLE `txn`(

  `evt_id` decimal(18,0), --id события, уникальный ключ

  `evt_tim` timestamp, --timestamp события

  `client_w4_id` decimal(18,0), --уникальный идентификатор клиента

  `mcc_code` decimal(18,0), --mcc код транзакции

  `local_amt` decimal(28,10) --сумма транзакции)

PARTITIONED BY (`trx_date` string) --Партиционирование по дате транзакции в формате yyyy-mm-dd

STORED AS PARQUET

LOCATION 'data/custom/rb/card/pa/txn'

 

Витрина связок (Объем 300Гб)
CREATE EXTERNAL TABLE `epk_lnk_host_id`(

  `epk_id` bigint, --идентификатор клиента

  `external_system` string, --Наименование системы источника, тут фильтровать нужно по 'WAY4'

  `external_system_client_id` string --id клиента в системе источника,

  `row_actual_from` string --дата начала актуальности записи)

PARTITIONED BY ( `row_actual_to` string) --дата окончания актуальности записи, нас интересуют только актуальные id клиента

STORED AS PARQUET

LOCATION 'data/custom/rb/epk/pa/epk_lnk_host_id'

 

DDL таблицы, которую хотим получить:
CREATE TABLE `ft_txn_aggr`(

  `epk_id` bigint, --идентификатор клиента

  `sum_txn` string, --сумма транзакций

  `mcc_code` string --mcc код транзакций)

PARTITIONED BY ( `report_dt` string) --отчетная дата, конец месяца в формате yyyy-mm-dd

STORED AS PARQUET

LOCATION 'data/custom/rb/txn_aggr/pa/ft_txn_aggr'


1) build.sbt 

name := "my-project"

version := "1.8.2"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)


2) plugins.sbt 

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.1")



3) MySparkSession.scala

// импорт необходимых классов и методов из библиотеки Spark SQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, last_day, to_date}

// создание объекта для установки сессии Spark
object MySparkSession {
  def createSession(): SparkSession = {
    SparkSession.builder()
      .appName("My Spark Application") // задание названия приложения
      .config("spark.some.config.option", "some-value") // установка конфигурационных параметров
      .getOrCreate() // создание или получение текущей сессии
  }
}

// основной объект приложения
object Main {
  def main(args: Array[String]): Unit = {
    // создание сессии Spark
    val spark = MySparkSession.createSession()


    // импорт необходимых методов для работы с Dataframe внутри сессии Spark
    import spark.implicits._

    // Передача временного диапазона для фильтрации при запуске run.sh, даты "2022-01-01" и "2023-01-01" будут использоваться только в качестве значений по умолчанию, если они не были переданы при запуске скрипта
    val reportStart = sys.props.getOrElse("reportStart", "2022-01-01")
    val reportEnd = sys.props.getOrElse("reportEnd", "2023-01-01")

    // чтение данных из файла формата parquet, фильтрация по временному диапазону и выборка необходимых столбцов
    val txnDF = spark.read.format("parquet").load("data/custom/rb/card/pa/txn")
      .filter($"evt_tim".between(reportStart, reportEnd))

    //Добавляет колонку "report_dt" с последним днём текущего месяца в датафрейм txnDF и сохраняет результат в новом датафрейме txn_report.
    val txn_report = txnDF.withColumn("report_dt", last_day(to_date($"evt_tim")).cast("date"))



    val epkDF = spark.read.format("parquet").load("data/custom/rb/epk/pa/epk_lnk_host_id")
      .filter($"external_system" === "WAY4" && $"row_actual_to" === "9999-12-31")  // В первом условии проверяется, что значение в столбце "external_system" равно "WAY4". Во втором условии проверяется, что значение в столбце "row_actual_to" равно "2999-12-31" (дата актуальности).
      .select($"epk_id", $"external_system_client_id".as("client_w4_id"))


    // Отключит автоматический выбор режима джоина broadcast(так как вторая таблица не поместится в память воркеров) для всех джойнов в Spark, будет использоваться SortMergeJoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    // SortMergeJoin - хороший выбор, учитывая, что ключи соединения присутствуют в обеих таблицах и таблица txn уже разбита на партиции по дате транзакции

    val ftTxnAggrDF = txn_report.join(epkDF, $"client_w4_id" === $"client_w4_id", "inner")
      .groupBy($"epk_id", $"mcc_code", $"report_dt") // группировка по epk_id, по mcc коду транзакции и последнему дню месяца report_dt
      .agg(sum($"local_amt").as("sum_txn"))    // считает сумму транзакций


    // запись результатов в файл формата parquet с разбиением по дате и режимом перезаписи
    val outputDir = "data/custom/rb/txn_aggr/pa/ft_txn_aggr"

      ftTxnAggrDF.write
      .partitionBy("report_dt")  //Метод partitionBy указывает, что данные должны быть разбиты на разделы в соответствии со значением колонки report_dt
      .option("path", outputDir)
      .mode("overwrite")  //mode("overwrite") указывает, что если файл уже существует, он будет перезаписан.
      .save()

    // остановка сессии Spark
    spark.stop()
  }
}



4) run.sh 

#!/bin/bash
export REPORT_START=2022-01-01
export REPORT_END=2023-01-01
sbt assembly
spark-submit --class Main --conf spark.driver.extraJavaOptions="-DreportStart=$REPORT_START -DreportEnd=$REPORT_END" target/scala-2.12/my-project_2.12-1.8.2.jar

