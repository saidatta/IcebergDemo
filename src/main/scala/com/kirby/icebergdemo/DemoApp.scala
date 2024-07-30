package com.kirby.icebergdemo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object DemoApp extends Logging {

  def main(args: Array[String]): Unit = {
    createTable()
    initialInsert()
    checkRawParquets()
    upsertData()
    checkRawParquets()
    timeTravelExample("8472915270827124666")
    expireSnapshot()
    rewriteMore()
  }

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    // Iceberg specific configs
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.harry_ns", "org.apache.iceberg.spark.SparkCatalog")
    // • HadoopCatalog supports tables that are stored in HDFS or your local file system.
    // • HiveCatalog uses a Hive Metastore to keep track of your Iceberg table by storing a reference to the latest metadata file.
    .config("spark.sql.catalog.harry_ns.type", "hadoop")
    .config("spark.sql.catalog.harry_ns.warehouse", "src/main/resources/warehouse/catalog/harry_ns/")
    .getOrCreate()

  /**
    * Create an Iceberg table
    */
  def createTable(): Unit = {

    val createTableSQL =
      """
     CREATE TABLE IF NOT EXISTS harry_ns.input_table (Id  string,
          Name  string,
          Gender  string,
          House  string,
          Blood_status  string,
          Hair_colour  string
          )
        USING iceberg
        PARTITIONED BY (House)
        """
    //TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '1')
    //TBLPROPERTIES ('write.metadata.previous-versions-max' = '1')
    spark.sql(createTableSQL)

    log.info("Snapshots after creation")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show

  }

  /**
    * Read input data Harry Potter from semicolon separated file
    * Fill the Iceberg table by
    */
  def initialInsert(): Unit = {

    val inputDF: DataFrame = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiLine", "true")
      .csv("src/main/resources/input_data/Characters.csv")

    inputDF.printSchema()

    inputDF.createGlobalTempView("input_data")

    val insertSQL =
      """
     INSERT INTO harry_ns.input_table
        SELECT Id,Name,Gender,House,Blood_status,Hair_colour
      FROM global_temp.input_data
      """
    spark.sql(insertSQL)

    log.info("Data after insert")
    spark.sql("SELECT * FROM harry_ns.input_table order by id").show(3)

    log.info("Snapshots after insert")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show()
  }

  def upsertData(): Unit = {

    val changedDataDF: DataFrame =
      spark.sql(
        """
SELECT
   1 as Id,
  'Harry James Potter' AS  Name,
  'Male' AS Gender,
  'Gryffindor' AS House,
  'II+' AS Blood_status,
  'blondie' AS Hair_colour
"""
      )
    log.info("Delta files")
    changedDataDF.show
    changedDataDF.createGlobalTempView("delta")

    spark.sql(
      """
MERGE INTO harry_ns.input_table base USING global_temp.delta incr
    ON base.id = incr.id
WHEN MATCHED THEN
    UPDATE SET base.Hair_colour = incr.Hair_colour
 """
    )

    log.info("Data after upsert")
    spark.sql("SELECT * FROM harry_ns.input_table order by id").show(3)

    log.info("Snapshots after upsert")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show
    log.info("Files after upsert")
    spark
      .sql("SELECT file_path,file_format,partition,record_count,null_value_counts FROM harry_ns.input_table.files")
      .show
  }

  def checkRawParquets(): Unit = {
    log.info("Reading raw parquet: src/main/resources/warehouse/catalog/harry_ns/input_table/data")
    spark.read
      .parquet("src/main/resources/warehouse/catalog/harry_ns/input_table/data")
      .orderBy("id")
      .limit(3)
      .show
  }

  def timeTravelExample(snapshotVersion: String): Unit = {
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show
    log.info("Select SQL - from T0. " + snapshotVersion)
    spark
      .sql(
        s"""
        SELECT id, gender, name, Hair_colour 
        FROM harry_ns.input_table VERSION AS OF $snapshotVersion
        ORDER BY id"""
      )
      .show(3)

    log.info("Select SQL - changes T0 & T1")
    spark
      .sql(
        s"""
        SELECT *
        FROM harry_ns.input_table as t2
        JOIN ( SELECT *  FROM harry_ns.input_table VERSION AS OF $snapshotVersion) as t1
        ON t2.id = t1.id
        WHERE t2.Hair_colour <> t1.Hair_colour
        ORDER BY t2.id
        """
      )
      .show(3)

  }

  //https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files
  def expireSnapshot(): Unit = {
    log.info("Snapshots before clearing")
    log.info("Files after insert")
    spark
      .sql("SELECT file_path,file_format,partition,record_count,null_value_counts FROM harry_ns.input_table.files")
      .show
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show
    spark
      .sql(
        """
        CALL harry_ns.system.expire_snapshots(
          table => 'harry_ns.input_table'
          , older_than => TIMESTAMP 'now'
          , retain_last => 1
        )"""
      )
      .show

    log.info("Snapshots after clearing")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show

//    spark.sql("SELECT * FROM harry_ns.input_table.manifests").show
//    spark.sql("SELECT * FROM harry_ns.input_table.files").show
//    spark
//      .sql("CALL harry_ns.system.rewrite_manifests('harry_ns.input_table')")
//      //.sql("CALL harry_ns.system.rewrite_data_files('harry_ns.input_table')")
//      .show
//    spark.sql("SELECT * FROM harry_ns.input_table.manifests").show
  }

  def rewriteMore(): Unit = {
    spark
      .sql(
        """
        CALL harry_ns.system.rewrite_manifests(table => 'harry_ns.input_table' )"""
      )
      .show
    expireSnapshot()
  }
}
