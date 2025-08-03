// Databricks notebook source
import org.apache.spark.sql.functions._
import spark.implicits._
import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column
import scala.util.Random
import java.time.LocalDateTime
import org.apache.spark.sql.types._

//json file
var df = spark.read.option("multiline", true).json("Your json file path here")

// Exploding AdditionalAttributes array
var flattened = df.select(explode($"AdditionalAttributes").alias("attr"))

// Filtering only join action
var joinsOnly = flattened.filter($"attr.Action" === "Join")

//attr is an alias
var joinDetails = joinsOnly.withColumn("step", explode($"attr.Steps"))
  .select(
    $"attr.Action",
    $"attr.TargetName",
    $"step.LeftTable",
    $"step.LeftColumnName",
    $"step.RightTable",
    $"step.RightColumnName",
    $"step.JoinType"
  )

joinDetails.show(false)

var primaryKeyMap= Map(
  "workflowViewsDB_UAP.af1da7e9_e740_47ac_b8d7_0b9cc7ade3cf" -> "header_project_projectdocument__id",
  "workflowViewsDB_UAP.1de8fb15_3079_4f49_bea5_85763d3269e1" -> "header_supplier_supplierprofile__id",
  "workflowViewsDB_UAP.790c0957_b590_484e_887a_7e6cbf7bce89" -> "header_contract_clmdetails__id",
  "deltaA_pk" -> "header_project_projectdocument__id",
  "deltaB_pk" -> "header_supplier_supplierprofile__id",
  "deltaC_pk" -> "header_contract_clmdetails__id",
)

//Naming convention= delta+ "any name" + _pk
var deltaTableMap: Map[String, String] = Map(
  "workflowViewsDB_UAP.af1da7e9_e740_47ac_b8d7_0b9cc7ade3cf" -> "deltaA_pk",
  "workflowViewsDB_UAP.1de8fb15_3079_4f49_bea5_85763d3269e1" -> "deltaB_pk",
  "workflowViewsDB_UAP.790c0957_b590_484e_887a_7e6cbf7bce89" -> "deltaC_pk",
)



// COMMAND ----------

case class JoinLink(source: String,target: String,keys: List[(String, String)],joinType: String = "inner",outputview: String)

var mergeJoinGraph: List[JoinLink] = joinDetails.collect().toList.map { row =>
  var leftTable = row.getAs[String]("LeftTable")
  var rightTable = row.getAs[String]("RightTable")
  var leftCols = row.getAs[Seq[String]]("LeftColumnName")
  var rightCols = row.getAs[Seq[String]]("RightColumnName")
  var joinType = row.getAs[String]("JoinType")
  var outputView = row.getAs[String]("TargetName")
  var keys: List[(String, String)] = leftCols.zip(rightCols).toList

  JoinLink(leftTable, rightTable, keys, joinType, outputView)
}
mergeJoinGraph.foreach(println)



// COMMAND ----------

// bidirectional join links from mergeJoinGraph
var initialJoinGraph: List[JoinLink] = mergeJoinGraph.flatMap { link =>
  var forward = JoinLink(
    source = s"delta${link.source}_pk",
    target = link.target,
    keys = link.keys,
    joinType = link.joinType,
    outputview = s"incrementalJoin_${link.source}_${link.target}"
  )

  var reverse = JoinLink(
    source = s"delta${link.target}_pk",
    target = link.source,
    keys = link.keys.map(_.swap),
    joinType = link.joinType,
    outputview = s"incrementalJoin_${link.target}_${link.source}"
  )

  Seq(forward, reverse)
}

initialJoinGraph.foreach { link =>
  var joinPairs = link.keys.map { case (f, t) => s"$f = $t" }.mkString(", ")
}

var allJoinLinks: List[JoinLink] = mergeJoinGraph ++ initialJoinGraph

// all unique table names from join graph
var allTables: Set[String] = mergeJoinGraph.flatMap(j => Seq(j.source, j.target)).toSet

var tableToColumnsMap: Map[String, Seq[String]] = allTables.map { tableName =>
  var cols = spark.table(tableName).columns.toSeq
  tableName -> cols
}.toMap

// joinKeyMap
def generateJoinKeyMapBasedOnAllUsages(
  allJoinLinks: List[JoinLink],
  tableToColumnsMap: Map[String, Seq[String]]
): Map[String, List[String]] = {

  var allJoinKeysUsed: Set[String] = allJoinLinks.flatMap(_.keys.flatMap {
    case (leftKey, rightKey) => Seq(leftKey, rightKey)
  }).toSet

  var joinKeyMap: Map[String, List[String]] = tableToColumnsMap.map {
    case (tableName, columns) =>
      var matchedJoinKeys = allJoinKeysUsed.intersect(columns.toSet)
      tableName -> matchedJoinKeys.toList
  }
  joinKeyMap
}

var joinKeyMap = generateJoinKeyMapBasedOnAllUsages(mergeJoinGraph, tableToColumnsMap)

joinKeyMap.foreach { case (tableName, keys) =>
  println(s"Table: $tableName")
  println(s"Join Keys: ${keys.mkString(", ")}")
}





// COMMAND ----------



// var a= spark.sql("SELECT header_project_projectdocument__id FROM workflowViewsDB_UAP.af1da7e9_e740_47ac_b8d7_0b9cc7ade3cf")
// a.distinct.show(false)
// println(s"Distinct count (a): " + a.distinct().count())
// var b= spark.sql("SELECT header_supplier_supplierprofile__id FROM workflowViewsDB_UAP.1de8fb15_3079_4f49_bea5_85763d3269e1")
// b.distinct.show(false)
// println(s"Distinct count (b): " + b.distinct().count())
// var c= spark.sql("SELECT header_contract_clmdetails__id FROM workflowViewsDB_UAP.790c0957_b590_484e_887a_7e6cbf7bce89")
// c.distinct.show(false)
// println(s"Distinct count (c): " + c.distinct().count())
// var d= spark.sql("SELECT objectcode  FROM workflowViewsDB_UAP.d7fb620e_0c36_43b5_90a4_9866d3a208da")
// d.distinct.show(false)
// println(s"Distinct count (d): " + d.distinct().count())

// COMMAND ----------

//naming convention- rawdelta +" "+ _pk, for timestamp col: UpdateTimestamp
val rawdeltaA_pk = Seq(
  ("a8e7276d-647e-49eb-bd2c-7c6e8b42cfbd", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("3054f411-2093-41c9-84df-be99f8525a10", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("7d2e6a40-4377-4f8c-bdfb-63b163f95c27", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("250f10c3-24e2-4b2c-b179-d45a5220a20f", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("223f423b-4eb3-425f-97ff-5b2b7ef9a60d", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("80165cdf-29a0-48c4-8800-3041cdebabe4", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("b388d698-fe43-491e-beb2-ca134864e184", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("918da242-f7ee-4738-883a-45c8e344c6e8", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("f60cb5bb-c499-4d34-b211-59a8744d89ef", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("1eb4dd19-ea39-413c-9a86-10f269e3ebe6", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("531ec129-324f-462c-b923-58a3de1b09c6", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("a8df04a8-fdab-4ce3-be91-05efd41cc9be", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("bdbb04b3-e55a-4d67-a5e7-90d146e82fa6", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("b0c59c92-62a8-4862-b3a5-e51d6cd579d5", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("47065dd3-c8ee-40bc-8fa7-8cbb41890ec2", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("40fa72a9-6739-43cf-86bc-8bc4d414cc8b", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("adaf9880-b336-48d2-a809-7dc374aaa639", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("f1addf20-0e69-4671-aeff-8850f3fe31b1", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("38b4d458-2050-4479-9f73-f4460cada4bc", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("537223e1-73db-490c-a8ea-26ab56ed7f6f", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("e4ada7cf-5292-4a9d-a9df-a7165107dc01", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("1750daeb-e00f-4554-acd3-2f335118aaff", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("70d06755-d6a9-425c-b1fa-7cc293e49dfc", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("564630dd-932f-453a-9702-06e95929ecc2", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("ec8e08a1-7b2e-4c0e-a525-bf35db618325", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("991b5e7c-6004-41d0-a108-ba96dbe2335e", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("9fb6f76a-a169-4881-9a83-b154c5cc7ff7", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("20fd5e44-22f1-4991-9220-41d06b6e1729", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("2f4d6a6e-54aa-4b25-a0bd-ffa723510e63", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("a20aae3c-6216-4a23-8ec6-185ba539c01f", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("bc7e565e-4264-4105-888f-6006d47bebf3", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("c6fe99c5-db3c-4af0-9ca1-2160464d5571", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("384b331d-3049-4bcd-81cd-e7e971d8fe1f", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("eb3cc731-2ef9-4c54-a0a1-712bfdea09e3", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("d2d07aa5-0ebd-414b-93a4-e3ac6dca61a2", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("50b7f386-b399-4b4f-b294-5c1090ee25ff", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("614f902a-9a66-4447-a36b-c6d81d661ba7", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("e1de7d46-33da-4b55-8041-e0624c345e44", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("2e84b49b-b64a-407b-9c86-27f4eb25518c", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("7fded26e-5597-47e8-ada2-7baade4276af", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("57bb5620-43e0-4a13-8ce7-1e9364b4a894", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("e8934d08-b789-4b91-91e2-8a7ac3293030", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("21233097-7964-4e09-a24f-18a788f34fd8", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("555cc973-43ca-4261-9655-8422ffb4cb24", Timestamp.valueOf("2024-07-09 09:20:00")),
  ("80b1066d-77be-4ae3-adb7-ffbf193e17f9", Timestamp.valueOf("2024-07-09 09:20:00"))
).toDF("header_project_projectdocument__id", "UpdateTimestamp")



val rawdeltaB_pk = Seq(
  ("e7ada39a-ba84-483f-b43b-ab844a35a587", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("70a63f0d-e8e1-4aa2-aeb9-d6625a79aae0", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("c217f923-412f-4edd-ba26-b793c9a526b5", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("e0fb6242-b7f7-4bba-8be2-e90411641083", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("bff4dcef-8828-4706-8e34-951afd3c0e86", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("a4d002aa-95b4-496c-ab23-1eabef2d208d", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("5463649f-f3d9-438c-84c9-70936765c1fc", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("bc21f93d-d53e-46a8-a94a-bce9399dc023", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("40344128-0ad5-460a-b370-7af3b48c6c96", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("23a645dd-bcdc-4fdb-bc78-7c114e3c2b20", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("7bb4ac8e-9cb7-47cd-95ac-c6173f2e099f", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("307d97fb-8795-418e-9c03-4790fcce052f", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("53746de9-fd2c-4c97-993f-481d0a60a0d7", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("684d1153-f01c-4f33-bea2-2bb9e28d9481", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("2873ad7f-7e1d-4145-a62d-8c6cc4e93671", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("2508bc68-6408-4aa1-a43f-20d30df0e33d", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("cdad6cea-4c96-4d1b-ae22-9c969c49fe66", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("8f43988c-2f85-4215-96d4-fe1bb3c9a615", Timestamp.valueOf("2024-07-09 10:15:00")),
  ("0971e5b9-a2ed-45fb-a9fe-0ff88bf8610a", Timestamp.valueOf("2024-07-09 10:15:00"))
).toDF("header_supplier_supplierprofile__id", "UpdateTimestamp")



val rawdeltaC_pk = Seq(
  ("bdcfe665-7e51-4618-9c24-c769b6eadd0e", Timestamp.valueOf("2024-07-09 11:00:00")),
  ("03f88ed5-6535-46f5-b30a-93980912e1ab", Timestamp.valueOf("2024-07-09 11:05:00")),
  ("d7534103-89be-4175-82b6-c10bcb43e146", Timestamp.valueOf("2024-07-09 11:10:00")),
  ("74247907-370a-499c-981d-36d913c67162", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("6ba28ac6-31d8-493a-b60d-49fe10433378", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("440f1b44-5a29-4795-b4da-28fa3b5a384f", Timestamp.valueOf("2024-07-09 11:15:00")),  
  ("e9074689-1a50-4221-9174-36b5031887f8", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("5af0449b-27f8-4370-bd63-6ec1a9b2cf9f", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("95bb9e6d-7155-4cdd-9cc0-bd97a20f9a6a", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("220cf473-bc53-43ba-9a10-57b9c296e342", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("6383198d-3dd6-4d6f-afb1-92ffb3c5d326", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("5b6a74d5-8643-49e0-b02b-d29d311a9ed2", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("906e1080-6a9d-468d-b4f2-f48ea4a6f4b8", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("635d833c-c1ff-465d-90e6-58277f877088", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("33c04dd8-4b7f-492f-aec7-6022ce1b1d0f", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("29906225-baf0-4a7a-abaa-ce18d5012ef6", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("d2336471-c5f6-402b-bde1-7fb887c7071a", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("f6ea0beb-ea34-49d6-8026-805c207ebc26", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("982a119b-5e30-47e9-931e-204081673618", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("e1afcb23-2e0f-4614-a9e9-87c9dec501e1", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("1d65ad02-4e74-4830-8279-c926d1c48e5d", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("125fb10a-2be3-4482-90ef-3e7053fbf1ed", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("5981d0f4-cecb-4616-9d4d-b6e34fa158ac", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("d623d8f8-08f3-403f-a8c1-7cd9057a680b", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("4484179c-ef2a-4099-aac9-70d1cfab0cc3", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("3f511522-a8fb-49c0-93b2-1c3a0d695974", Timestamp.valueOf("2024-07-09 11:15:00")),
  ("69a2380b-825a-4312-a861-c5f80485d63f", Timestamp.valueOf("2024-07-09 11:15:00")),
).toDF("header_contract_clmdetails__id", "UpdateTimestamp")


rawdeltaA_pk.createOrReplaceTempView("rawdeltaA_pk")
rawdeltaC_pk.createOrReplaceTempView("rawdeltaC_pk")
rawdeltaB_pk.createOrReplaceTempView("rawdeltaB_pk")

val lastRefreshTimestamp = Timestamp.valueOf("2023-07-01 00:00:00")
//creating delta tables
spark.catalog.listTables()
  .filter(_.name.startsWith("raw"))
  .collect()
  .foreach { table =>
    var rawName = table.name                    
    var targetView = rawName.stripPrefix("raw")

    var df = spark.table(rawName).filter(col("UpdateTimestamp") >= lit(lastRefreshTimestamp)).drop("UpdateTimestamp")

    df.createOrReplaceTempView(targetView)
    println(s"Filtered view created: $targetView")
    df.show(false) 
  }



// COMMAND ----------


var affectedPKMap: scala.collection.mutable.Map[String, DataFrame] = 
  scala.collection.mutable.Map(
    deltaTableMap.map { case (fullModule, deltaTable) =>
      var pkCol = primaryKeyMap(fullModule)
      var df = spark.table(deltaTable).select(pkCol).distinct()
      fullModule -> df
     }.toSeq: _*
  )

// COMMAND ----------

def shortName(name: String): String= {
  name
    .replaceAll("module", "")
    .replaceAll("Module", "")
    .replaceAll("delta", "")
    .replaceAll("_pk", "")
    .replaceAll("tempmerge", "")
    .replaceAll("temp", "")
    .replaceAll("affected", "")
    .replaceAll("incrementalJoin_", "")}

// COMMAND ----------

// Creating temp modules with names containing dots
def createTempModuleWithKeys(tempViewName: String, sourceModule: String, primaryKey: String, joinKeys: List[String]): Unit = {
  var selectedColumns = (primaryKey :: joinKeys).distinct
  var tempDF = spark.table(sourceModule).select(selectedColumns.map(col): _*)
  tempDF.createOrReplaceTempView(s"`$tempViewName`")
  println(s"Created temp view: $tempViewName")
  spark.sql(s"SELECT * FROM `$tempViewName`").show()
}

var baseModules = primaryKeyMap.keys.filterNot(_.contains("delta")).toList

baseModules.foreach { module =>
  var primaryKey = primaryKeyMap(module)
  var joinKeys = joinKeyMap.getOrElse(module, List.empty[String])
  // Use backticks to allow dots in the view name
  createTempModuleWithKeys(s"temp$module", module, primaryKey, joinKeys)
}

// COMMAND ----------

def runPreFilter(deltaDF: DataFrame,targetTable: String,deltaJoinKey: String,targetJoinKey: String,outputView: String): Unit = {
  var targetDF = spark.sql(f"select * from ${targetTable}")
  var aliasDelta = "d"
  var aliasTarget = "t"
  var deltaCols = deltaDF.columns
  var targetCols = targetDF.columns.filterNot(deltaCols.contains)
  var selectExprs = deltaCols.map(c => col(s"$aliasDelta.$c")) ++ targetCols.map(c => col(s"$aliasTarget.$c"))
  var joined = deltaDF.alias(aliasDelta).join(targetDF.alias(aliasTarget), col(s"$aliasDelta.$deltaJoinKey") === col(s"$aliasTarget.$targetJoinKey"), "left")
    .select(selectExprs: _*)

// var joined = targetDF.filter(col(s"$targetJoinKey").isin(deltaDF.select(col(deltaJoinKey)).as[String].collect(): _*))
  joined.createOrReplaceTempView(s"`$outputView`") 
  println(s"Created prefilter view: $outputView")
  println(f"delta count : ${deltaDF.count()}")
  println(f"pre filter count : ${joined.count()}")
}

// var allIncrementalViews= Set.empty[String]

def runIncrementalJoin(table1: String,table2: String,keys: List[(String, String)],outputView: String,joinType: String): Unit = {

  var df1= spark.sql(f"select * from ${table1}")
  var df2= spark.sql(f"select * from ${table2}")

  var joinCondition= keys.map { case (colA, colB) =>
    df1(colA) === df2(colB)
  }.reduce(_ && _)

  var result= df1.join(df2, joinCondition, "left")
  result.createOrReplaceTempView(outputView)
  println(s" Created view: $outputView : $joinType")
  // spark.sql(s"SELECT * FROM $outputView").show()
}



// COMMAND ----------


var allIterationMergedViews: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty[String]
def smartMergeTables(df1: DataFrame, df2: DataFrame): DataFrame = {
  var allCols = (df1.columns ++ df2.columns).distinct
  def alignColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    var alignedCols: Seq[Column] = cols.map { colName =>
      if (df.columns.contains(colName)) df(colName) else lit(null).as(colName)
    }
    df.select(alignedCols: _*)
  }

  var df1Aligned = alignColumns(df1, allCols)
  var df2Aligned = alignColumns(df2, allCols)

  // Merge after filtering and aligning
 df1Aligned.unionByName(df2Aligned)
}


// COMMAND ----------

var iteration = 1
var newKeysFound = true

val intermediateDeltaKeyMap = scala.collection.mutable.Map[String, DataFrame]()
val intermediateDeltaKeyCountMap = scala.collection.mutable.Map[String, Long]()

affectedPKMap.foreach { case (fullModule, _) =>
  val pkCol = primaryKeyMap(fullModule)
  intermediateDeltaKeyMap(pkCol) = spark.emptyDataFrame
  intermediateDeltaKeyCountMap(pkCol) = 0L
}

while (newKeysFound) {

  var affectedPKMap: scala.collection.mutable.Map[String, DataFrame] = 
  scala.collection.mutable.Map(
    deltaTableMap.map { case (fullModule, deltaTable) =>
      var pkCol = primaryKeyMap(fullModule)
      var df = spark.table(deltaTable).select(pkCol).distinct()
      fullModule -> df
     }.toSeq: _*
  )
  
  println(s"\nIteration $iteration\n")
  newKeysFound = false
var iter=1
  //PreFilter on all tempModules 
  baseModules.foreach { module =>
    var deltaTable = affectedPKMap.getOrElse(module, spark.emptyDataFrame)
    var targetModule = s"`temp$module`"
    var deltaJoinKey = primaryKeyMap(module)
    var targetJoinKey = deltaJoinKey
    var affectedView = s"affected${shortName(module)}"
    runPreFilter(deltaTable, targetModule, deltaJoinKey, targetJoinKey, affectedView)
  }
var merged=spark.emptyDataFrame

  //IncrementalJoin + Merge per JoinLink 
  for (link <- mergeJoinGraph) {
    var from = link.source
    var to = link.target
    var keys = link.keys
    var joinType = link.joinType
    var outputView = link.outputview

    var fromShort= shortName(from)
    var toShort= shortName(to)

var isFromBase= baseModules.contains(from)
var isToBase= baseModules.contains(to)
//forward pass
var join1: (String, String, List[(String, String)], String, String) = 
  if (isFromBase && isToBase) {
    // Case 1: both base modules
    (s"`affected$fromShort`", s"`temp$to`", keys,s"incrementalJoin_${fromShort}_${toShort}".replace(".", "_"), joinType)
  } else if (!isFromBase && isToBase) {
    // Case 2a: from = outputview, to = base
    (s"`merged$fromShort`", s"`temp$to`", keys,s"incrementalJoin_${fromShort}_${toShort}".replace(".", "_"), joinType)
  } else if (isFromBase && !isToBase) {
    // Case 2b: from is base, to is outputview
     (s"`affected$fromShort`", s"$to", keys, s"incrementalJoin_${fromShort}_${toShort}".replace(".", "_"), joinType)
  } else {
    // Case 3: both are outputViews (merged)
    (s"`merged$fromShort`", s"`$to`", keys ,s"incrementalJoin_${fromShort}_${toShort}".replace(".", "_"), joinType)
  }
//backward pass
var join2: (String, String, List[(String, String)], String, String) = 
  if (isFromBase && isToBase) {
    (s"`affected$toShort`", s"`temp$from`", keys.map(_.swap),s"incrementalJoin_${toShort}_${fromShort}".replace(".", "_"), joinType)
  } else if (!isFromBase && isToBase) {
    (s"`affected$toShort`", s"`$from`", keys.map(_.swap),s"incrementalJoin_${toShort}_${fromShort}".replace(".", "_"), joinType)
  } else if (isFromBase && !isToBase) {
   (s"`merged$toShort`", s"`temp$from`", keys.map(_.swap), s"incrementalJoin_${toShort}_${fromShort}".replace(".", "_"), joinType)
  } else {
  (s"`merged$toShort`", s"`$from`", keys.map(_.swap), s"incrementalJoin_${toShort}_${fromShort}".replace(".", "_"), joinType)
  }

List(join1, join2).foreach {
  case (table1, table2, joinKeys, outView, jType) =>
    runIncrementalJoin(table1, table2, joinKeys, outView, jType)
}

// allIncrementalViews ++= List(join1._4, join2._4)

    var view1= join1._4
    var view2= join2._4

// Main merging logic
if (spark.catalog.tableExists(view1) && spark.catalog.tableExists(view2)) {
  var df1 = spark.sql(s"SELECT * FROM `$view1`")
  var df2 = spark.sql(s"SELECT * FROM `$view2`")

  merged = smartMergeTables(df1, df2)

  var mergedTempView = s"mergedTempView$iter"
  spark.sql(s"DROP VIEW IF EXISTS `$mergedTempView`")
  merged.createOrReplaceTempView(mergedTempView)

  var safeOutputView = s"merged$outputView"
   spark.sql(s"DROP VIEW IF EXISTS `$safeOutputView`")
  spark.sql(s"CREATE OR REPLACE TEMP VIEW `$safeOutputView` AS SELECT * FROM `$mergedTempView`")

  println(s"[MERGED] $view1 + $view2 -> $safeOutputView")
  // spark.sql(s"SELECT * FROM `$safeOutputView`").show()  

  iter = iter + 1
  println("debug1")
}
}

// intermediate_deltakey1 = merged.select("header_project_projectdocument__id").na.drop().distinct()
// intermediate_deltakey2 = merged.select("header_supplier_supplierprofile__id").na.drop().distinct()
// intermediate_deltakey3 = merged.select("header_contract_clmdetails__id").na.drop().distinct()

intermediateDeltaKeyMap.keys.foreach { pkCol =>
  val df = merged.select(pkCol).na.drop().distinct()
  intermediateDeltaKeyMap(pkCol) = df
  intermediateDeltaKeyCountMap(pkCol) = df.count()
}

affectedPKMap.keys.foreach { fullModule =>
  val pkCol = affectedPKMap(fullModule).columns(0) 
  val newDF = intermediateDeltaKeyMap.getOrElse(pkCol, spark.emptyDataFrame)
  val oldDF = affectedPKMap(fullModule)
  val oldCount = oldDF.count()
  val newCount = newDF.count()

  println(s"\n Checking module: $fullModule | PK: $pkCol")
  println(s"Old Count: $oldCount | New Count: $newCount")

  if (oldCount < newCount) {
    println(s"New keys found for $fullModule")
    newKeysFound=true
    intermediateDeltaKeyCountMap(pkCol) = newCount

    val oldDF = affectedPKMap(fullModule)
    val updatedDF = oldDF.unionByName(newDF).distinct()

    affectedPKMap(fullModule) = updatedDF

    val viewName = deltaTableMap.getOrElse(fullModule, s"default_${pkCol}_pk")
    updatedDF.createOrReplaceTempView(viewName)

    println(s"Updated count for $fullModule ($pkCol): $newCount")
 
  }
}

  iteration += 1
 
}

// COMMAND ----------

// val new_data=spark.sql("select * from `mergedworkflowViewsDB_UAP.30de0ec3_344b_4346_a321_0025001a3870`")
// val old_data=spark.sql("select * from workflowViewsDB_UAP.incremental_test")
// val new_columns=new_data.columns
// val old_columns=new_data.columns
// val commonColumns = new_columns.intersect(old_columns)
// if(new_columns.length==commonColumns.length){
// import org.apache.spark.sql.functions.{concat_ws, sha2}

// val new_data_with_hash = new_data.withColumn(
//   "hashkey",
//   sha2(concat_ws("||", commonColumns.map(col): _*), 256)
// )
// val old_data_with_hash = old_data.withColumn(
//   "hashkey_old",
//   sha2(concat_ws("||", commonColumns.map(col): _*), 256)
// )
// val joined_df = old_data_with_hash.join(new_data_with_hash, old_data_with_hash("header_contract_clmdetails__id")===new_data_with_hash("header_contract_clmdetails__id"), "full")
// //val test=joined_df.select("hashkey","hashkey_old").distinct
// display(joined_df)
// }

// COMMAND ----------

