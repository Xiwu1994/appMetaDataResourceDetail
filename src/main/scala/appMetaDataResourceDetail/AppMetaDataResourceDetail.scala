package appMetaDataResourceDetail

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.collection.mutable.HashMap

object AppMetaDataResourceDetail {
  Logger.getLogger("org").setLevel(Level.WARN)
  // 最后入库的 表结构
  case class app_meta_data_resource_detail(
    gdid:String,
    gdid_md5:String,
    resource_name:String,
    resource_type:String,
    category:String,
    json_detail:String,
    p_day:String
  )
  def get_value(line: Row, key: String): String = {
    /*
    * 对于Row 输入key 输出对应value*/
    val value = line.get(line.fieldIndex(key))
    if (value == null){
      return "NULL"
    } else {
      return line.get(line.fieldIndex(key)).toString
    }
  }
  def getValuesMap(key:Seq[String],value:Seq[String]): Map[String,String]  = {
    /*
    * input: key_list & value_list
    * return: Map[key->value] */
    val map = new HashMap[String,String]()
    for( i <- 0 until key.length){
      map.put(key.apply(i) , value.apply(i))
    }
    map.toMap
  }
  def getNowDate():String={
    /*
    * return Today(yyyy-MM-dd)*/
    var now:Date = new Date()
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var today = dateFormat.format( now )
    today
  }

  def main(args: Array[String]): Unit = {
    //设置master为local，用来进行本地调试
    val spark = SparkSession.builder().appName("appMetaDataResourceDetail").enableHiveSupport().getOrCreate()
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    // 1、return app_info_db
    val today = getNowDate()
    val select_db_sql = s"select * from dim_meta_data.dim_meta_data_resource_db where p_day = '$today'"
    // select db_name, db_info from db_form
    val app_res_db = sqlContext.sql(select_db_sql).rdd.mapPartitions( partition => {
      partition.map( line => {
        val gdid = get_value(line, "gdid")
        val gdid_md5 = get_value(line, "gdid_md5")
        val resource_name = get_value(line, "db_name")
        val category = get_value(line, "category")
        val resource_type = "db"
        val data_value = Map(
          "gdid" -> gdid,
          "name" -> get_value(line, "db_name"),
          "tags" -> List(),
          "type" -> "database",
          "store_engine" -> get_value(line, "db_engine"),
          "database" -> get_value(line, "db_name"),
          "online_status" -> get_value(line, "status"),
          "level" -> get_value(line, "db_level"),
          "description" -> get_value(line, "db_comment"),
          "creator_name" -> "NULL",
          "owner_name" -> get_value(line, "owner_name"),
          "updater_name" -> "NULL",
          "create_time" -> "NULL",
          "update_time" -> "NULL",
          "space_used" -> "NULL",
          "table_count" -> get_value(line, "table_number")
        )
        val json_str = Json(DefaultFormats).write(Map("data" -> data_value))

        // return
        gdid + "\t" + gdid_md5 + "\t" + resource_name + "\t" + resource_type + "\t" + category + "\t" + json_str
      })
    }).cache()

    // 2、return app_info_table
    val select_column_sql = s"select * from dim_meta_data.dim_meta_data_resource_column where p_day = '$today'"
    val column_rdd = sqlContext.sql(select_column_sql).rdd.cache()
    // column_group_by_rdd: select table_name, column_info(Map) from column_form group by table_name
    val column_group_by_rdd = column_rdd.mapPartitions( partition => {
      partition.map(line => {
        val db_engine = get_value(line, "db_engine")
        val col_index = db_engine match {
          case "hive" => get_value(line, "column_index")
          case "mysql" => get_value(line, "ordinal_position")
          case _ => "NULL"
        }
        (get_value(line, "table_gdid"), get_value(line, "column_name") + "#" +
          get_value(line, "column_comment") + "#" + get_value(line, "column_type") + "#" +
          get_value(line, "is_partition_column") + "#" + col_index)
      })
    }).groupByKey().mapPartitions( partition => {
      partition.map(line => {
        var column_list: List[Map[String, String]] = List()
        var partition_column_list: List[Map[String, String]] = List()
        for (elem <- line._2) {
          val Array(column_name, column_comment, column_type, is_partition_column, col_index) = elem.split("#")
          val column_map = Map(
            "col_index" -> col_index,
            "name" -> column_name,
            "data_type" -> column_type,
            "description" -> column_comment
          )
          column_list = column_list :+ column_map
          if (is_partition_column == "1") {
            partition_column_list = partition_column_list :+ column_map
          }
        }
        val column_list_json_str = Json(DefaultFormats).write(column_list)
        val partition_column_list_json_str = Json(DefaultFormats).write(partition_column_list)

        // return table_name, column_info(column_list + partition_column_list)
        (line._1, column_list_json_str + "#" + partition_column_list_json_str)
      })
    }).cache()

    val select_table_sql = s"select * from dim_meta_data.dim_meta_data_resource_table where p_day = '$today'"
    val table_info_key = Seq("category", "gdid_md5", "table_name", "db_engine", "total_size", "data_length",
      "db_name", "status", "table_comment", "owner_name", "create_time", "update_time", "table_cols", "gdid")

    // app_res_table: select table_name, table_info, column_info(Map) from table_form join column_group_by_rdd on table_name=table_name
    val app_res_table = sqlContext.sql(select_table_sql).rdd.mapPartitions( partition => {
      partition.map(line => {
        var table_info_value = Seq[String]()
        for (elem <- table_info_key) {
          table_info_value = table_info_value :+ get_value(line, elem)
        }
        val table_gdid = get_value(line, "gdid")

        //return table_gdid, table_info
        (table_gdid, table_info_value.mkString("#"))
      })
    }).leftOuterJoin(column_group_by_rdd).mapPartitions( partition => {
      partition.map(line => {
        // line: (table_gdid, (table_info, column_info))

        // a. process table_info
        val table_info_value = line._2._1.split("#")
        val table_info_map = getValuesMap(table_info_key, table_info_value)
        // b. process column_info
        val table_column_info: String = line._2._2 match {
          case None => "[]#[]"
          case _ => line._2._2.toString
        }
        val Array(column_list_str, partition_column_list_str) = table_column_info.split("#")

        val resource_name = table_info_map("table_name")
        val resource_type = "table"
        val space_used = table_info_map("db_engine") match {
          case "hive" => table_info_map("total_size")
          case "mysql" => table_info_map("data_length")
          case _ => "NULL"
        }
        val data_value = Map(
          "gdid" -> table_info_map("gdid"),
          "name" -> table_info_map("table_name"),
          "tags" -> List(),
          "type" -> "table",
          "store_engine" -> table_info_map("db_engine"),
          "database" -> table_info_map("db_name"),
          "online_status" -> table_info_map("status"),
          "level" -> "NULL",
          "description" -> table_info_map("table_comment"),
          "creator_name" -> "NULL",
          "owner_name" -> table_info_map("owner_name"),
          "updater_name" -> "NULL",
          "create_time" -> table_info_map("create_time"),
          "update_time" -> table_info_map("update_time"),
          "space_used" -> space_used,
          "column_count" -> table_info_map("table_cols"),
          "columns" -> column_list_str,
          "partitions" -> partition_column_list_str
        )
        val json_str = Json(DefaultFormats).write(Map("data" -> data_value))
        table_info_map("gdid") + "\t" + table_info_map("gdid_md5") + "\t" + resource_name + "\t" +
          resource_type + "\t" + table_info_map("category") + "\t" + json_str
      })
    }).cache()

    // 3. return app_info_column
    // select column_name, column_info from column_form
    val app_res_column = column_rdd.mapPartitions( partition => {
      partition.map(line => {
        val gdid = get_value(line, "gdid")
        val gdid_md5 = get_value(line, "gdid_md5")
        val resource_name = get_value(line, "column_name")
        val resource_type = "column"
        val data_value = Map(
          "gdid" -> gdid,
          "name" -> resource_name,
          "tags" -> List(),
          "type" -> "field",
          "store_engine" -> get_value(line, "db_engine"),
          "database" -> get_value(line, "db_name"),
          "table" -> get_value(line, "table_name"),
          "online_status" -> "NULL",
          "level" -> "NULL",
          "description" -> get_value(line, "column_comment"),
          "creator_name" -> "NULL",
          "owner_name" -> "NULL",
          "updater_name" -> "NULL",
          "create_time" -> "NULL",
          "update_time" -> "NULL"
        )
        val json_str = Json(DefaultFormats).write(Map("data" -> data_value))
        gdid + "\t" + gdid_md5 + "\t" + resource_name + "\t" + resource_type + "\t" +
          get_value(line, "category") + "\t" + json_str
      })
    }).cache()

    /*
      insert overwrite table app_meta_data_resource_detail
      select resource_id, resource_info from db_res
      union all
      select resource_id, resource_info from table_res
      union all
      select resource_id, resource_info from column_res
     */
    app_res_db.union(app_res_table).union(app_res_column).mapPartitions(partition => {partition.map(_.split("\t"))})
      .mapPartitions(partition => {
        partition.map(e => {
          app_meta_data_resource_detail(e(0), e(1), e(2), e(3), e(4), e(5), today)
        })
      }).toDF().write
      .mode(SaveMode.Overwrite)
//      .partitionBy("p_day")
//      .format("parquet")
//      .insertInto("app_meta_data.app_meta_data_resource_detail")
//      .saveAsTable("app_meta_data.app_meta_data_resource_detail")
      // PS. saveAsTable Or insertInto + partitionBy + OverwirteMode 会删除所有分区
      .parquet(s"/user/hive/warehouse/app_meta_data.db/app_meta_data_resource_detail/p_day=$today")

    app_res_db.unpersist()
    column_rdd.unpersist()
    column_group_by_rdd.unpersist()
    app_res_table.unpersist()
    app_res_column.unpersist()

    val add_partition_sql = s"ALTER TABLE app_meta_data.app_meta_data_resource_detail ADD IF NOT EXISTS PARTITION (p_day='$today') " +
      s"location '/user/hive/warehouse/app_meta_data.db/app_meta_data_resource_detail/p_day=$today'"
    sqlContext.sql(add_partition_sql)
  }
}