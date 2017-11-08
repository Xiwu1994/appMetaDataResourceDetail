import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.json4s.native.Json
import org.apache.spark.sql.{Row, SQLContext}
import org.json4s.DefaultFormats

object appMetaDataResourceDetail {

  Logger.getLogger("org").setLevel(Level.WARN)
  val Debug = true
  // dim_db 文件位置
  var dim_meta_data_resource_db_file_path = ""
  // dim_table 文件位置
  var dim_meta_data_resource_table_file_path = ""
  // dim_db 文件位置
  var dim_meta_data_resource_column_file_path = ""
  if(Debug) {
    dim_meta_data_resource_db_file_path = "/Users/liebaomac/Desktop/dim_meta_data_resource_db.txt"
    dim_meta_data_resource_table_file_path = "/Users/liebaomac/Desktop/dim_meta_data_resource_table.txt"
    dim_meta_data_resource_column_file_path = "/Users/liebaomac/Desktop/dim_meta_data_resource_column.txt"
  } else {
    dim_meta_data_resource_db_file_path = "hdfs://xxxx";
    dim_meta_data_resource_table_file_path = "hdfs://xxxx";
    dim_meta_data_resource_column_file_path = "hdfs://xxxx";
  }

  def get_value(line: Row, key: String): String = {
    return line.get(line.fieldIndex(key)).toString
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appMetaDataResourceDetail")
    //设置master为local，用来进行本地调试
    if(Debug){
      conf.setMaster("local[1]")
    }else{
      conf.setMaster("yarn")
    }
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    /*
    gdid string comment "库的gdid，生成规则：数据库类型::数据库名称",
    gdid_md5 string comment "gdid的md5",
    db_engine string comment "数据库的engine：mysql、hive、mongo",
    db_name string comment "数据库名称",
    db_comment string comment "数据库注释",
    table_number bigint comment "数据库下的table数量",
    category string comment "资源分类: hive、buiness_mysql、buiness_mongo",
    host_name string comment "(business)host地址",
    port bigint comment "(business)port端口",
    db_type string comment "(business)0-mysql主库、1-mysql从库，2-mongo主库、3-mongo从库....（默认为1",
    business_type string comment "(business)业务类型（预留字段，业务的分类规则需要待确定）",
    status int comment "(business)0-下线,1-在线",
    default_character_set_name string comment "(business)数据库默认的字符集",
    default_collation_name string comment "(business)数据库默认的collation",
    db_location_uri string comment "(hive)数据库的HDFS地址",
    owner_name string comment "(hive)数据库的owner",
    owner_type string comment "(hive)数据库所有人的类型(user、role)",
    db_level string comment "(hive)数据库的层级（ods、intg、dim、fact、dwa、app)"
    {
    "data":{
        "gdid":"xxxxx",
        "name":"xxxx",
        "tags":["tag1","tag2"],
        "type":"database",
        "store_engine":"mysql",
        "database":"xxxxx",
        "online_status":"xxxx",
        "level":"ods",
        "description":"xxxxx",
        "creator_name":"xxx",
        "owner_name":"zzzz",
        "updater_name":"yyyyy",
        "create_time":"2017-10-11 11:10:11",
        "update_time":"2017-10-11 11:10:11",
        "space_used":100000,
        "table_count":2000
      }
      gdid	string	资源的全局ID
      gdid_hash	string	资源ID的hash值
      resource_name	string	资源名称
      resource_type	string	资源的类型：db、table、column、report、metrics、dimension
      category	string	资源的最上级分类，目前会分为：mysql业务数据库、hive数据仓库、bi应用数据库，之后会不断的丰富或调整分类方式，比如按照资源所属的部门来进行分类
    }
    * */
    // 1、return app_db_info
    val select_db_sql = "select * from dim_meta_data.dim_meta_data_resource_db where p_day = '2017-11-08'"
    val app_meta_data_resource_db = sqlContext.sql(select_db_sql).rdd.map{ line =>
      val resource_name = get_value(line, "db_name")
      val resource_type = "db"
      val data_value = Map(
        "gdid"->get_value(line,"gdid"),
        "name"->get_value(line,"db_name"),
        "tags"->List(),
        "type"->"database",
        "store_engine"->get_value(line,"db_engine"),
        "database"->get_value(line,"db_name"),
        "online_status"->get_value(line,"status"),
        "level"->get_value(line,"db_level"),
        "description"->get_value(line,"db_comment"),
        "creator_name"->null,
        "owner_name"->get_value(line,"owner_name"),
        "updater_name"->null,
        "create_time"->null,
        "update_time"->null,
        "space_used"->null,
        "table_count"->get_value(line,"table_number")
      )
      val json_str = Json(DefaultFormats).write(Map("data"->data_value))
      // return
      get_value(line,"gdid") + "\t" + get_value(line,"gdid_md5") + "\t" + resource_name +
        "\t" + resource_type + "\t" + get_value(line,"category") + "\t" + json_str
    }

    // 2、return app_table_info
    val select_column_sql = "select * from dim_meta_data.dim_meta_data_resource_column where p_day = '2017-11-08'"
    val column_rdd = sqlContext.sql(select_column_sql).rdd.cache()
    val column_group_by_rdd = column_rdd.map{ line =>
      val db_engine = get_value(line,"db_engine")
      val col_index = db_engine match {
        case "hive" => get_value(line,"column_index")
        case "mysql" => get_value(line,"ordinal_position")
        case _ => null
      }
      (get_value(line,"table_gdid"), get_value(line,"column_name") + "#" +
        get_value(line,"column_comment") + "#" + get_value(line,"column_type") + "#" +
        get_value(line,"is_partition_column") + "#" + col_index)
    }.groupByKey().map { line =>
      var column_list: List[Map[String,String]] = List()
      var partition_column_list: List[Map[String,String]] = List()
      for (elem <- line._2) {
        val Array(column_name, column_comment, column_type, is_partition_column, col_index) = elem.split("#")
        val column_map = Map("col_index"->col_index, "name"->column_name, "data_type"-> column_type, "description"->column_comment)
        column_list = column_list :+ column_map
        if (is_partition_column == "1") {
          partition_column_list = partition_column_list :+ column_map
        }
      }
      val column_list_json_str = Json(DefaultFormats).write(column_list)
      val partition_column_list_json_str = Json(DefaultFormats).write(partition_column_list)
      (line._1, column_list_json_str + "#" + partition_column_list_json_str)
      // return table_name, column_info(column_list + partition_column_list)
    }

    val select_table_sql = "select * from dim_meta_data.dim_meta_data_resource_table where p_day = '2017-11-08'"
    sqlContext.sql(select_table_sql).rdd.map{ line =>
      val table_gdid = get_value(line, "gdid")
      val resource_name = get_value(line, "table_name")
      val resource_type = "table"
      val space_used = get_value(line, "db_engine") match {
        case "hive" => get_value(line, "total_size")
        case "mysql" => get_value(line, "data_length")
        case _ => null
      }
      val data_value = Map(
        "gdid"->get_value(line, "gdid"),
        "name"->get_value(line, "table_name"),
        "tags"->List(),
        "type"->"table",
        "store_engine"->get_value(line, "db_engine"),
        "database"->get_value(line, "db_name"),
        "online_status"->get_value(line, "status"),
        "level"->null,
        "description"->get_value(line, "table_comment"),
        "creator_name"->null,
        "owner_name"->get_value(line, "owner_name"),
        "updater_name"->null,
        "create_time"->get_value(line, "create_time"),
        "update_time"->get_value(line, "update_time"),
        "space_used"->space_used,
        "column_count"->get_value(line, "table_cols")
      )
      (table_gdid, resource_name+"#"+resource_type+"#"+Json(DefaultFormats).write(data_value))
    }.join(column_group_by_rdd).map { line =>
      // line: (table_gdid, (table_info, column_info))

      // b. process column_info
      val Array(column_list_str, partition_column_list_str) = line._2._2.split("#")

      val data_value = Map(
        "columns"->column_list_str,
        "partitions"->partition_column_list_str
      )
      val json_str = Json(DefaultFormats).write(Map("data"->data_value))
      gdid + "\t" + gdid_md5 + "\t" + resource_name + "\t" + resource_type + "\t" + category + "\t" + json_str
    }
      .collect().foreach(println _)

    /*
      gdid	string	资源的全局ID
      gdid_hash	string	资源ID的hash值
      resource_name	string	资源名称
      resource_type	string	资源的类型：db、table、column、report、metrics、dimension
      category	string	资源的最上级分类，目
      {
        "data":{
          "gdid":"xxxxx",
          "name":"xxxx",
          "tags":["tag1","tag2"],
          "type":"table",
          "store_engine":"hive",
          "database":"xxxxx",
          "online_status":"xxxx",
          "level":"ods",
          "description":"xxxxx",
          "creator_name":"xxx",
          "owner_name":"zzzz",
          "updater_name":"yyyyy",
          "create_time":"2017-10-11 11:10:11",
          "update_time":"2017-10-11 11:10:11",
          "space_used":100000,
          "column_count":2000,
          "columns":[
              {
               "col_index":1,
               "name":"xxx",
               "data_type":"string",
               "description":"xxxxxxx"
              }
          ],
          "partitions":[
              {
               "col_index":1,
               "name":"xxx",
               "data_type":"string",
               "description":"xxxxxxx"
              }
          ]
        }
      }
    * TABLE:
      gdid string comment "表的gdid，生成规则：数据库类型::数据库名称::表名称",
      gdid_md5 string comment "gdid的md5",
      db_gdid string comment "库的gdid，生成规则：数据库类型::数据库名称",
      db_engine string comment "数据库的engine：mysql、hive、mongo",
      db_name string comment "库名称",
      table_name string comment "表名称",
      table_type string comment "表的类型,base_table－基本表,view－视图",
      table_rows bigint comment "表的总行数",
      table_cols bigint comment "表的总列数",
      table_comment string comment "表注释",
      create_time string comment "表的创建时间",
      update_time string comment "表的更新时间",
      category string comment "资源分类: hive、buiness_mysql、buiness_mongo",
      data_length bigint comment "(business)表的数据大小",
      table_collation string comment "(business)表的字符校验编码集",
      host_name string comment "(business)db所在的主机名称",
      port bigint comment "(business)数据库对外的端口号",
      engine string comment "(business)存储引擎，比如mysql可能包括myisam、innodb等",
      status int comment "(business)0-下线,1-在线",
      owner_name string comment "(hive)表的拥有者",
      table_store_type string comment "(hive)表的存储格式parquet、orc等",
      table_location_uri string comment "(hive)表的hdfs存储位置",
      is_compressed int comment "(hive)是否压缩",
      compress_format string comment "(hive)压缩格式",
      is_partioned int comment "(hive)是否分区表",
      partion_columns string comment "(hive)分区字段列表",
      field_delimiter string comment "(hive)字段分隔符",
      num_files bigint comment "(hive)文件的数量",
      raw_data_size bigint comment "(hive)原始数据的大小",
      total_size bigint comment "(hive)占用HDFS存储空间大小"

      */

    /*
    COLUMN:
    gdid string comment "表字段的gdid，生成规则：数据库类型::数据库名称::表名称::字段名称",
    gdid_md5 string comment "gdid的md5",
    db_gdid string comment "库的gdid，生成规则：数据库类型::数据库名称",
    db_engine string comment "数据库的engine：mysql、hive、mongo",
    db_name string comment "库名称",
    table_gdid string comment "表的gdid，生成规则：数据库类型::数据库名称::数据表名称",
    table_name string comment "表名称",
    column_name string comment "字段名称",
    column_type string comment "字段类型",
    column_comment string comment "字段注释",
    category string comment "资源分类: hive、buiness_mysql、buiness_mongo",
    is_nullable string comment "(business)是否允许为空",
    character_set_name string comment "(business)字符集名称",
    host_name string comment "(business)host地址",
    port bigint comment "(business)port端口",
    column_default string comment "(business)字段默认值",
    collation_name string comment "(business)字段的字符校验编码集",
    ordinal_position bigint comment "(business)字段在表中的位置,从1开始",
    column_index bigint comment "(hive)列的位置信息,从0开始",
    is_partition_column int comment "(hive)是否为分区字段",
    is_index_column int comment "(hive)是否为index索引列，目前hive未使用任何的index"
  * */

//    table_rdd.join(column_rdd).groupByKey().map { x =>
//      val table_name = x._1
//      var column_list: List[Map[String,String]] = List()
//      var table_cols = 0
//      for (elem <- x._2) {
//        table_cols = elem._1.toInt
//        val column_info = elem._2.split("#")
//        val column_name = column_info(0)
//        val column_type = column_info(1)
//        column_list = column_list :+ Map("name"->column_name,"type"->column_type)
//      }
//      val data_value = Map("table_name"->table_name,"table_cols"->table_cols,"column"->column_list)
//      Json(DefaultFormats).write(Map("data"->data_value))
//    }
//    val app_meta_data_resource_table = sc.textFile(dim_meta_data_resource_table_file_path).map { line =>
//
//    }




//    val column_rdd = sc.textFile(dim_meta_data_resource_column_file_path).map { line =>
//      val split_line = line.split("\t")
//      (split_line(0), split_line(1)+ "#" +split_line(2))
//    }
    /*
    * output: {'data': {'table_name': xx, 'table_cols': xx, 'column': [{'name': xx, 'type': xx}, {'name': xx, 'type': xx}, ...]}, {}...}
    * */
//    table_rdd.join(column_rdd).groupByKey().map { x =>
//      val table_name = x._1
//      var column_list: List[Map[String,String]] = List()
//      var table_cols = 0
//      for (elem <- x._2) {
//        table_cols = elem._1.toInt
//        val column_info = elem._2.split("#")
//        val column_name = column_info(0)
//        val column_type = column_info(1)
//        column_list = column_list :+ Map("name"->column_name,"type"->column_type)
//      }
//      val data_value = Map("table_name"->table_name,"table_cols"->table_cols,"column"->column_list)
//      Json(DefaultFormats).write(Map("data"->data_value))
//    }.collect().foreach(println _)
  }
}
