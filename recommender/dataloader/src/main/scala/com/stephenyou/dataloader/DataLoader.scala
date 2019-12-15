package com.stephenyou.dataloader


import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/** *
  * Movie数据集字段通过 ^ 进行分割
  * 电影id
  * 电影名称
  * 电影的描述
  * 电影的时长
  * 电影的发行日期
  * 电影的拍摄日期
  * 电影的语言
  * 电影的类型
  * 电影的演员
  * 电影的导演
  **/

//格式化快捷键ctrl+alt+l
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String,
                 val issue: String, val shoot: String, val language: String, val genres: String,
                 val actors: String, val directors: String)

/**
  * Rating数据集，用户对于电影的评分数据及，用逗号分割
  * 用户id
  * 电影id
  * 用户对于电影的评分
  * 用户对电影评分的时间
  */
case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
  * Tag数据集，用户对于电影的标签数据集，用逗号分割
  * 用户id
  * 电影id
  * 标签的具体内容
  * 用户对电影打标签的时间
  */
case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/** *
  * MongoDB的连接配置
  *
  * @param url MongoDB的连接
  * @param db  MongoDB要操作的数据库
  */
case class MongoConfig(val uri: String, val db: String)

/** *
  * ElasticSearch的连接配置
  *
  * @param httpHosts      Http的主机列表，使用逗号分割
  * @param transportHosts Transport主机列表，用逗号分割
  * @param index          需要操作的索引
  * @param clustername    ES集群的名称
  */
case class ESConfig (val httpHosts: String, val transportHosts: String, val index: String, val clustername: String)

//数据的主加载服务
object DataLoader {

  val MOVIE_DATA_PATH="E:\\IdeaProject\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\movies.csv"
  val RATING_DATA_PATH="E:\\IdeaProject\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\ratings.csv"
  val TAG_DATA_PATH="E:\\IdeaProject\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\tags.csv"

  val MONGODB_MOVIE_COLLECTION="Movie"
  val MONGODB_RATING_COLLECTION="Rating"
  val MONGODB_TAG_COLLECTION="Tag"

  val ES_MOVIE_INDEX="Movie"

  //程序的入口
  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.cores"-> "local[*]",
      "mongo.uri"-> "mongodb://192.168.12.141:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts"->"movie-re1:9200",
      "es.transportHosts"->"movie-re1:9300",
      "es.index"->"recommender",
      "es.cluster.name"->"es-cluster"
    )

    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)

    //创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //将Movie，Rating，Tag数据集加载进来
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

    import spark.implicits._
    //将RDD转化为DataFrame
    val movieDF =movieRDD.map(item =>{
      val attr=item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()

    val ratingDF =ratingRDD.map(item =>{
      val attr=item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    val tagDF =tagRDD.map(item =>{
      val attr=item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    implicit val mongoConfig =MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)

    //需要将数据保存在MongoDB中,我们封装一个
    //storeDataInMongoDB(movieDF,ratingDF,tagDF)

    //需要将数据保存到ES中

    import org.apache.spark.sql.functions._
    /***
      * MID, Tags
      * 1    tag1|tag2|...
      */

    //将tag数据集中的mid和tag取出来
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags")).select("mid","tags")
    //将newTag聚合到movie数据集中
    implicit val esConfig = ESConfig(config.get("es.httpHosts").get,config.get("es.transportHosts").get,config.get("es.index").get,config.get("es.cluster.name").get)
    val movieWithTagsDF=movieDF.join(newTag,Seq("mid","mid"),"left")
    storeDataInES(movieWithTagsDF)

    //关闭spark
    spark.stop()
  }

  //将数据保存到MongoDB的方法
  //用隐式参数来穿配置文件
  def storeDataInMongoDB(movieDF: DataFrame,ratingDF:DataFrame,tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //新建一个到MongoDB的连接
    val mongoClient =MongoClient(MongoClientURI(mongoConfig.uri))

    //1.如果MongoDB中有对应的数据库，name应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).drop()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).drop()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).drop()

    //2.将当前数据写入数据库,查看官方文档
    movieDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //3.对数据表建索引
    //createIndex()方法基本语法格式如下所示：db.collection.createIndex(keys, options)
    //Key 值为你要创建的索引字段，1 为指定按升序创建索引，如果你想按降序来创建索引指定为 -1 即可。
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid"->1))

    //4.关闭MongoDB的连接
    mongoClient.close()
  }

  //将数据保存到ES中的方法
  def storeDataInES(movieDF:DataFrame)(implicit esConfig:ESConfig): Unit = {
    //新建一个配置
    val settings:Settings=Settings.builder().put("cluster.name",esConfig).build()

    //需要新建一个ES的客户端
    val esClient= new PreBuiltTransportClient(settings)

    //需要将transportHost添加到esClient中
    val REGEX_HOST_PORT ="(.+):(\\d+)".r

    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String)=>{
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }
    //需要清除ES中遗留的数据
    if(esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists()){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
    //将数据写入到ES中
    movieDF
      .write
      .option("es.nodes",esConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index+"/"+ES_MOVIE_INDEX)

  }

}


