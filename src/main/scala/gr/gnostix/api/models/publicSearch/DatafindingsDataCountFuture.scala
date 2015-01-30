package gr.gnostix.api.models.publicSearch

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportOra
import gr.gnostix.api.utilities.SqlUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

object DatafindingsDataCountFutureDao extends DatabaseAccessSupportOra {

  implicit val getSentimentLineResult = GetResult(r => (r.<<): Int)

  val logger = LoggerFactory.getLogger(getClass)


  def getDataDefault(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,
                     profileId: Int, datasource: String): Future[Option[(String, Int)]] = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    val prom = Promise[Option[(String, Int)]]()

    Future {
      prom.success(getData(fromDate, toDate, mySqlDynamic, datasource))

      /*      data match {
              case Some(data) => data
              case None => None
            }*/
    }
    prom.future
  }


  def getDataByKeywords(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,
                        profileId: Int, keywords: List[Int], datasource: String): Future[Option[(String, Int)]] = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    val prom = Promise[Option[(String, Int)]]()

    Future {
      prom.success(getData(fromDate, toDate, mySqlDynamic, datasource))

    }
    prom.future
  }

  def getDataByTopics(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int],
                      datasource: String): Future[Option[(String, Int)]] = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    val prom = Promise[Option[(String, Int)]]()

    Future {
      prom.success(getData(fromDate, toDate, mySqlDynamic, datasource))

    }
    prom.future
  }


  private def getData(fromDate: DateTime, toDate: DateTime, mySqlDynamic: String, datasource: String): Option[(String, Int)] = {

    val sql = buildQuery(fromDate, toDate, mySqlDynamic, datasource)

    sql match {
      case Some(sql) => {

        var myData: Int = 0

        getConnection withSession {
          implicit session =>
            logger.info("getThirdLevelData tw ------------->" + sql)
            val records = Q.queryNA[Int](sql)
            // we get only one record because we count the results
            myData = records.first()
        }
        val lineData = (datasource, myData)
        Some(lineData)
      }

      case None => None
    }

  }

  private def buildQuery(fromDate: DateTime, toDate: DateTime, sqlDynamic: String, datasource: String): Option[String] = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    datasource match {
      case "twitter" => Some(getSqlTW(fromDateStr, toDateStr, sqlDynamic))
      case "facebook" => Some(getSqlFB(fromDateStr, toDateStr, sqlDynamic))
      case "gplus" => Some(getSqlGplus(fromDateStr, toDateStr, sqlDynamic))
      case "youtube" => Some(getSqlYT(fromDateStr, toDateStr, sqlDynamic))
      case "web" => Some(getSqlWebByType(fromDateStr, toDateStr, sqlDynamic, WebDatasources.web.head._1))
      case "linkedin" => Some(getSqlWebByType(fromDateStr, toDateStr, sqlDynamic, WebDatasources.linkedin.head._1))
      case "news" => Some(getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.news.head._1))
      case "blog" => Some(getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.blogs.head._1))
      case "personal" => Some(getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.personal.head._1))
      case _ => {
        logger.info("---------> no sql code for this datasource ${datasource}  ")
        None
      }
    }

  }

  private def getSqlTW(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*)from twitter_results i
                           where i.t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} ) """
    sql
    //logger.info("------------>" + sql)
  }


  private def getSqlFB(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*)from facebook_results i
                           where i.f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} ) """

    sql

  }

  private def getSqlGplus(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*) from googleplus_results i
                           where i.itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} ) """

    sql

  }

  private def getSqlYT(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*) from youtube_results i
                           where i.Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} ) """

    sql

  }

  private def getSqlWebByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, webType: Int): String = {
    val sql = s"""select count(*) from web_Results i
                           where i.item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_grp_id = ${webType}
                           and fk_queries_id in (select q_id from queries where  ${sqlGetProfileData} ) """

    sql

  }


  private def getSqlFeedByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, feedType: Int): String = {
    val sql = s"""select count(*) from feed_results i
                           where i.RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_grp_id = ${feedType}
                           and fk_queries_id in (select q_id from queries where  ${sqlGetProfileData} ) """

    sql

  }


}


