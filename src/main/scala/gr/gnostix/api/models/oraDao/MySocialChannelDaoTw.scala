package gr.gnostix.api.models.oraDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.models.plainModels._
import gr.gnostix.api.utilities.DateUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 21/10/14.
 */
object MySocialChannelDaoTw extends DatabaseAccessSupport {
  implicit val getTwLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
  implicit val getMentionsFavs = GetResult(r => TwitterMentionFav(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getTwitterRetweets = GetResult(r => TwitterRetweets(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getTwitterStats = GetResult(r => TwitterStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getLineCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Option[Payload] = {
    val sql = buildQuery(fromDate, toDate, profileId, dataType, engId)

    //bring the actual data
    val data = dataType match {
      case "mention" | "retweet" | "favorite" => getData(fromDate, toDate, dataType, sql)
      case "totalmention" | "totalfavorite" | "totalretweet" => getDataTotal(fromDate, toDate, dataType, sql)
    }
    //val data = getData(fromDate, toDate, dataType, profileId, sql)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }


  def getStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryStats(fromDate, toDate, profileId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getStats(mySqlDynamic))
    }
    prom.future
  }


  def getLineAllData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Future[Option[SocialData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[SocialData]]()

    Future {
      prom.success(getData(fromDate, toDate, dataType, mySqlDynamic))
    }
    prom.future
  }

  def getTotalSumData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataTotal(fromDate, toDate, dataType, mySqlDynamic))
    }
    prom.future
  }


  // get raw data
  def getTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {

    val prom = Promise[Option[ApiData]]()
    val mySqlDynamic = dataType match {
      case "mention" | "favorite" => buildQueryMentionsFavs(fromDate, toDate, profileId, engId, dataType)
      case "retweet" => buildQueryRetweets(fromDate, toDate, profileId, engId)
    }

    Future {
      dataType match {
        case "mention" | "favorite" => prom.success(getMentionFavMessages(mySqlDynamic, dataType))
        case "retweet" => prom.success(getRetweetMessages(mySqlDynamic))
      }
    }

    prom.future
  }

  private def getMentionFavMessages(sql: String, dataType: String): Option[ApiData] = {

    val dataTypeTw = dataType match {
      case "mention" => "twitter_mentions"
      case "favorite" => "twitter_favorites"
    }
    try {
      var myData = List[TwitterMentionFav]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel tw ------------->" + sql)
          val records = Q.queryNA[TwitterMentionFav](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> nodata tw  " + dataTypeTw)
        Some(ApiData(dataTypeTw, myData))
      } else {
        logger.info(" -------------> nodata ")
        Some(ApiData("nodata", None))
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getRetweetMessages(sql: String): Option[ApiData] = {

    try {
      var myData = List[TwitterRetweets]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel tw ------------->" + sql)
          val records = Q.queryNA[TwitterRetweets](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> nodata tw retweets ")
        Some(ApiData("twitter_retweets", myData))
      } else {
        logger.info(" -------------> nodata ")
        Some(ApiData("nodata", None))
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getStats(sql: String): Option[ApiData] = {

    try {
      var myData = List[TwitterStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel tw ------------->" + sql)
          val records = Q.queryNA[TwitterStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have data stats ")
        Some(ApiData("stats", myData.head))
      } else {
        logger.info(" -------------> nodata ")
        Some(ApiData("nodata", None))
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getData(fromDate: DateTime, toDate: DateTime, dataType: String, sql: String): Option[SocialData] = {

    try {
      var myData = List[DataLineGraph]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel twitter ------------->" + sql)
          val records = Q.queryNA[DataLineGraph](sql)
          myData = records.list()
      }

      val lineData = SocialData(dataType, myData)

      lineData match {
        case SocialData(_, _) => Option(lineData)
      }
    } catch {
      case e: Exception => None
    }

  }

  private def getDataTotal(fromDate: DateTime, toDate: DateTime, dataType: String, sql: String): Option[ApiData] = {

    try {
      var myData = 0
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel twitter -------------> " + sql)
          val records = Q.queryNA[Int](sql)
          myData = records.first()
      }

      Some(ApiData(dataType, myData))

    } catch {
      case e: Exception => None
    }

  }


  private def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val sqlEngAccount = engId match {
      case Some(x) => x + " )"
      case None => "select s.id from eng_profile_social_credentials s where s.fk_profile_id in (" +
        " select profile_id from profiles where profile_id = " + profileId + ") and s.fk_datasource_id = 2)"
    }
    logger.info("------------->" + sqlEngAccount + "-----------")
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    dataType match {
      case "mention" | "favorite" => getSqlMentionFav(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount, dataType)
      case "totalmention" | "totalfavorite" => getSqlMentionFavTotal(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount, dataType)
      case "retweet" => getSqlRetweet(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
      case "totalretweet" => getSqlRetweetTotal(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
    }

  }

  private def getSqlMentionFavTotal(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String, dataType: String) = {
    val twType = dataType match {
      case "totalmention" => "TW_MENTIONS"
      case "totalfavorite" => "TW_FAVORITES"
    }
    val sql = s"""
      select count(*) from ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        """
    sql
  }

  private def getSqlMentionFav(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String, dataType: String) = {
    val twType = dataType match {
      case "mention" => "TW_MENTIONS"
      case "favorite" => "TW_FAVORITES"
    }

    val grouBydate = DateUtils.sqlGrouByDateOra(numDays)

    val sql = s"""
      select count(*),trunc(created_at,'${grouBydate}') from ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'${grouBydate}') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'${grouBydate}')
                    order by trunc(created_at,'${grouBydate}')asc
                     """
    logger.info("------------>" + sql)
    sql

  }

  private def getSqlRetweetTotal(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val sql =
      s"""
      select count(*) from ENG_TW_RETWEETS
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """.stripMargin
    logger.info("------------>" + sql)
    sql
  }

  private def getSqlRetweet(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDateOra(numDays)
    val sql = s"""
       select count(*),trunc(created_at,'${grouBydate}') from ENG_TW_RETWEETS
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'${grouBydate}') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'${grouBydate}')
                    order by trunc(created_at,'${grouBydate}')asc
                     """
    logger.info("------------>" + sql)
    sql

  }


  private def buildQueryMentionsFavs(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int], dataType: String): String = {

    val twType = dataType match {
      case "mention" => "TW_MENTIONS"
      case "favorite" => "TW_FAVORITES"
    }

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
          select t.id, t.CREATED_AT, t.ACTION_FROM_USER_HANDLER, t.ACTION_FROM_USER_ID, t.ACTION_FROM_USER_FOLLOWERS,
              t.ACTION_FROM_USER_LISTED, t.TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, t.FAVORITES, t.status_id  from ENG_TW_MENT_AND_FAV t
           where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q where  q.attr = '${twType}'
           and fk_profile_social_eng_id in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=2 and id = ${x}  ))
                   and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          order by created_at asc
         """
      case None =>
        s"""
          select t.id, t.CREATED_AT, t.ACTION_FROM_USER_HANDLER, t.ACTION_FROM_USER_ID, t.ACTION_FROM_USER_FOLLOWERS,
              t.ACTION_FROM_USER_LISTED, t.TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, t.FAVORITES, t.status_id  from ENG_TW_MENT_AND_FAV t
           where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q where  q.attr = '${twType}'
           and fk_profile_social_eng_id in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=2 ))
                   and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          order by created_at asc
         """
    }

    sqlEngAccount
  }


  private def buildQueryRetweets(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
            select t.ID, t.CREATED_AT, t.RETWEET_STATUS_ID, t.RETWEETED_COUNT,t.RETWEETED_TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, t.twitter_handle from ENG_TW_RETWEETS t
             where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q where fk_profile_social_eng_id
             in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=2 and id = ${x} ))
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            order by created_at asc
         """
      case None =>
        s"""
            select t.ID, t.CREATED_AT, t.RETWEET_STATUS_ID, t.RETWEETED_COUNT,t.RETWEETED_TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, t.twitter_handle from ENG_TW_RETWEETS t
             where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q where fk_profile_social_eng_id
             in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=2 ))
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            order by created_at asc
         """
    }

    sqlEngAccount
  }


  private def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
          SELECT max(status_number) as tweets,max(followers),max(following), max(favorites), max(listed), max(handle), max(ffsl_date) FROM ENG_TW_STATS t
           where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where attr = 'TW_FFSL'
            and FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where id = ${engId} and fk_profile_id=${profileId} and fk_datasource_id=2))
              and ffsl_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """
      case None =>
        s"""
          SELECT max(status_number) as tweets,max(followers),max(following), max(favorites), max(listed), max(handle), max(ffsl_date) FROM ENG_TW_STATS t
           where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where attr = 'TW_FFSL'
            and FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=2))
              and ffsl_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """
    }

    sqlEngAccount
  }

}

