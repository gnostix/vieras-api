package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.{ApiData, DataLineGraph, MsgNum, Payload, SocialData, TwitterMentionFav, TwitterRetweets, TwitterStats}
import gr.gnostix.api.utilities.{SqlUtils, DateUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 21/10/14.
 */
object MySocialChannelDaoTw extends DatabaseAccessSupportPg {
  implicit val getTwLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
  implicit val getMentionsFavs = GetResult(r => TwitterMentionFav(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getTwitterRetweets = GetResult(r => TwitterRetweets(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getTwitterStats = GetResult(r => TwitterStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getLineCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Option[Payload] = {
    val sql = buildQuery(fromDate, toDate, profileId, companyId, dataType, engId)

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


  def getStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryStats(fromDate, toDate, profileId, companyId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getStats(mySqlDynamic))
    }
    prom.future
  }


  def getLineAllData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[SocialData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, companyId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[SocialData]]()

    Future {
      prom.success(getData(fromDate, toDate, dataType, mySqlDynamic))
    }
    prom.future
  }

  def getTotalSumData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, companyId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataTotal(fromDate, toDate, dataType, mySqlDynamic))
    }
    prom.future
  }


  // get raw data
  def getTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {

    val prom = Promise[Option[ApiData]]()
    val mySqlDynamic = dataType match {
      case "mention" | "favorite" => buildQueryMentionsFavs(fromDate, toDate, profileId, companyId, engId, dataType)
      case "retweet" => buildQueryRetweets(fromDate, toDate, profileId, companyId, engId)
    }

    Future {
      dataType match {
        case "mention" | "favorite" => prom.success(getMentionFavMessages(mySqlDynamic, dataType))
        case "retweet" => prom.success(getRetweetMessages(mySqlDynamic))
      }
    }

    prom.future
  }

  // get raw data
  def getPeakTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, peakDate: DateTime,
                      profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {


    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val prom = Promise[Option[ApiData]]()
    val mySqlDynamic = dataType match {
      case "mention" | "favorite" => buildQueryMentionsFavsPeak(peakDate, profileId, companyId, engId, dataType, grouBydate)
      case "retweet" => buildQueryRetweetsPeak(peakDate, profileId, companyId, engId, dataType, grouBydate)
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

      Some(ApiData(dataTypeTw, myData))
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

      Some(ApiData("twitter_retweets", myData))

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

      Some(ApiData("stats", myData.head))
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


  private def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, credId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "twitter", credId)

    logger.info("------------->" + sqlEngAccount + "-----------")
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    dataType match {
      case "mention" | "favorite" => getSqlMentionFav(numDays, fromDateStr, toDateStr, sqlEngAccount, dataType)
      case "totalmention" | "totalfavorite" => getSqlMentionFavTotal(numDays, fromDateStr, toDateStr, sqlEngAccount, dataType)
      case "retweet" => getSqlRetweet(numDays, fromDateStr, toDateStr, sqlEngAccount)
      case "totalretweet" => getSqlRetweetTotal(numDays, fromDateStr, toDateStr, sqlEngAccount)
    }

  }

  private def getSqlMentionFavTotal(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String, dataType: String) = {
    val twType = dataType match {
      case "totalmention" => "TW_MENTIONS"
      case "totalfavorite" => "TW_FAVORITES"
    }
    val sql = s"""
     select count(*) from vieras.ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from vieras.eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}' and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
        and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
     """

    sql
  }

  private def getSqlMentionFav(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String, dataType: String) = {
    val twType = dataType match {
      case "mention" => "TW_MENTIONS"
      case "favorite" => "TW_FAVORITES"
    }

    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val sql = s"""
      select count(*),date_trunc('${grouBydate}',created) from vieras.ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from vieras.eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and date_trunc('${grouBydate}',created) >= to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by date_trunc('${grouBydate}',created)
                    order by date_trunc('${grouBydate}',created) asc
        """
    logger.info("------------>" + sql)
    sql

  }


  private def getSqlRetweetTotal(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val sql =
      s"""
        select count(*) from vieras.ENG_TW_RETWEETS
         where fk_eng_engagement_data_quer_id in (select q.id from vieras.eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS' and FK_PROFILE_SOCIAL_ENG_ID in  ( $sqlEngAccount  )
          and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """.stripMargin
    logger.info("------------>" + sql)
    sql
  }

  private def getSqlRetweet(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)
    val sql = s"""
        select count(*),date_trunc('${grouBydate}',created) from vieras.ENG_TW_RETWEETS
        where fk_eng_engagement_data_quer_id in (select q.id from vieras.eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and date_trunc('${grouBydate}',created) >= to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by date_trunc('${grouBydate}',created)
                    order by date_trunc('${grouBydate}',created) asc
                     """
    logger.info("------------>" + sql)
    sql

  }

  private def buildQueryMentionsFavsPeak(peakDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int], dataType: String, groupByDate: String): String = {

    val twType = dataType match {
      case "mention" => "TW_MENTIONS"
      case "favorite" => "TW_FAVORITES"
    }

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val peakDateStr: String = fmt.print(peakDate)


    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "twitter", credId)

    val sql = groupByDate match {
      case "hour" | "day" | "week" | "month" | "year" => {
        s"""
          select t.id, t.CREATED, t.ACTION_FROM_USER_HANDLER, t.ACTION_FROM_USER_ID, t.ACTION_FROM_USER_FOLLOWERS,
              t.ACTION_FROM_USER_LISTED, t.TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, t.FAVORITES, t.status_id  from vieras.ENG_TW_MENT_AND_FAV t
           where fk_eng_engagement_data_quer_id in ( select q.id from vieras.eng_engagement_data_queries q where  q.attr = '${twType}'
              and fk_profile_social_eng_id in  ( $sqlEngAccount  )
              and  created between   date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS'))
              and date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS')+ INTERVAL '1 ${groupByDate}')
              and created < date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS') + INTERVAL '1 ${groupByDate}')
          order by created asc
         """
      }
    }
    sql
  }

  private def buildQueryMentionsFavs(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int], dataType: String): String = {

    val twType = dataType match {
      case "mention" => "TW_MENTIONS"
      case "favorite" => "TW_FAVORITES"
    }

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "twitter", credId)

    val sql =
      s"""
          select t.id, t.CREATED, t.ACTION_FROM_USER_HANDLER, t.ACTION_FROM_USER_ID, t.ACTION_FROM_USER_FOLLOWERS,
              t.ACTION_FROM_USER_LISTED, t.TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, t.FAVORITES, t.status_id  from vieras.ENG_TW_MENT_AND_FAV t
           where fk_eng_engagement_data_quer_id in ( select q.id from vieras.eng_engagement_data_queries q where  q.attr = '${twType}'
           and fk_profile_social_eng_id in  ( $sqlEngAccount  )
             and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
             and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          order by created asc
         """


    sql
  }

  private def buildQueryRetweetsPeak(peakDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int], dataType: String, groupByDate: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val peakDateStr: String = fmt.print(peakDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "twitter", credId)

    val sql = groupByDate match {
      case "hour" | "day" | "week" | "month" | "year" => {
        s"""
          select t.ID, t.CREATED, t.RETWEET_STATUS_ID, t.RETWEETED_COUNT,t.RETWEETED_TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, ra.screen_name, ra.listed,  ra.followers, ra.following
          from vieras.ENG_TW_RETWEETS t
          left join vieras.eng_tw_retweets_ffsl ra on t.id=ra.fk_eng_tw_retweets_id
              where fk_eng_engagement_data_quer_id in ( select q.id from vieras.eng_engagement_data_queries q where fk_profile_social_eng_id in  ( $sqlEngAccount  )
                and  t.created between   date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS'))
                and date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS')+ INTERVAL '1 ${groupByDate}')
                and t.created < date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS') + INTERVAL '1 ${groupByDate}')
             order by t.created asc
         """
      }
    }
    sql
  }

  private def buildQueryRetweets(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "twitter", credId)

    val sql =
      s"""
          select t.ID, t.CREATED, t.RETWEET_STATUS_ID, t.RETWEETED_COUNT,t.RETWEETED_TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID, t.twitter_handle from vieras.ENG_TW_RETWEETS t
              where fk_eng_engagement_data_quer_id in ( select q.id from vieras.eng_engagement_data_queries q where fk_profile_social_eng_id in  ( $sqlEngAccount  )
                and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
             order by created asc
         """

    sql
  }


  private def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "twitter", credId)

    val sql =
      s"""
        SELECT max(status_number) as tweets,max(followers),max(following), max(favorites), max(listed), max(handle), max(created) FROM vieras.ENG_TW_STATS t
          where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where attr = 'TW_FFSL'
           and FK_PROFILE_SOCIAL_ENG_ID in  ( $sqlEngAccount  )
           and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """

    sql
  }

}


