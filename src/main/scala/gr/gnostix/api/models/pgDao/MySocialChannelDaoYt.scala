package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.models.plainModels.{ApiData, DataLineGraph, YoutubeLineData, YoutubeStats, YoutubeVideoData, YoutubeVideoStats}
import gr.gnostix.api.utilities.DateUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}


object MySocialChannelDaoYt extends DatabaseAccessSupport {
  implicit val getYtLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getYoutubeStats = GetResult(r => YoutubeStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getYoutubeVideoStats = GetResult(r => YoutubeVideoStats(r.<<, r.<<, r.<<, r.<<))
  implicit val getYoutubeVideoData = GetResult(r => YoutubeVideoData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getYoutubeLineData = GetResult(r => YoutubeLineData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryStats(fromDate, toDate, profileId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getStats(mySqlDynamic))
    }
    prom.future
  }

  def getVideoStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryVideoStats(fromDate, toDate, profileId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getVideoStats(mySqlDynamic))
    }
    prom.future
  }


  def getLineCounts(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val sql = buildQueryLine(fromDate, toDate, profileId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getLineData(sql))
    }
    prom.future

  }


  // get raw data
  def getTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryData(fromDate, toDate, profileId, engId)

    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getVideoData(mySqlDynamic))
    }

    prom.future
  }


  private def getStats(sql: String): Option[ApiData] = {

    try {
      var myData = List[YoutubeStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel yt ------------->" + sql)
          val records = Q.queryNA[YoutubeStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have channel stats ")
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


  private def getVideoStats(sql: String): Option[ApiData] = {

    try {
      var myData = List[YoutubeVideoStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social video yt ------------->" + sql)
          val records = Q.queryNA[YoutubeVideoStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have video stats ")
        Some(ApiData("video_stats", myData.head))
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

  private def getVideoData(sql: String): Option[ApiData] = {

    try {
      var myData = List[YoutubeVideoData]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social video yt ------------->" + sql)
          val records = Q.queryNA[YoutubeVideoData](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have video stats ")
        Some(ApiData("video_data", myData))
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


  private def getLineData(sql: String): Option[ApiData] = {

    try {
      var myData = List[YoutubeLineData]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social video yt ------------->" + sql)
          val records = Q.queryNA[YoutubeLineData](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have video stats ")
        Some(ApiData("video_data", dataMinus(myData) ))
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

  //case class YoutubeLineData(subscribers: Int, totalViews: Int, videoViews: Int, likes: Int, dislikes: Int, favorites: Int, created: Timestamp)

  /**
   * this a helper function to minus from a list the previous with the current class/tuple
   * @param li
   * @return
   */
  def dataMinus(li: List[YoutubeLineData]): List[YoutubeLineData] = {
    val buf = new ListBuffer[YoutubeLineData]

    @annotation.tailrec
    def go(li: List[YoutubeLineData]): List[YoutubeLineData] = {
      li match {
        case Nil => Nil
        case x :: Nil => List(buf.toList: _*)
        case x :: y :: Nil => buf += YoutubeLineData(
          y.subscribers - x.subscribers,
          y.totalViews - x.totalViews,
          y.videoViews - x.videoViews,
          y.likes - x.likes,
          y.dislikes - x.dislikes,
          y.favorites - x.favorites,
          y.created);
          go(y :: Nil)
        case x :: y :: xs => buf += YoutubeLineData(
          y.subscribers - x.subscribers,
          y.totalViews - x.totalViews,
          y.videoViews - x.videoViews,
          y.likes - x.likes,
          y.dislikes - x.dislikes,
          y.favorites - x.favorites,
          y.created);
          go(y :: xs)
      }
    }

    go(li)
  }


  private def buildQueryData(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val sqlEngAccount = engId match {
      case Some(x) => x + " )"
      case None => "select s.id from eng_profile_social_credentials s where s.fk_profile_id in (" +
        " select profile_id from profiles where profile_id = " + profileId + ") and s.fk_datasource_id = 9)"
    }
    logger.info("------------->" + sqlEngAccount + "-----------")
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val grouBydate = DateUtils.sqlGrouByDate(numDays)

    // ADD THE RIGHT SQL QUERY HERE !!!!!!!
    val sql = s"""
         select t.y_title, t.Y_PLAYER_URL, t.Y_THUMBNAILS, t.Y_FAVORITE_COUNT, t.Y_VIEW_COUNT,t.Y_DISLIKE_COUNT, t.Y_LIKE_COUNT, t.Y_SUM_TEXT
          from ENG_YT_WALL t
           where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q where  q.attr = 'YT_USER_WALL'
             and fk_profile_social_eng_id in ( $sqlEngAccount )
                and m_sysdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        order by m_sysdate asc
      """
    logger.info("------------>" + sql)
    sql

  }


  private def buildQueryLine(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {
    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val sqlEngAccount = engId match {
      case Some(x) => x + " )"
      case None => "select s.id from eng_profile_social_credentials s where s.fk_profile_id in (" +
        " select profile_id from profiles where profile_id = " + profileId + ") and s.fk_datasource_id = 9)"
    }

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val grouBydate = DateUtils.sqlGrouByDate(numDays)

    val sql = s"""
       SELECT max(subscribers), max(total_views), max(video_views), max(likes), max(dislikes), max(favorites),
        trunc(ffsl_date,'${grouBydate}') FROM ENG_YT_STATS t
         where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where attr = 'YT_FFSL'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
            and ffsl_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           group by trunc(ffsl_date,'${grouBydate}')
           order by trunc(ffsl_date,'${grouBydate}')asc
      """
    logger.info("------------>" + sql)
    sql
  }


  private def buildQueryVideoStats(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
         SELECT sum(y_like_count), sum(y_dislike_count), sum(y_favorite_count), sum(y_view_count) FROM ENG_YT_WALL t
           where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where attr = 'YT_USER_WALL'
            and FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId}  and id = ${x} ))
                  and y_published_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """
      case None =>
        s"""
         SELECT sum(y_like_count), sum(y_dislike_count), sum(y_favorite_count), sum(y_view_count) FROM ENG_YT_WALL t
           where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where attr = 'YT_USER_WALL'
            and FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} ))
                  and y_published_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
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
        SELECT max(subscribers), max(total_views), max(ffsl_date), max(likes), max(dislikes), max(favorites), max(video_views) FROM ENG_YT_STATS t
           where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where attr = 'YT_FFSL'
            and FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where id = ${engId} and fk_profile_id=${profileId} ))
                  and ffsl_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """
      case None =>
        s"""
        SELECT max(subscribers) , max(total_views), max(ffsl_date), max(likes), max(dislikes), max(favorites), max(video_views) FROM ENG_YT_STATS t
           where T.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where attr = 'YT_FFSL'
            and FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} ))
                  and ffsl_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """
    }

    sqlEngAccount
  }

}


