package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.models.plainModels.{ApiData, DataLineGraph, DemographicsDataFB, FacebookComment, FacebookDemographics, FacebookPost, FacebookStats, FacebookStatsApi, FacebookStatsTop, MsgNum, Payload, SocialData, SocialDataSum}
import gr.gnostix.api.utilities.DateUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 21/10/14.
 */
object MySocialChannelDaoFB extends DatabaseAccessSupport {
  implicit val getFbLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
  implicit val getFbDemographics = GetResult(r => FacebookDemographics(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getFbStats = GetResult(r => FacebookStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getFbComment = GetResult(r => FacebookComment(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getFbPost = GetResult(r => FacebookPost(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getLineCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Option[Payload] = {
    val sql = buildQuery(fromDate, toDate, profileId, dataType, engId)

    //bring the actual data
    val data = dataType match {
      case "post" | "comment" => getData(fromDate, toDate, dataType, profileId, sql)
      case "totalpost" | "totalcomment" => getDataTotal(fromDate, toDate, dataType, profileId, sql)
    }
    //val data = getData(fromDate, toDate, dataType, profileId, sql)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }

  def getLineAllData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Future[Option[SocialData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[SocialData]]()

    Future {
      prom.success(getData(fromDate, toDate, dataType, profileId, mySqlDynamic))
    }
    prom.future
  }

  def getTotalSumData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Future[Option[SocialDataSum]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[SocialDataSum]]()

    Future {
      prom.success(getDataTotal(fromDate, toDate, dataType, profileId, mySqlDynamic))
    }
    prom.future
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


  def getDemographics(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryDemographics(fromDate, toDate, profileId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataDemographics(mySqlDynamic))
    }
    prom.future
  }

  // get raw data
  def getTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {

    val prom = Promise[Option[ApiData]]()
    val mySqlDynamic = dataType match {
      case "comment" => buildQueryComments(fromDate, toDate, profileId, engId)
      case "post" => buildQueryPosts(fromDate, toDate, profileId, engId)
    }

    Future {
      dataType match {
        case "comment" => prom.success(getCommentMessages(mySqlDynamic))
        case "post" => prom.success(getPostMessages(mySqlDynamic))
      }
    }

    prom.future
  }

  private def getPostMessages(sql: String): Option[ApiData] = {

    try {
      var myData = List[FacebookPost]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[FacebookPost](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> nodata fb post ")
        Some(ApiData("facebook_posts", myData))
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

  private def getCommentMessages(sql: String): Option[ApiData] = {

    try {
      var myData = List[FacebookComment]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[FacebookComment](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> nodata fb comments ")
        Some(ApiData("facebook_comments", myData))
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


  private def getDataDemographics(sql: String): Option[ApiData] = {

    try {
      var myData = List[FacebookDemographics]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[FacebookDemographics](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        val maleData = myData.toList.head
        val male = maleData.age17 + maleData.age24 + maleData.age34 + maleData.age44 +
          maleData.age54 + maleData.age64 + maleData.age65Plus

        val femaleData = myData.toList.tail.head // the head of the rest items..
        val female = femaleData.age17 + femaleData.age24 + femaleData.age34 + femaleData.age44 +
            femaleData.age54 + femaleData.age64 + femaleData.age65Plus

        val age = List(maleData.age17 + femaleData.age17, maleData.age24 + femaleData.age24, maleData.age34 + femaleData.age34,
          maleData.age44 + femaleData.age44, maleData.age54 + femaleData.age54, maleData.age64 + femaleData.age64,
          maleData.age65Plus + femaleData.age65Plus)


        Some(ApiData("demographics", DemographicsDataFB(female, male, age, myData)))
      } else {
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
      var myData = List[FacebookStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[FacebookStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        val reach = myData.map(_.reach).sum
        val views = myData.map(_.talkingAbout).sum
        val engaged = myData.map(_.engaged).sum
        val talkingAbout = myData.map(_.talkingAbout).sum
        val newLikes = myData.last.pageLikes - myData.head.pageLikes
        val shares = myData.map(_.postShares).sum


        logger.info(" -------------> nodata stats ")
        Some(ApiData("stats", FacebookStatsApi(FacebookStatsTop(reach, views, engaged, talkingAbout, newLikes, shares), myData)))
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

  private def getData(fromDate: DateTime, toDate: DateTime, dataType: String, profileId: Int, sql: String): Option[SocialData] = {

    try {
      var myData = List[DataLineGraph]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[DataLineGraph](sql)
          myData = records.list()
      }

      val lineData = SocialData("facebook", myData)

      lineData match {
        case SocialData(_, _) => Option(lineData)
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getDataTotal(fromDate: DateTime, toDate: DateTime, dataType: String, profileId: Int, sql: String): Option[SocialDataSum] = {

    try {
      var myDataTotal = 0
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[Int](sql)
          myDataTotal = records.first()
      }

      val sumData = SocialDataSum("facebook", myDataTotal)

      Option(sumData)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val sqlEngAccount = engId match {
      case Some(x) => x + " )"
      case None => "select s.id from eng_profile_social_credentials s where s.fk_profile_id in (" +
        " select profile_id from profiles where profile_id = " + profileId + ") and s.fk_datasource_id = 1)"
    }
    logger.info("------------->" + sqlEngAccount + "-----------")
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    dataType match {
      case "post" => getSqlPosts(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
      case "totalpost" => getSqlPostsTotal(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
      case "comment" => getSqlComments(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
      case "totalcomment" => getSqlCommentsTotal(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
    }

  }

  def getSqlPostsTotal(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val sql = s"""select count(*) from eng_fb_wall
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')"""
    sql
  }

  def getSqlPosts(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDate(numDays)

    val sql = s"""select count(*),trunc(msg_date,'${grouBydate}') from eng_fb_wall
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and trunc(msg_date,'${grouBydate}') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(msg_date,'${grouBydate}')
                    order by trunc(msg_date,'${grouBydate}')asc"""
    logger.info("------------>" + sql)
    sql

  }

  def getSqlCommentsTotal(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val sql = s"""select count(*) from ENG_FB_WALL_COMMENTS
                      where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
                        where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                          and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')"""
    logger.info("------------>" + sql)
    sql
  }

  def getSqlComments(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDate(numDays)

    val sql = s"""select count(*),trunc(comment_date,'${grouBydate}') from ENG_FB_WALL_COMMENTS
                      where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
                        where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                          and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and trunc(comment_date,'${grouBydate}') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(comment_date,'${grouBydate}')
                    order by trunc(comment_date,'${grouBydate}')asc"""
    logger.info("------------>" + sql)
    sql

  }

  def buildQueryDemographics(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
         select * from (select * from ENG_FB_DEMOGRAPHICS i where I.FK_ENG_ENGAGEMENT_DATA_QUER_ID  in
            (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where ID = ${x}  and  fk_profile_id=${profileId}))
              and item_date is not null
              and gender='M'    AND ITEM_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('2014/12/04', 'YYYY/MM/DD HH24:MI:SS')
          order by item_date desc)   where   rownum<=1
          union all
          select * from (select * from ENG_FB_DEMOGRAPHICS i where I.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where
              FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where ID = ${x}  and  fk_profile_id=${profileId}))
             and item_date is not null
             and gender='F'     AND ITEM_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          order by item_date desc)   where   rownum<=1
         """
      case None =>
        s"""
        select * from (select * from ENG_FB_DEMOGRAPHICS i where I.FK_ENG_ENGAGEMENT_DATA_QUER_ID  in
          (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS
                    where fk_profile_id=${profileId} and fk_datasource_id=1))
           and item_date is not null
            and gender='M'    AND ITEM_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          order by item_date desc)   where   rownum<=1
         union all
           select * from (select * from ENG_FB_DEMOGRAPHICS i where I.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where
             FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=1))
             and item_date is not null
             and gender='F'     AND ITEM_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           order by item_date desc)   where   rownum<=1
         """
    }

    sqlEngAccount
  }


  def buildQueryComments(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
         select id,message,comment_date,user_name,user_id,likes,fk_post_id,fk_eng_engagement_data_quer_id,comment_id, post_user_id
           from ENG_FB_WALL_COMMENTS
            where fk_eng_engagement_data_quer_id in  (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                      (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=1  and id = ${x}   ))
              and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          group by id,message,comment_date,user_name,user_id,likes,fk_post_id,fk_eng_engagement_data_quer_id,comment_id, post_user_id
          order by comment_date asc
         """
      case None =>
        s"""
        select id,message,comment_date,user_name,user_id,likes,fk_post_id,fk_eng_engagement_data_quer_id,comment_id, post_user_id
           from ENG_FB_WALL_COMMENTS
            where fk_eng_engagement_data_quer_id in  (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                      (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=1))
              and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          group by id,message,comment_date,user_name,user_id,likes,fk_post_id,fk_eng_engagement_data_quer_id,comment_id, post_user_id
          order by comment_date asc
         """
    }

    sqlEngAccount
  }


  def buildQueryPosts(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
          select id,message,msg_date, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
           from eng_fb_wall
            where fk_eng_engagement_data_quer_id in  (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                      (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=1 and id = ${x} ))
              and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('29-12-2014 00:00:00', 'DD-MM-YYYY HH24:MI:SS')
          group by id,message,msg_date, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
          order by msg_date asc
         """
      case None =>
        s"""
          select id,message,msg_date, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
           from eng_fb_wall
            where fk_eng_engagement_data_quer_id in  (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                      (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=1))
              and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          group by id,message,msg_date, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
          order by msg_date asc
         """
    }

    sqlEngAccount
  }

  def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")
    val grouBydate = DateUtils.sqlGrouByDate(numDays)

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = engId match {
      case Some(x) =>
        s"""
          SELECT NVL(FB_SW.QID, FB_COMM.QID) QID, NVL(FB_SW.MDATE, FB_COMM.MDATE) MDATE, PAGE_LIKES, POSTS, POST_LIKES, POST_SHARES, COMMENTS, COMM_LIKES, TALKING_ABOUT_COUNT, REACH, VIEWS, ENGAGED
          FROM ((SELECT NVL(FB_STATS.QID, FB_WALL.QID) QID, NVL(FB_STATS.MDATE, FB_WALL.MDATE) MDATE,  PAGE_LIKES,  POSTS, POST_LIKES, POST_SHARES,TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED
           FROM  (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, trunc(FFSL_DATE, '${grouBydate}') AS MDATE,  ROUND(MAX(FANPAGE_FANS)) PAGE_LIKES, TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED
           FROM ENG_FB_STATS
              WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                              (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where id = ${x} and fk_profile_id=${profileId} and fk_datasource_id=1))
                 AND FFSL_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID,TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED, trunc(FFSL_DATE, '${grouBydate}')) FB_STATS
               FULL OUTER JOIN
                (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, trunc(MSG_DATE, '${grouBydate}') AS MDATE,   COUNT(*) POSTS, SUM(LIKES) POST_LIKES, SUM(SHARES) POST_SHARES
                 FROM ENG_FB_WALL   WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                 (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where  id = ${x} and  fk_profile_id=${profileId} and fk_datasource_id=1))
                       AND MSG_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID, trunc(MSG_DATE, '${grouBydate}')) FB_WALL
                  ON FB_STATS.MDATE = FB_WALL.MDATE) FB_SW  FULL OUTER JOIN (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, trunc(COMMENT_DATE, '${grouBydate}') AS MDATE,  COUNT(*) COMMENTS,   SUM(LIKES) COMM_LIKES
                   FROM ENG_FB_WALL_COMMENTS
                     WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                      (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where  id = ${x} and fk_profile_id=${profileId} and fk_datasource_id=1))
                           AND COMMENT_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID, trunc(COMMENT_DATE, '${grouBydate}')) FB_COMM ON FB_SW.MDATE = FB_COMM.MDATE)
        ORDER BY MDATE
         """
      case None =>
        s"""
          SELECT NVL(FB_SW.QID, FB_COMM.QID) QID, NVL(FB_SW.MDATE, FB_COMM.MDATE) MDATE, PAGE_LIKES, POSTS, POST_LIKES, POST_SHARES, COMMENTS, COMM_LIKES, TALKING_ABOUT_COUNT, REACH, VIEWS, ENGAGED
          FROM ((SELECT NVL(FB_STATS.QID, FB_WALL.QID) QID, NVL(FB_STATS.MDATE, FB_WALL.MDATE) MDATE,  PAGE_LIKES,  POSTS, POST_LIKES, POST_SHARES,TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED
           FROM  (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, trunc(FFSL_DATE, '${grouBydate}') AS MDATE,  ROUND(MAX(FANPAGE_FANS)) PAGE_LIKES, TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED
           FROM ENG_FB_STATS
              WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                              (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where  fk_profile_id=${profileId} and fk_datasource_id=1))
                 AND FFSL_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID,TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED, trunc(FFSL_DATE, '${grouBydate}')) FB_STATS
               FULL OUTER JOIN
                (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, trunc(MSG_DATE, '${grouBydate}') AS MDATE,   COUNT(*) POSTS, SUM(LIKES) POST_LIKES, SUM(SHARES) POST_SHARES
                 FROM ENG_FB_WALL   WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                 (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=1))
                       AND MSG_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID, trunc(MSG_DATE, '${grouBydate}')) FB_WALL
                  ON FB_STATS.MDATE = FB_WALL.MDATE) FB_SW  FULL OUTER JOIN (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, trunc(COMMENT_DATE, '${grouBydate}') AS MDATE,  COUNT(*) COMMENTS,   SUM(LIKES) COMM_LIKES
                   FROM ENG_FB_WALL_COMMENTS
                     WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in
                      (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=1))
                           AND COMMENT_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID, trunc(COMMENT_DATE, '${grouBydate}')) FB_COMM ON FB_SW.MDATE = FB_COMM.MDATE)
        ORDER BY MDATE
         """
    }

    sqlEngAccount
  }


}

