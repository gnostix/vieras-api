package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.slick.jdbc.{StaticQuery => Q, GetResult}


import scala.slick.jdbc.GetResult

/**
 * Created by rebel on 21/10/14.
 */
object MySocialChannelDaoFB extends DatabaseAccessSupport {
  implicit val getFbLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
  implicit val getFbDemographics = GetResult(r => FacebookDemographics(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

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

  def getDemographics(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryDemographics(fromDate, toDate, profileId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataDemographics(mySqlDynamic))
    }
    prom.future
  }

  case class FemaleFB(female: Int)

  case class MaleFB(male: Int)

  case class DemographicsDataFB(female: Int, male: Int, age: List[Int], rawData: List[FacebookDemographics])


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

        val age = List(maleData.age17 + femaleData.age17, maleData.age24 + femaleData.age24,maleData.age34 + femaleData.age34,
                        maleData.age44 + femaleData.age44,maleData.age54 + femaleData.age54,maleData.age64 + femaleData.age64,
                        maleData.age65Plus + femaleData.age65Plus)


        Some(ApiData("demographics", DemographicsDataFB(female, male, age, myData)))
      } else {
        Some(ApiData("nodata", None))
      }

    } catch {
      case e: Exception => None
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
      case e: Exception => None
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
      case e: Exception => None
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
    if (numDays == 0) {
      val sql = s"""select count(*),trunc(msg_date,'HH') from eng_fb_wall
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(msg_date,'HH')
                    order by trunc(msg_date,'HH')asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays >= 1 && numDays <= 30) {
      val sql = s"""select count(*),trunc(msg_date) from eng_fb_wall
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and FK_PROFILE_SOCIAL_ENG_ID in (  $sqlEngAccount )
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(msg_date)
                    order by trunc(msg_date)asc"""

      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*),trunc(msg_date,'ww') from eng_fb_wall
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and FK_PROFILE_SOCIAL_ENG_ID in (  $sqlEngAccount )
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(msg_date,'ww')
                    order by trunc(msg_date,'ww')asc"""
      sql
    } else {
      val sql = s"""select count(*),trunc(msg_date,'month') from eng_fb_wall
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and FK_PROFILE_SOCIAL_ENG_ID in (  $sqlEngAccount )
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(msg_date,'month')
                    order by trunc(msg_date,'month')asc"""
      sql
    }
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

    if (numDays == 0) {
      val sql = s"""select count(*),trunc(comment_date,'HH') from ENG_FB_WALL_COMMENTS
                      where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
                        where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                          and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(comment_date,'HH')
                    order by trunc(comment_date,'HH')asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays >= 1 && numDays <= 30) {
      val sql = s"""select count(*),trunc(comment_date,'DD') from ENG_FB_WALL_COMMENTS
                      where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
                        where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                          and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(comment_date,'DD')
                    order by trunc(comment_date,'DD')asc"""

      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*),trunc(comment_date,'ww') from ENG_FB_WALL_COMMENTS
                      where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
                        where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                          and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(comment_date,'ww')
                    order by trunc(comment_date,'ww')asc"""
      sql
    } else {
      val sql = s"""select count(*),trunc(comment_date,'month') from ENG_FB_WALL_COMMENTS
                      where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
                        where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                          and comment_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(comment_date,'month')
                    order by trunc(comment_date,'month')asc"""
      sql
    }
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
            (select id from ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where ID = ${engId}  and  fk_profile_id=${profileId}))
              and item_date is not null
              and gender='M'    AND ITEM_DATE BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('2014/12/04', 'YYYY/MM/DD HH24:MI:SS')
          order by item_date desc)   where   rownum<=1
          union all
          select * from (select * from ENG_FB_DEMOGRAPHICS i where I.FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from ENG_ENGAGEMENT_DATA_QUERIES where
              FK_PROFILE_SOCIAL_ENG_ID in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where ID = ${engId}  and  fk_profile_id=${profileId}))
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


}
