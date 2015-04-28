package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels._
import gr.gnostix.api.utilities.{SqlUtils, DateUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 21/10/14.
 */
object MySocialChannelDaoFB extends DatabaseAccessSupportPg {
  implicit val getFbLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
  implicit val getFbDemographics = GetResult(r => FacebookDemographics(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getFbStats = GetResult(r => FacebookStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getFbComment = GetResult(r => FacebookComment(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getFbPost = GetResult(r => FacebookPost(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getFbPostPeakDataRaw = GetResult(r => FacebookPostWithCommentsRaw(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<,r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getLineCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Option[Payload] = {
    val sql = buildQuery(fromDate, toDate, profileId, companyId, dataType, engId)

    //bring the actual data
    val data = dataType match {
      case "post" | "comment" => getData(sql)
      case "totalpost" | "totalcomment" => getDataTotal(sql)
    }
    //val data = getData(fromDate, toDate, dataType, profileId, sql)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }

  def getLineAllData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[SocialData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, companyId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[SocialData]]()

    Future {
      prom.success(getData(mySqlDynamic))
    }
    prom.future
  }

  def getTotalSumData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[SocialDataSum]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, companyId, dataType, engId)
    //bring the actual data
    val prom = Promise[Option[SocialDataSum]]()

    Future {
      prom.success(getDataTotal(mySqlDynamic))
    }
    prom.future
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


  def getDemographics(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, engId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryDemographics(fromDate, toDate, profileId, companyId, engId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataDemographics(mySqlDynamic))
    }
    prom.future
  }

  // get raw data
  def getTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {

    val prom = Promise[Option[ApiData]]()
    val mySqlDynamic = dataType match {
      case "comment" => buildQueryComments(fromDate, toDate, profileId, companyId, engId)
      case "post" => buildQueryPosts(fromDate, toDate, profileId, companyId, engId)
    }

    Future {
      dataType match {
        case "comment" => prom.success(getCommentMessages(mySqlDynamic))
        case "post" => prom.success(getPostMessages(mySqlDynamic))
      }
    }

    prom.future
  }

  // get raw data
  def getPeakTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, peakDate: DateTime, profileId: Int, companyId: Int, dataType: String, engId: Option[Int]): Future[Option[ApiData]] = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val prom = Promise[Option[ApiData]]()
    val mySqlDynamic = dataType match {
      case "comment" => buildQueryCommentsPeakDate(peakDate, profileId, companyId, engId, grouBydate)
      case "post" => buildQueryPostsCommentsPeakDate(peakDate, profileId, companyId, engId, grouBydate)
    }

    Future {
      dataType match {
        case "comment" => prom.success(getCommentMessages(mySqlDynamic))
        case "post" => prom.success(getPostPeakMessages(mySqlDynamic))
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

      Some(ApiData("facebook_posts", myData))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getPostPeakMessages(sql: String): Option[ApiData] = {

    try {
      var myData = List[FacebookPostWithCommentsRaw]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[FacebookPostWithCommentsRaw](sql)
          myData = records.list()
      }

      Some(ApiData("facebook_posts", fixPostCommentsRawData(myData)))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def fixPostCommentsRawData(li: List[FacebookPostWithCommentsRaw]): List[FacebookPostWithComments] = {

    li.groupBy(_.postId).map{
      case (x,y) => FacebookPostWithComments(y.head.postMessage, y.head.postCreated, y.head.postUserName, y.head.postUserId,
        y.head.postLikes, y.head.postComments, y.head.postId, y.head.postShares,
        y.filter(_.commentId != null).map(x => FacebookPostWithCommentsOnlyRaw(x.commentMessage, x.commentDate, x.commentUserName,
        x.commentUserId, x.commentLikes, x.commentId)).toList)
    }.toList
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

      Some(ApiData("facebook_comments", myData))
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
        Some(ApiData("demographics", List()))
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
        Some(ApiData("stats", List()))
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getData(sql: String): Option[SocialData] = {

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

  private def getDataTotal(sql: String): Option[SocialDataSum] = {

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


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, dataType: String, credId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    logger.info("------------->" + sqlEngAccount + "-----------")
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    dataType match {
      case "post" => getSqlPosts(numDays, fromDateStr, toDateStr, sqlEngAccount)
      case "totalpost" => getSqlPostsTotal(numDays, fromDateStr, toDateStr, sqlEngAccount)
      case "comment" => getSqlComments(numDays, fromDateStr, toDateStr, sqlEngAccount)
      case "totalcomment" => getSqlCommentsTotal(numDays, fromDateStr, toDateStr, sqlEngAccount)
    }

  }

  def getSqlPostsTotal(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val sql = s"""
                  select count(*) from vieras.eng_fb_wall where fk_eng_engagement_data_quer_id in
                      ( select q.id from vieras.eng_engagement_data_queries q where q.is_active = 1
                        and q.attr = 'FB_FANPAGE_WALL'
                    and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                    and created between    to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS'))
                        """
    sql
  }

  def getSqlPosts(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val sql = s"""
                 select count(*),date_trunc('${grouBydate}',created) from vieras.eng_fb_wall
                      where fk_eng_engagement_data_quer_id in
                        ( select q.id from vieras.eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                      and created between  to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and date_trunc('${grouBydate}',created) >=  to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by date_trunc('${grouBydate}',created)
                    order by date_trunc('${grouBydate}',created) asc
                 """
    logger.info("------------>" + sql)
    sql

  }

  def getSqlCommentsTotal(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val sql = s"""
                  select count(*) from vieras.ENG_FB_WALL_COMMENTS  where fk_eng_engagement_data_quer_id in
                      (select q.id from vieras.eng_engagement_data_queries q  where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                     and created  between  to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                  """
    logger.info("------------>" + sql)
    sql
  }

  def getSqlComments(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val sql = s"""
                select count(*), date_trunc('${grouBydate}',created) from vieras.ENG_FB_WALL_COMMENTS
                       where fk_eng_engagement_data_quer_id in (select q.id from vieras.eng_engagement_data_queries q where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                  and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                  and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                  and date_trunc('${grouBydate}',created) >=  to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                 group by date_trunc('${grouBydate}',created)
                 order by date_trunc('${grouBydate}',created) asc
              """
    logger.info("------------>" + sql)
    sql

  }

  def buildQueryDemographics(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    val sql = s"""
        select fk_eng_engagement_data_quer_id,max(age_13_17),max(age_18_24),max(age_25_34),max(age_35_44),max(age_45_54),max(age_55_64),max(age_65_plus),gender,created
            from vieras.ENG_FB_DEMOGRAPHICS i where I.FK_ENG_ENGAGEMENT_DATA_QUER_ID  in
             (select q.id from vieras.eng_engagement_data_queries q where FK_PROFILE_SOCIAL_ENG_ID in  ( $sqlEngAccount )
            and created is not null and gender='M'
            and created between  to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          group by fk_eng_engagement_data_quer_id,gender,created
          union all
          select fk_eng_engagement_data_quer_id,max(age_13_17),max(age_18_24),max(age_25_34),max(age_35_44),max(age_45_54),max(age_55_64),max(age_65_plus),gender,created
           from vieras.ENG_FB_DEMOGRAPHICS i where I.FK_ENG_ENGAGEMENT_DATA_QUER_ID in
             (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where  FK_PROFILE_SOCIAL_ENG_ID in   ( $sqlEngAccount )
            and created is not null  and gender='F'
            and created between  to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          group by fk_eng_engagement_data_quer_id,gender,created
          order by created desc limit 2
         """


    sql
  }


  def buildQueryComments(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    val sql = s"""
          select id,message,created,user_name,user_id, likes,fk_post_id, fk_eng_engagement_data_quer_id, comment_id,  post_user_id
              from vieras.ENG_FB_WALL_COMMENTS  where fk_eng_engagement_data_quer_id in
                (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
              and created between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
          group by id,message,created,user_name,user_id,likes,fk_post_id,fk_eng_engagement_data_quer_id,comment_id, post_user_id
          order by created asc
         """

    sql
  }

  def buildQueryCommentsPeakDate(peakDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int], groupByDate: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val peakDateStr: String = fmt.print(peakDate)


    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    val sql = groupByDate match {
      case "hour" | "day" | "week" | "month" | "year" => {
        s"""
          select id,message,created,user_name,user_id, likes,fk_post_id, fk_eng_engagement_data_quer_id, comment_id,  post_user_id
              from vieras.ENG_FB_WALL_COMMENTS  where fk_eng_engagement_data_quer_id in
                (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
              and  created between   date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS'))
              and date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS')+ INTERVAL '1 ${groupByDate}')
              and created < date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS') + INTERVAL '1 ${groupByDate}')
          group by id,message,created,user_name,user_id,likes,fk_post_id,fk_eng_engagement_data_quer_id,comment_id, post_user_id
          order by created asc
         """
      }

    }


    sql
  }

  def buildQueryPostsCommentsPeakDate(peakDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int], groupByDate: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val peakDateStr: String = fmt.print(peakDate)
    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    val sql = groupByDate match {
      case "hour" | "day" | "week" | "month" | "year" => {
        s"""
    select p.message as post_msg, p.created as post_date, p.from_user as post_user, p.from_user_id as post_user_id, p.likes as post_likes,
    p.comments as post_comments, p.msg_id as post_id, p.shares as post_shares,
      m.message as comment_msg, m.created as comment_date, m.user_name  as comment_user_name, m.user_id  as comment_usr_id, m.likes  as comment_likes,
       m.comment_id as comment_id
               from vieras.eng_fb_wall p
      left join vieras.ENG_FB_WALL_COMMENTS m on p.msg_id=m.fk_post_id
               where p.fk_eng_engagement_data_quer_id in
                (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
                  and  p.created between   date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS'))
                  and date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS')+ INTERVAL '1 ${groupByDate}')
                  and p.created < date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS') + INTERVAL '1 ${groupByDate}')
             group by p.id, p.message, p.created, p.from_user, p.from_user_id, p.likes, p.comments, p.msg_id, p.shares,
        m.message, m.created, m.user_name, m.user_id, m.likes, m.comment_id
             order by p.created asc
         """
      }
    }

    sql
  }

  def buildQueryPostsPeakDate(peakDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int], groupByDate: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val peakDateStr: String = fmt.print(peakDate)
    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    val sql = groupByDate match {
      case "hour" | "day" | "week" | "month" | "year" => {
        s"""
          select id,message,created, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
           from vieras.eng_fb_wall where fk_eng_engagement_data_quer_id in
            (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
              and  created between   date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS'))
              and date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS')+ INTERVAL '1 ${groupByDate}')
              and created < date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS') + INTERVAL '1 ${groupByDate}')
         group by id,message,created, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
         order by created asc

         """
      }
    }

    sql
  }

  def buildQueryPosts(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    val sql = s"""
          select id,message,created, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
           from vieras.eng_fb_wall where fk_eng_engagement_data_quer_id in
            (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
              and created  between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         group by id,message,created, from_user,from_user_id,likes,comments, fk_eng_engagement_data_quer_id, msg_id,post_link,shares
         order by created asc

         """

    sql
  }

  def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")
    val grouByDate = DateUtils.sqlGrouByDatePg(numDays)

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, "facebook", credId)

    val sql = s"""
        SELECT coalesce(FB_SW.QID, FB_COMM.QID) QID, coalesce(FB_SW.MDATE, FB_COMM.MDATE) MDATE, PAGE_LIKES, POSTS, POST_LIKES, POST_SHARES, COMMENTS, COMM_LIKES, TALKING_ABOUT_COUNT, REACH, VIEWS, ENGAGED
          FROM ((SELECT coalesce(FB_STATS.QID, FB_WALL.QID) QID, coalesce(FB_STATS.MDATE, FB_WALL.MDATE) MDATE,  PAGE_LIKES,  POSTS, POST_LIKES, POST_SHARES,TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED
           FROM  (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, date_trunc('${grouByDate}',created) AS MDATE,  ROUND(MAX(FANPAGE_FANS)) PAGE_LIKES, TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED
           FROM vieras.ENG_FB_STATS WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
           AND created  between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID,TALKING_ABOUT_COUNT, REACH , VIEWS, ENGAGED, date_trunc('${grouByDate}',created)) FB_STATS
               FULL OUTER JOIN
                (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, date_trunc('${grouByDate}',created) AS MDATE,   COUNT(*) POSTS, SUM(LIKES) POST_LIKES, SUM(SHARES) POST_SHARES
                 FROM vieras.ENG_FB_WALL   WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
              AND created  between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
           GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID, date_trunc('${grouByDate}',created)) FB_WALL
                  ON FB_STATS.MDATE = FB_WALL.MDATE) FB_SW  FULL OUTER JOIN (SELECT FK_ENG_ENGAGEMENT_DATA_QUER_ID QID, date_trunc('${grouByDate}',created) AS MDATE,  COUNT(*) as COMMENTS,   SUM(LIKES) COMM_LIKES
                   FROM vieras.ENG_FB_WALL_COMMENTS
                     WHERE FK_ENG_ENGAGEMENT_DATA_QUER_ID in (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount )
            AND created  between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        GROUP BY FK_ENG_ENGAGEMENT_DATA_QUER_ID, date_trunc('${grouByDate}',created)) FB_COMM ON FB_SW.MDATE = FB_COMM.MDATE)
        ORDER BY MDATE
         """


    sql
  }


}

