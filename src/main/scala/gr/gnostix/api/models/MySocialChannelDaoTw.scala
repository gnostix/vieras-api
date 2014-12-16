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
  object MySocialChannelDaoTw extends DatabaseAccessSupport {
    implicit val getTwLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
    implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
    implicit val getMentionsFavs = GetResult(r => TwitterMentionFav(r.<<, r.<<,r.<<, r.<<,r.<<, r.<<,r.<<, r.<<,r.<<, r.<<))
    implicit val getTwitterRetweets = GetResult(r => TwitterRetweets(r.<<,r.<<, r.<<,r.<<, r.<<,r.<<))
    implicit val getTwitterStats = GetResult(r => TwitterStats(r.<<, r.<<,r.<<, r.<<,r.<<, r.<<,r.<<, r.<<,r.<<))

    val logger = LoggerFactory.getLogger(getClass)


    def getLineCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Option[Payload] = {
      val sql = buildQuery(fromDate, toDate, profileId, dataType, engId)

      //bring the actual data
      val data = dataType match {
        case "mention" | "retweet" | "favorite" => getData(fromDate, toDate, sql)
        case "totalmention" | "totalfavorite" | "totalretweet" => getDataTotal(fromDate, toDate, sql)
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
        prom.success(getData(fromDate, toDate, mySqlDynamic))
      }
      prom.future
    }

    def getTotalSumData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, engId: Option[Int]): Future[Option[SocialDataSum]] = {
      val mySqlDynamic = buildQuery(fromDate, toDate, profileId, dataType, engId)
      //bring the actual data
      val prom = Promise[Option[SocialDataSum]]()

      Future {
        prom.success(getDataTotal(fromDate, toDate, mySqlDynamic))
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

/*        if (myData.size > 0) {
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
        }*/
        Some(ApiData("nodata", None))
      } catch {
        case e: Exception => {
          e.printStackTrace()
          None
        }
      }

    }

    private def getData(fromDate: DateTime, toDate: DateTime,  sql: String): Option[SocialData] = {

      try {
        var myData = List[DataLineGraph]()
        getConnection withSession {
          implicit session =>
            logger.info("get my social channel twitter ------------->" + sql)
            val records = Q.queryNA[DataLineGraph](sql)
            myData = records.list()
        }

        val lineData = SocialData("twitter", myData)

        lineData match {
          case SocialData(_, _) => Option(lineData)
        }
      } catch {
        case e: Exception => None
      }

    }

    private def getDataTotal(fromDate: DateTime, toDate: DateTime,  sql: String): Option[SocialDataSum] = {

      try {
        var myDataTotal = 0
        getConnection withSession {
          implicit session =>
            logger.info("get my social channel twitter -------------> " + sql)
            val records = Q.queryNA[Int](sql)
            myDataTotal = records.first()
        }

        val sumData = SocialDataSum("twitter", myDataTotal)

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

    def getSqlMentionFavTotal(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String, dataType: String) = {
      val twType = dataType match {
        case "mention" => "TW_MENTIONS"
        case "favorite" => "TW_FAVORITES"
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

    def getSqlMentionFav(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String, dataType: String) = {
      val twType = dataType match {
        case "mention" => "TW_MENTIONS"
        case "favorite" => "TW_FAVORITES"
      }

      if (numDays == 0) {
        val sql = s"""
      select count(*),trunc(created_at,'HH') from ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'HH') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'HH')
                    order by trunc(created_at,'HH')asc
                     """
        logger.info("------------>" + sql)
        sql
      } else if (numDays >= 1 && numDays <= 30) {
        val sql = s"""select count(*),trunc(created_at, 'DD') from ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'DD') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at, 'DD')
                    order by trunc(created_at, 'DD')asc"""

        sql
      } else if (numDays > 30 && numDays < 90) {
        val sql = s"""select count(*),trunc(created_at,'ww') from ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'ww') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'ww')
                    order by trunc(created_at,'ww')asc"""
        sql
      } else {
        val sql = s"""select count(*),trunc(created_at,'month') from ENG_TW_MENT_AND_FAV
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = '${twType}'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'month') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'month')
                    order by trunc(created_at,'month')asc"""
        sql
      }
    }

    def getSqlRetweetTotal(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
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

    def getSqlRetweet(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {

      if (numDays == 0) {
        val sql = s"""
       select count(*),trunc(created_at,'HH') from ENG_TW_RETWEETS
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'HH') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'HH')
                    order by trunc(created_at,'HH')asc
                     """
        logger.info("------------>" + sql)
        sql
      } else if (numDays >= 1 && numDays <= 30) {
        val sql = s"""
              select count(*),trunc(created_at,'DD') from ENG_TW_RETWEETS
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'DD') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'DD')
                    order by trunc(created_at,'DD')asc
                    """

        sql
      } else if (numDays > 30 && numDays < 90) {
        val sql = s"""
              select count(*),trunc(created_at,'ww') from ENG_TW_RETWEETS
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'ww') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'ww')
                    order by trunc(created_at,'ww')asc
                    """
        sql
      } else {
        val sql = s"""
              select count(*),trunc(created_at,'month') from ENG_TW_RETWEETS
        where fk_eng_engagement_data_quer_id in (select q.id from eng_engagement_data_queries q
          where q.is_active = 1 and q.attr = 'TW_RETWEETS'
            and FK_PROFILE_SOCIAL_ENG_ID in ( $sqlEngAccount  )
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                     and trunc(created_at,'month') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    group by trunc(created_at,'month')
                    order by trunc(created_at,'month')asc
                    """
        sql
      }
    }



    def buildQueryMentionsFavs(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int], dataType: String): String = {

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


    def buildQueryRetweets(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

      val datePattern = "dd-MM-yyyy HH:mm:ss"
      val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
      val fromDateStr: String = fmt.print(fromDate)
      val toDateStr: String = fmt.print(toDate)

      val sqlEngAccount = engId match {
        case Some(x) =>
          s"""
            select t.ID, t.CREATED_AT, t.RETWEET_STATUS_ID, t.RETWEETED_COUNT,t.RETWEETED_TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID from ENG_TW_RETWEETS t
             where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q where fk_profile_social_eng_id
             in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=2 and id = ${x} ))
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            order by created_at asc
         """
        case None =>
          s"""
            select t.ID, t.CREATED_AT, t.RETWEET_STATUS_ID, t.RETWEETED_COUNT,t.RETWEETED_TEXT, t.FK_ENG_ENGAGEMENT_DATA_QUER_ID from ENG_TW_RETWEETS t
             where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q where fk_profile_social_eng_id
             in (select id from ENG_PROFILE_SOCIAL_CREDENTIALS where fk_profile_id=${profileId} and fk_datasource_id=2 ))
                     and created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            order by created_at asc
         """
      }

      sqlEngAccount
    }


    def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): String = {

      val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
      logger.info("------------->" + numDays + "-----------")
      val grouBydate = numDays match {
        case 0 => "HH"
        case x if 0 until 30 contains x => "DD"
        case x if 31 until 90 contains x => "ww"
        case x if x > 90 => "month"
      }

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

