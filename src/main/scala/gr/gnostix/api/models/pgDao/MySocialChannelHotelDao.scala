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
object MySocialChannelHotelDao extends DatabaseAccessSupportPg {
  implicit val getLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
  implicit val getReviewStats = GetResult(r => HotelReviewStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getRatingStats = GetResult(r => HotelRatingStats(r.<<, r.<<))
  implicit val getServicesLine = GetResult(r => HotelServicesLine(r.<<, r.<<, r.<<))
  //implicit val getHotelTextData = GetResult(r => HotelTextData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getHotelTextDataRating = GetResult(r => HotelTextDataRating(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))


  val logger = LoggerFactory.getLogger(getClass)
  val positiveScore = 7
  val negativeScore = 4


  def getDataCounts(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, dataType: String, datasourceId: Option[Int]): Option[Payload] = {
    val sql = buildQuery(fromDate, toDate, userId, profileId, companyId, dataType, datasourceId)

    //bring the actual data
    val data = dataType match {
      case "line" => getData(sql)
      case "total" => getDataTotal(dataType, sql)
    }

    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }

  def getServicesLineCountsAverageSentiment(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime
                                            , userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryServicesLineSentiment(fromDate, toDate, userId, profileId, companyId, datasourceId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataServicesLineSentiment(mySqlDynamic))
    }
    prom.future
  }

  def getReviewStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,
                     companyId: Int, datasourceId: Option[Int]): Future[Option[List[ApiData]]] = {
    val mySqlDynamic = buildQueryStats(fromDate, toDate, userId, profileId, companyId, datasourceId)
    //bring the actual data
    val prom = Promise[Option[List[ApiData]]]()

    Future {
      prom.success(getDataStats(mySqlDynamic))
    }
    prom.future
  }

  def getTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                  datasourceId: Option[Int]): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryTextData(fromDate, toDate, userId, profileId, companyId, datasourceId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRaw(mySqlDynamic))
    }
    prom.future
  }

  def getSentimentTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                  datasourceId: Option[Int], sentiment: String): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQuerySentimentTextData(fromDate, toDate, userId, profileId, companyId, datasourceId, sentiment)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRaw(mySqlDynamic))
    }
    prom.future
  }


  def getServiceTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                           datasourceId: Option[Int], service: String): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryServiceTextData(fromDate, toDate, userId, profileId, companyId, datasourceId, service)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRawService(mySqlDynamic))
    }
    prom.future
  }


  def getServiceBySentimentTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                           datasourceId: Option[Int], service: String, sentiment: String): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryServiceSentimentTextData(fromDate, toDate, userId, profileId, companyId, datasourceId, service, sentiment)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRawService(mySqlDynamic))
    }
    prom.future
  }


  def getStayTypeTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                           datasourceId: Option[Int], stayType: String): Future[Option[ApiData]] = {
    val mySqlDynamic = buildQueryStayTypeTextData(fromDate, toDate, userId, profileId, companyId, datasourceId, stayType)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRaw(mySqlDynamic))
    }
    prom.future
  }



  def getPeakTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, peakDate: DateTime
                      , userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int]): Future[Option[ApiData]] = {
    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val mySqlDynamic = grouBydate match {
      case  "hour" => buildQueryPeakTextData(peakDate, userId, profileId, companyId, datasourceId, "day") /// it is never by hour in review messages
      case "day" | "week" | "month" | "year" => buildQueryPeakTextData(peakDate, userId, profileId, companyId, datasourceId, grouBydate)
    }

    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRaw(mySqlDynamic))
    }
    prom.future
  }

  def getServicePeakTextData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, peakDate: DateTime
                      , userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int], service: String): Future[Option[ApiData]] = {
    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val mySqlDynamic = grouBydate match {
      case  "hour" => buildQueryServicePeakTextData(peakDate, userId, profileId, companyId, datasourceId, "day", service) /// it is never by hour in review messages
      case "day" | "week" | "month"  | "year" => buildQueryServicePeakTextData(peakDate, userId, profileId, companyId, datasourceId, grouBydate, service)
    }

    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRawService(mySqlDynamic))
    }
    prom.future
  }


  def getReviewRatingStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                           datasourceId: Option[Int]): Future[Option[List[ApiData]]] = {
    val mySqlDynamic = buildQueryRatingStats(fromDate, toDate, userId, profileId, companyId, datasourceId)
    //bring the actual data
    val prom = Promise[Option[List[ApiData]]]()

    Future {
      prom.success(getDataRatingStats(mySqlDynamic))
    }
    prom.future
  }


  def getDataCountsFuture(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                          dataType: String, datasourceId: Option[Int]): Future[Option[SocialData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, userId, profileId, companyId, dataType, datasourceId)
    //bring the actual data
    val prom = Promise[Option[SocialData]]()

    Future {
      prom.success(getData(mySqlDynamic))
    }
    prom.future
  }


  def getTotalSumDataFuture(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int,
                            dataType: String, datasourceId: Option[Int]): Future[Option[SocialDataSum]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, userId, profileId, companyId, dataType, datasourceId)
    //bring the actual data
    val prom = Promise[Option[SocialDataSum]]()

    Future {
      prom.success(getDataTotal(dataType, mySqlDynamic))
    }
    prom.future
  }


  private def getDataServicesLineSentiment(sql: String): Option[ApiData] = {
    try {
      var myData = List[HotelServicesLine]()
      getConnection withSession {
        implicit session =>
          logger.info("get services sentiment line ------------->" + sql)
          val records = Q.queryNA[HotelServicesLine](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> data services sentiment line " )
        val cleanData = myData.groupBy(x => x.created.toString).map {
          case (x, y) => (x, y.groupBy(r => r.ratingName).map {
            case (w, s) => (w, (BigDecimal(s.map(_.ratingValue).sum / s.size).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble))
          })
        }

        Some(ApiData("services_line", cleanData.toList.sortBy(_._1)))
      } else {
        logger.info(" -------------> nodata ")
        Some(ApiData("services_line", List()))
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }


  private def getDataRatingStats(sql: String): Option[List[ApiData]] = {
    try {
      var myData = List[HotelRatingStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel rating stats ------------->" + sql)
          val records = Q.queryNA[HotelRatingStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have hotel rating stats " + myData)

        val (neg, pos) = getTopMinusMaxReviews(myData)

        val negative_tips = neg.map(x => {
          val review = if (x.msgNum > 1) "reviews" else "review"
          (x.serviceName -> s""" ${x.msgNum} ${review} mentioned the hotel ${x.serviceName} negatively """)
        })
        val positive_tips = pos.map(x => {
          val review = if (x.msgNum > 1) "reviews" else "review"
          (x.serviceName -> s"""Based on ${x.msgNum} ${review}, the hotel ${x.serviceName} is mentioned positively""")
        })

        val tips = Map("positive_tips" -> positive_tips, "negative_tips" -> negative_tips)
        // geographic data
 /*       val value = myData.filter(x => x.ratingName.equalsIgnoreCase("value")).groupBy(_.ratingValue)
          .map {
          case (a, s) => (a -> s.size)
        }.toList


        val staff = myData.groupBy(_.ratingName).filter(_._1 contains "staff")
        val room = myData.groupBy(_.ratingName).filter(_._1 contains "room")
        val cleanliness = myData.groupBy(_.ratingName).filter(_._1 contains "cleanliness")
        val sleep = myData.groupBy(_.ratingName).filter(_._1 contains "sleep")
        val location = myData.groupBy(_.ratingName).filter(_._1 contains "location")


        val ratingTips = Map("value" -> value, "staff" -> staff, "room" -> room, "cleanliness" -> cleanliness, "sleep" -> sleep, "location" -> location)
*/
        val servicesStats = getServicesAverageRating(myData)

        Some(List(ApiData("servicesStats", servicesStats), ApiData("tips", tips)))

      } else {
        logger.info(" -------------> nodata ")
        Some(List(ApiData("servicesStats", List()), ApiData("tips", List())))
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }

  /**
   * returns for the services in this map the average score
   * @param li
   * @return
   */
  private def getServicesAverageRating(li: List[HotelRatingStats]): Map[String, Double] = {

    li.groupBy(_.ratingName).map {
      case (x, y) => (x, y.map(_.ratingValue).sum / y.size)
    }
  }


  private def getTopMinusMaxReviews(li: List[HotelRatingStats]): (List[ServiceSentiment], List[ServiceSentiment]) = {
    /*      ------------ Test data --------------
    * val li = List(HotelRatingStats("Value", 10), HotelRatingStats("Value", 8),
  HotelRatingStats("Value", 10), HotelRatingStats("Value", 8), HotelRatingStats("Value", 6),
  HotelRatingStats("sleep", 6), HotelRatingStats("staff", 6), HotelRatingStats("room", 8),
  HotelRatingStats("location", 10), HotelRatingStats("room", 4), HotelRatingStats("staff", 6),
  HotelRatingStats("room", 4), HotelRatingStats("sleep", 8), HotelRatingStats("Value", 10),
  HotelRatingStats("location", 8), HotelRatingStats("staff", 10), HotelRatingStats("staff", 6),
  HotelRatingStats("clean", 1), HotelRatingStats("staff", 10), HotelRatingStats("location", 6),
  HotelRatingStats("staff", 4), HotelRatingStats("sleep", 6), HotelRatingStats("staff", 8),
  HotelRatingStats("sleep", 6), HotelRatingStats("location", 8), HotelRatingStats("Value", 8),
  HotelRatingStats("clean", 4), HotelRatingStats("clean", 8), HotelRatingStats("staff", 6),
  HotelRatingStats("sleep", 8), HotelRatingStats("clean", 1), HotelRatingStats("location", 10),
  HotelRatingStats("room", 10), HotelRatingStats("sleep", 4), HotelRatingStats("staff", 6),
  HotelRatingStats("location", 10), HotelRatingStats("staff", 8), HotelRatingStats("sleep", 8))*/

/*

    val firstStep = li.
      groupBy(_.ratingName).map {
      case (x, y) => (x, y.groupBy(_.ratingValue).map {
        case (a, s) => RevStat(s.head.ratingName, a, s.size)
      })
    }

    val secondStep = firstStep.toStream.map {
      case (q, w) => {
        List(w.toList.sortBy(r => (r.score, r.numMsg)).head,
          w.toList.sortBy(r => (r.score, r.numMsg)).reverse.head)
      }
    }

    val massagedData =
      secondStep.toList.flatten.sortBy(n => (n.score, n.numMsg))

    val neg = massagedData.take(5).toList.sortBy(x => x.service_name)
    val pos = massagedData.reverse.take(5).toList.sortBy(x => x.service_name)
*/

    val pos = li.filter(_.ratingValue >= positiveScore).groupBy(_.ratingName).map{
      case (x,y) => ServiceSentiment(x, y.size)
    }.toList.sortBy(_.msgNum).reverse.take(5)

    val neg = li.filter(_.ratingValue <= negativeScore).groupBy(_.ratingName).map{
      case (x,y) => ServiceSentiment(x, y.size)
    }.toList.sortBy(_.msgNum).reverse.take(5)


    (neg, pos)
  }

  private def getTextDataRaw(sql: String): Option[ApiData] = {
    try {
      var myData = List[HotelTextDataRating]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel HotelTextData ------------->" + sql)
          val records = Q.queryNA[HotelTextDataRating](sql)
          myData = records.list()
      }

      Some(ApiData("reviews", GeoLocationDao.fixRatingTextData(myData)))

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }

  private def getTextDataRawService(sql: String): Option[ApiData] = {
    try {
      var myData = List[HotelTextDataRating]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel HotelTextData ------------->" + sql)
          val records = Q.queryNA[HotelTextDataRating](sql)
          myData = records.list()
      }

      Some(ApiData("reviews", GeoLocationDao.fixRatingTextData(myData)))

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }



  private def getDataStats(sql: String): Option[List[ApiData]] = {

    try {
      var myData = List[HotelReviewStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel stats ------------->" + sql)
          val records = Q.queryNA[HotelReviewStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have hotel stats ")

        // top boxes stats
        val stats = Map("score" -> myData.map(x => x.datasourceHotelRating).max,
          "outOf" -> myData.map(x => x.maxHotelScore).max,
          "reviewsNum" -> myData.size,
          "positive" -> myData.filter(x => x.vierasReviewRating >= positiveScore).size,
          "negative" -> myData.filter(x => x.vierasReviewRating <= negativeScore).size)

        //logger.info(" -------------> negative reviews " + myData.filter(x => x.vierasReviewRating <= negativeScore))
        // stay type graph
        /*        val stayType = myData.groupBy(_.stayType).map{
                  case (name, tuple) => (name -> tuple.size)
                }.toMap*/


        val stayType = {
          val cleanData = myData.filter(x => x.stayType != null)

          /*Map("couple" -> cleanData.filter(x => x.stayType.toLowerCase.contains("couple")
            || x.stayType.toLowerCase.contains("partner")).size, //add also partner
            "friend" -> cleanData.filter(_.stayType.toLowerCase.contains("friend")).size,
            "business" -> cleanData.filter(_.stayType.toLowerCase.contains("business")).size,
            "family" -> cleanData.filter(_.stayType.toLowerCase.contains("famil")).size,
            "solo" -> cleanData.filter(x => x.stayType.toLowerCase.contains("solo")
              || x.stayType.toLowerCase.contains("person")).size)
          */
          cleanData.groupBy(_.stayType).map{
            case (x,y) => (x -> y.size)
          }
        }

        // geographic data
        val countries = myData.groupBy(_.country).map {
          case (x, y) => (x -> y.size)
        }.toList.sortBy(_._2).toMap

        Some(List(ApiData("stats", stats), ApiData("countries", countries), ApiData("stayType", stayType)))

      } else {
        logger.info(" -------------> nodata ")
        Some(List(ApiData("stats", List()), ApiData("countries", List()), ApiData("stayType", List())))
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
          logger.info("get my social channel hotel ------------->" + sql)
          val records = Q.queryNA[DataLineGraph](sql)
          myData = records.list()
      }

      val lineData = SocialData("hotel", myData)

      lineData match {
        case SocialData(_, _) => Option(lineData)
      }
    } catch {
      case e: Exception => None
    }

  }

  private def getDataTotal(dataType: String, sql: String): Option[SocialDataSum] = {

    try {
      var myDataTotal = 0
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel hotel ------------->" + sql)
          val records = Q.queryNA[Int](sql)
          myDataTotal = records.first()
      }

      val sumData = SocialDataSum("hotel", myDataTotal)

      Option(sumData)
    } catch {
      case e: Exception => None
    }

  }

  private def buildQueryStats(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)


    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }


    val sql =         s"""
        select r.ID,r.REVIEWER ,r.VIERAS_STAY_TYPE, r.VIERAS_COUNTRY, r.VIERAS_TOTAL_RATING as vieras_review_rating,
             h.TOTAL_RATING  as datasource_hotel_rating, vd.ds_rating_scale as max_hotel_rating
        from vieras.ENG_REVIEWS r, vieras.eng_hotels h, vieras.vieras_datasources vd, vieras.ENG_PROFILE_HOTEL_CREDENTIALS cre
           where r.FK_HOTEL_ID IN ( ${sqlEngAccount} )
              and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and r.FK_HOTEL_ID = h.ID
              and cre.FK_HOTEL_ID = h.id
              and vd.id = cre.fk_datasource_id
              and r.vieras_total_rating is not null
              group by r.id,r.REVIEWER ,r.VIERAS_STAY_TYPE, r.VIERAS_COUNTRY, r.VIERAS_TOTAL_RATING ,h.TOTAL_RATING , vd.ds_rating_scale
        """


    //logger.info("------------->" + sql + "-----------")

    sql
  }


  private def buildQueryPeakTextData(peakDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int], groupByDate: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val peakDateStr: String = fmt.print(peakDate)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }


    val sql =     s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 140),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.vieras_stay_type
          , ra.vieras_rating_name, ra.vieras_rating_value
                from  vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr, vieras.vieras_datasources dt, vieras.ENG_REVIEWS r
                 left join vieras.eng_review_rating ra on r.id=ra.fk_review_id
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and r.created between   date_trunc('${groupByDate}', to_date('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS'))
                      and date_trunc('${groupByDate}', to_date('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS')+ INTERVAL '1 ${groupByDate}' )
                      and r.created < date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS') + INTERVAL '1 ${groupByDate}')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
        """


    //logger.info("------------->" + sql + "-----------")

    sql
  }

  private def buildQueryServicePeakTextData(peakDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int], groupByDate: String, service: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val peakDateStr: String = fmt.print(peakDate)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }


    val sql =     s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 140),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.vieras_stay_type,
           ra.vieras_rating_name, ra.vieras_rating_value
                from vieras.ENG_REVIEWS r, vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr,
                vieras.vieras_datasources dt, vieras.eng_review_rating ra
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and r.id = ra.fk_review_id
                      and LOWER(ra.vieras_rating_name) = LOWER('${service}')
                      and r.created between   date_trunc('${groupByDate}', to_date('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS'))
                      and date_trunc('${groupByDate}', to_date('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS')+ INTERVAL '1 ${groupByDate}')
                      and r.created < date_trunc('${groupByDate}', to_timestamp('${peakDateStr}' ,'DD-MM-YYYY HH24:MI:SS') + INTERVAL '1 ${groupByDate}')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
        """


    //logger.info("------------->" + sql + "-----------")

    sql
  }



  private def buildQueryTextData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }


    val sql =         s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 240),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.vieras_stay_type
          , ra.vieras_rating_name, ra.vieras_rating_value
                from vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr, vieras.vieras_datasources dt, vieras.ENG_REVIEWS r
                 left join vieras.eng_review_rating ra on r.id=ra.fk_review_id
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
        """


    //logger.info("------------->" + sql + "-----------")

    sql
  }



  private def buildQuerySentimentTextData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int], sentiment: String): String = {

    // here we are asking the review rating!
    val sentValue = sentiment match {
      case "positive" => s"""r.vieras_total_rating >= ${positiveScore}"""
      case "negative" => s"""r.vieras_total_rating <= ${negativeScore}"""
      case _ => s"""r.vieras_total_rating <= 0""" //for fan..
    }

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }

    val sql = s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 240),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.vieras_stay_type
          , ra.vieras_rating_name, ra.vieras_rating_value
                from vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr, vieras.vieras_datasources dt, vieras.ENG_REVIEWS r
                left join vieras.eng_review_rating ra on r.id=ra.fk_review_id
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and ${sentValue}
                      and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
         """


    //logger.info("------------->" + sql + "-----------")

    sql
  }


  private def buildQueryServiceSentimentTextData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int], service: String , sentiment: String): String = {

    // this is diferrent frm the orevious one because we are asking here for service rating and not the review rating
    val sentValue = sentiment match {
      case "positive" => s"""ra.vieras_rating_value >= ${positiveScore}"""
      case "negative" => s"""ra.vieras_rating_value <= ${negativeScore}"""
      case _ => s"""r.vieras_total_rating <= 0""" //for fan..
    }

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)



    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }

    val sql =         s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 240),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.vieras_stay_type
          ,ra.vieras_rating_name, ra.vieras_rating_value
                from vieras.ENG_REVIEWS r, vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr, vieras.vieras_datasources dt,
                vieras.eng_review_rating ra
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and r.id = ra.fk_review_id
                      and LOWER(ra.vieras_rating_name) like LOWER('%${service}%')
                      and ${sentValue}
                      and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
        """

    //logger.info("------------->" + sql + "-----------")

    sql
  }




  private def buildQueryServiceTextData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int], service: String ): String = {


    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)



    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }

    val sql =         s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 240),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.vieras_stay_type
          ,ra.vieras_rating_name, ra.vieras_rating_value
                from vieras.ENG_REVIEWS r, vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr, vieras.vieras_datasources dt,
                vieras.eng_review_rating ra
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and r.id = ra.fk_review_id
                      and LOWER(ra.vieras_rating_name) like LOWER('%${service}%')
                      and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
        """

    //logger.info("------------->" + sql + "-----------")

    sql
  }




  private def buildQueryStayTypeTextData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int], stayType: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }


    val sql =         s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 240),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.vieras_stay_type
          , ra.vieras_rating_name, ra.vieras_rating_value
                from vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr, vieras.vieras_datasources dt, vieras.ENG_REVIEWS r
                left join vieras.eng_review_rating ra on r.id=ra.fk_review_id
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and LOWER(vieras_stay_type) like LOWER('%${stayType}%')
                      and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
        """

    //logger.info("------------->" + sql + "-----------")

    sql
  }




  private def buildQueryServicesLineSentiment(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }


    val sql =         s"""
        select i.vieras_rating_name,i.vieras_rating_value, date_trunc('${grouBydate}',r.created)
          from vieras.ENG_REVIEW_RATING i,vieras.ENG_REVIEWS r,vieras.ENG_PROFILE_HOTEL_CREDENTIALS f
          where i.fk_review_id = r.id and r.fk_hotel_id = f.fk_hotel_id
            and f.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and r.CREATED between to_timestamp('${fromDateStr}', 'dd-mm-yyyy hh24:mi:ss')
                      and to_timestamp('${toDateStr}', 'dd-mm-yyyy hh24:mi:ss')
                      and vieras_rating_name is not null
                      group by vieras_rating_name,vieras_rating_value, date_trunc('${grouBydate}',r.created)
                      order by date_trunc('${grouBydate}',r.created)
         """

    //logger.info("------------->" + sql + "-----------")

    sql
  }


  private def buildQueryRatingStats(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, datasourceId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(userId, profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)
    }



    val sql =         s"""
            select hr.VIERAS_RATING_NAME, hr.VIERAS_RATING_VALUE  from vieras.ENG_REVIEWS r, vieras.eng_review_rating hr
                 where FK_HOTEL_ID IN ( ${sqlEngAccount} )
                    and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and r.ID = hr.fk_review_id
                    and hr.VIERAS_RATING_NAME is not null
         """

    //logger.info("------------->" + sql + "-----------")

    sql
  }

  /**
   *
   * @param fromDate
   * @param toDate
   * @param profileId
   * @param datasourceId
   * @return a sql query combination of datasource id and credentials id if present on of those or both (pre profile id)
   */
  private def buildQuery(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  companyId: Int, dataType: String, datasourceId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelCredIdQuery(userId, profileId, companyId, x)
      case None => SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)

    }
    logger.info("------------->" + sqlEngAccount + "-----------")
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    dataType match {
      case "line" => getSqlHotelDataLine(numDays, fromDateStr, toDateStr, sqlEngAccount)
      case "total" => getSqlHotelDataTotal(numDays, fromDateStr, toDateStr, sqlEngAccount)
    }

  }

  private def getSqlHotelDataTotal(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val sql = s"""
    select count(*) from vieras.ENG_REVIEWS
      where FK_HOTEL_ID IN ( $sqlEngAccount )
        and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
	      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        """
    sql
  }

  private def getSqlHotelDataLine(numDays: Int, fromDateStr: String, toDateStr: String, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val sql = s"""
        select count(*),date_trunc('${grouBydate}',created) from vieras.ENG_REVIEWS
          where FK_HOTEL_ID IN ( $sqlEngAccount )
             and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
             and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
             and date_trunc('${grouBydate}',created) >= to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        group by date_trunc('${grouBydate}',created)
        order by date_trunc('${grouBydate}',created) asc
                     """
    logger.info("------------>" + sql)
    sql

  }


}



