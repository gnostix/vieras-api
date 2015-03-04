package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.pgDao.MySocialChannelDaoYt._
import gr.gnostix.api.models.plainModels._
import gr.gnostix.api.utilities.HelperFunctions
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 2/3/15.
 */
object MySocialChannelDaoGA extends DatabaseAccessSupportPg {

  implicit val getGaDataData = GetResult(r => GoogleAnalyticsData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<,r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<,r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getGoogleAnalytics(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, engId: Option[Int]): Future[Option[List[ApiData]]] = {
    val mySqlDynamic = buildQueryStats(fromDate, toDate, profileId, engId)
    //bring the actual data
    val prom = Promise[Option[List[ApiData]]]()

    Future {
      prom.success(getGaStats(mySqlDynamic))
    }
    prom.future
  }


  private def getGaStats(sql: String): Option[List[ApiData]] = {

    try {
      var myData = List[GoogleAnalyticsData]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social G analytics ------------->" + sql)
          val records = Q.queryNA[GoogleAnalyticsData](sql)
          myData = records.list()
      }

      if (myData.size > 0) {

        // create the stats object  GoogleAnalyticsStats
       val gaStats = getStats(myData)


        logger.info(" -------------> we have  G analytics ")
        Some(List(ApiData("ga_stats", gaStats),ApiData("ga_data", myData)))
      } else {
        logger.info(" -------------> nodata ")
        Some(List(ApiData("ga_stats", List()), ApiData("ga_data", myData)))
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getStats(li: List[GoogleAnalyticsData]): GoogleAnalyticsStats = {
    val users = li.map(x => x.users).sum
    val newUsers = li.map(x => x.newUsers).sum
    val avgSessionDuration = li.map(x => x.avgSessionDuration).sum / li.size
    val pageViews = li.map(x => x.pageViews).sum
    val bounces = li.map(x => x.bounces).sum
    val bounceRate = li.map(x => x.bounceRate).sum / li.size

    GoogleAnalyticsStats(users, newUsers, bounces, HelperFunctions.doublePrecision1(bounceRate), avgSessionDuration, pageViews)
  }

  private def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = buildCredentialsQuery(profileId, credId)

    val sql =
      s"""
        select country ,browser , operating_system,sessions,avg_session_duration, profileid
          ,profile_name ,created, source,search_used , session_count,
            page_views, session_duration, users, new_users, bounces, bounce_rate
            from vieras.eng_ga_stats
            where fk_eng_engagement_data_quer_id in (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where attr = 'GA_STATS'
                    and FK_PROFILE_SOCIAL_ENG_ID in  ( $sqlEngAccount  )
              and created between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
		          and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """

    sql
  }


  private def buildCredentialsQuery(profileId: Int, credId: Option[Int]): String = {
    credId match {
      case Some(x) => x + " )"
      case None => "select s.id from vieras.eng_profile_social_credentials s where s.fk_profile_id in (" + profileId + ") and s.fk_datasource_id = 15)"
    }
  }

}
