package gr.gnostix.api.models.pgDao

import java.sql.Timestamp

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.pgDao.MySocialChannelDaoYt._
import gr.gnostix.api.models.plainModels._
import gr.gnostix.api.utilities.{DateUtils, SqlUtils, HelperFunctions}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 2/3/15.
 */
object MySocialChannelDaoGA extends DatabaseAccessSupportPg {

  implicit val getGaDataData = GetResult(r => GoogleAnalyticsData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getGaLineGraph = GetResult(r => GaLineData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getGoogleAnalytics(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, engId: Option[Int]): Future[Option[List[ApiData]]] = {
    val mySqlDynamic = buildQueryStats(fromDate, toDate, profileId, companyId, engId)
    val sqlLineGraph = buildQueryLineGraph(fromDate, toDate, profileId, companyId, engId)
    //bring the actual data
    val prom = Promise[Option[List[ApiData]]]()

    Future {
      prom.success(getGaStats(mySqlDynamic, sqlLineGraph))
    }
    prom.future
  }


  private def getGaStats(sql: String, sqlLineGraph: String): Option[List[ApiData]] = {

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

        // get hits by source
        val sourceHits = getSourceHits(myData)

        // get hits by operating system
        val osHits = getHitsByOS(myData)

        // get hits by browser
        val browserHits = getHitsByBrowser(myData)

        // get hits by Country
        val countryHits = getHitsByCountry(myData)

        // get all line graphs
        val gaLineGraph = getLineGraphAll(sqlLineGraph)

        logger.info(" -------------> we have  G analytics ")
        Some(List(ApiData("ga_stats", gaStats), ApiData("source_hits", sourceHits),
          ApiData("os_hits", osHits), ApiData("browser_hits", browserHits),
          ApiData("country_hits", countryHits), ApiData("ga_line_graph", gaLineGraph), ApiData("ga_data", myData)))

      } else {
        logger.info(" -------------> nodata ")
        Some(List(ApiData("ga_stats", List()), ApiData("source_hits", List()),
          ApiData("os_hits", List()), ApiData("browser_hits", List()), ApiData("country_hits", List()),
          ApiData("ga_line_graph", List()), ApiData("ga_data", myData)))
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  private def getLineGraphAll(sql: String): Option[List[GaLineData]] = {

//    li.groupBy(x => x.created).map {
//      case (x, y) => GaLineData(x, y.map(u => u.users).sum, y.map(nu => nu.newUsers).sum, y.map(nu => nu.bounces).sum,
//        y.map(b => b.bounceRate).sum / y.size, y.map(a => a.avgSessionDuration).sum / y.size, y.map(nu => nu.pageViews).sum)
//    }.toList.sortWith( (a,b) => a.created.before(b.created))


    try {
      var myData = List[GaLineData]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel fb ------------->" + sql)
          val records = Q.queryNA[GaLineData](sql)
          myData = records.list()
      }

      Some(myData)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }

  private def getHitsByCountry(li: List[GoogleAnalyticsData]): Map[String, Int] = {
    li.groupBy(x => x.country).map {
      case (x, y) => (x -> y.map(i => i.users).sum)
    }
  }

  private def getHitsByBrowser(li: List[GoogleAnalyticsData]): Map[String, Int] = {
    li.groupBy(x => x.browser).map {
      case (x, y) => (x -> y.map(i => i.users).sum)
    }
  }

  private def getHitsByOS(li: List[GoogleAnalyticsData]): Map[String, Int] = {
    li.groupBy(x => x.operatingSystem).map {
      case (x, y) => (x -> y.map(i => i.users).sum)
    }
  }

  private def getSourceHits(li: List[GoogleAnalyticsData]): Map[String, Int] = {
    li.groupBy(x => x.source).map {
      case (x, y) => (x -> y.map(i => i.users).sum)
    }
  }


  private def getStats(li: List[GoogleAnalyticsData]): GoogleAnalyticsStats = {
    val users = li.map(x => x.users).sum
    val newUsers = li.map(x => x.newUsers).sum
    val avgSessionDuration = li.map(x => x.avgSessionDuration).sum / li.filter(x => x.avgSessionDuration != 0).size
    val pageViews = li.map(x => x.pageViews).sum
    val bounces = li.map(x => x.bounces).sum
    val bounceRate = li.map(x => x.bounceRate).sum / li.size

    GoogleAnalyticsStats(users, newUsers, bounces, HelperFunctions.doublePrecision1(bounceRate), avgSessionDuration, pageViews)
  }

  private def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, 15, credId)

    val sql =
      s"""
        select country ,browser , operating_system,sessions,avg_session_duration, profileid
          ,profile_name ,created::date, source,search_used , session_count,
            page_views, session_duration, users, new_users, bounces, bounce_rate
            from vieras.eng_ga_stats
            where fk_eng_engagement_data_quer_id in (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where attr = 'GA_STATS'
                    and FK_PROFILE_SOCIAL_ENG_ID in  ( $sqlEngAccount  )
              and created between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
		          and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
         """

    sql
  }

  private def buildQueryLineGraph(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, credId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    val grouByDate = DateUtils.sqlGrouByDatePg(numDays)

    val sqlEngAccount = SqlUtils.buildSocialCredentialsQuery(profileId, companyId, 15, credId)

    val sql =
      s"""
            select date_trunc('${grouByDate}',created) as created, sum(users) as users, sum(new_users) as new_users,
              sum(bounces) as bounces, avg(bounce_rate) as bounce_rate, avg(avg_session_duration) as avg_session_duration, sum(page_views) as page_views
                        from vieras.eng_ga_stats
                        where fk_eng_engagement_data_quer_id in (select id from vieras.ENG_ENGAGEMENT_DATA_QUERIES where attr = 'GA_STATS'
                                and FK_PROFILE_SOCIAL_ENG_ID in  (  $sqlEngAccount  )
                          and created between to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                          and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
               group by date_trunc('${grouByDate}',created)
               order by date_trunc('${grouByDate}',created) asc

         """

    sql
  }

}
