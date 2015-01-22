package gr.gnostix.api.models.oraDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 18/11/14.
 */
case class Sentiment(positive: Int, negative: Int, neutral: Int)

case class HospitalityServicesSentiment(serviceName: String, score: Int, sentiment: Sentiment)

object HospitalityServicesDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  def getByName(implicit ctx: ExecutionContext, serviceName: String, profileId: Int, fromDate: DateTime,
                toDate: DateTime): Future[Option[HospitalityServicesSentiment]] = {

    val prom = Promise[Option[HospitalityServicesSentiment]]()

    Future {
      prom.success(getData(serviceName, profileId, fromDate, toDate))

    }
    prom.future
  }


  def getData(serviceName: String, profileId: Int, fromDate: DateTime, toDate: DateTime): Option[HospitalityServicesSentiment] = {
    implicit val getHospitalitySentimentResult = GetResult(r => Sentiment(r.<<, r.<<, r.<<))

    val datePattern = "dd-MM-yyyy HH:mm:ss"

    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    var myData = Sentiment

    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Sentiment]( s"""
      select positive_reviews, neutral_reviews,negative_reviews
        from (select
          sum(case when vieras_rating_value > 6 then 1 else 0 end) positive_reviews,
          sum(case when vieras_rating_value <4 then 1 else 0 end) negative_reviews,
          sum(case when vieras_rating_value >= 4 and vieras_rating_value <= 6 then 1 else 0 end) neutral_reviews
            from (select * from ENG_HOTEL_RATING i where i.fk_pid in
              ( select review_id from ENG_HOTEL_REVIEWS where fk_hotel_id in (select fk_hotel_id from ENG_PROFILE_HOTEL_CREDENTIALS where fk_profile_id=${profileId})
              and review_date   BETWEEN TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') AND TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS'))
              and I.VIERAS_RATING_NAME='${serviceName}'
              or (fk_pid in (select fk_hotel_id from ENG_PROFILE_HOTEL_CREDENTIALS where fk_profile_id=${profileId} )
               and I.VIERAS_RATING_NAME='${serviceName}')))
          """)
        if (records.list.size == 0) None
        else {
          val sent: Sentiment = records.first
          Some(HospitalityServicesSentiment(serviceName, sent.positive - sent.negative, sent))
        }
    }
  }
}
