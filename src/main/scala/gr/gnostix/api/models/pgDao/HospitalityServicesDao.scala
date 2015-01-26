package gr.gnostix.api.models.pgDao


import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.models.plainModels.{RevStat, HotelRatingStats}
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

  private def getTopMinusMaxReviews(li: List[HotelRatingStats]): (List[RevStat], List[RevStat]) = {
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

    val neg = massagedData.take(5).toList
    val pos = massagedData.reverse.take(5).toList


    (neg, pos)
  }

}
