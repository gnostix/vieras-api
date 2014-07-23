package gr.gnostix.api.utilities

import gr.gnostix.api.models.{SocialData, DatafindingsSentimentLineDao, SentimentLine}
import org.joda.time.DateTime
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
/**
 * Created by rebel on 23/7/14.
 */
object AsyncUtilsGnx {


/*  def getSentiment(fromDate: DateTime, toDate: DateTime, profileId: Int, datasource: String) (implicit ctx: ExecutionContext): Future[SocialData] = {
    val prom = Promise[SocialData]()

    DatafindingsSentimentLineDao.getDataDefault(fromDate, toDate, profileId, datasource) onComplete {
      case Success(myData) => prom.complete(myData)
      case Failure(exception) => println(exception)
    }
    prom.future

  }*/

}
