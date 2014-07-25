package gr.gnostix.api.tmp

import akka.actor.Actor
import org.slf4j.LoggerFactory

/**
 * Created by rebel on 23/7/14.
 */
class GnxActor extends Actor {
  val logger = LoggerFactory.getLogger(getClass)

  def receive = {
    case "sentiment" => {
      logger.info("------------> message arrived")
      sender ! "sentiment done"
    }
/*    case "sentiment done" => {
      logger.info("------------> sentiment done  message arrived")
      //"sentiment done"
    }*/
    case _ => sender ! "I don't get you!!"
  }

}

class GnxActor2 extends Actor {
  val logger = LoggerFactory.getLogger(getClass)

  def receive = {
    case "sentiment" => {
      logger.info("------------> message arrived")
      sender ! "kokokokoko"
    }
    /*    case "sentiment done" => {
          logger.info("------------> sentiment done  message arrived")
          //"sentiment done"
        }*/
    case _ => sender ! "I don't get you!!"
  }

}

