package gr.gnostix.api.utilities

import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory


object DateUtils {
  val logger = LoggerFactory.getLogger(getClass)

  def findNumberOfDays(fromDate: DateTime, toDate: DateTime): Int = {
    try {
      val days = Days.daysBetween(fromDate, toDate).getDays
      logger.info("-----------------------> number of days between the two dates  " + fromDate + " " + toDate)
      logger.info("-----------------------> number of days between the two dates  " + days)
      days
    } catch {
      case e: Exception => println("-------------- exception in findNumberOfDays")
      0
    }
  }

  def sqlGrouByDate(numDays: Int): String = {
    numDays match {
      case 0 => "HH"
      case x if 0 until 31 contains x => "DD"
      case x if 31 until 91 contains x => "ww"
      case x if x >= 91 => "month"
      case _ => "month"
    }
  }
}
