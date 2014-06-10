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
        000
    }
  }
}
