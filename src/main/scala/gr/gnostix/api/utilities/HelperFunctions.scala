package gr.gnostix.api.utilities

import gr.gnostix.api.models.plainModels.{ApiData, ApiMessages, ErrorDataResponse}

import scala.util.Random

/**
 * Created by rebel on 13/1/15.
 */
object HelperFunctions {

  def f2(dashboardData: Option[ApiData]) = {
    dashboardData match {
      case Some(dt) => ApiMessages.generalSuccessOneParam(Map(dt.dataName -> dt.data))
      case None => ErrorDataResponse(404, "Error on data")
    }

  }

  def f3(dashboardData: Option[List[ApiData]]) = {

    dashboardData match {
      case Some(dt) => {

        //val existData = dt.filter(_.dataName != "nodata")

        val myData = dt.map {
          case (x) => {
            //println(x)
            (x.dataName -> x.data)
          }
        }.toMap

        val hasData = dt.size match {
          case x if (x > 0) => ApiMessages.generalSuccessOneParam(myData)
          case x if (x == 0) => ApiMessages.generalSuccessNoData
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }

  def fixSumData(dashboardData: Option[List[ApiData]]) = {

    dashboardData match {
      case Some(dt) => {

        //val existData = dt.filter(_.dataName != "nodata")

        val myData = dt.map {
          case (x) => {
            //println(x)
            (x.dataName -> x.data)
          }
        }.toMap

        val hasData = dt.size match {
          case x if (x > 0) => ApiMessages.generalSuccessOneParam("messages" -> myData)
          case x if (x == 0) => ApiMessages.generalSuccessNoData
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }


  def f4(dashboardData: Option[List[ApiData]]) = {

    dashboardData match {
      case Some(dt) => ApiMessages.generalSuccessOneParam(dashboardData)
      case None => ErrorDataResponse(404, "Error on data")
    }

  }


  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }

  private def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = new Random().nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

  // create a md5 hash from a string
  def sha1Hash(text: String): String = java.security.MessageDigest.getInstance("SHA").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }


  def doublePrecision1(num: Double): Double = {
    BigDecimal(num).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
