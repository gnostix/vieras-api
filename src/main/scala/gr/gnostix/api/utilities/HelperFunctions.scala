package gr.gnostix.api.utilities

import gr.gnostix.api.models.{ErrorDataResponse, ApiMessages, ApiData}

/**
 * Created by rebel on 13/1/15.
 */
object HelperFunctions {

  def f2(dashboardData: Option[ApiData]) = {
    dashboardData match {
      case Some(dt) => {

        val hasData = dt.dataName match {
          case "nodata" => ApiMessages.generalSuccessNoData
          case _ => ApiMessages.generalSuccessOneParam( Map(dt.dataName -> dt.data))
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }

  def f3(dashboardData: Option[List[ApiData]]) = {

    dashboardData match {
      case Some(dt) => {

        val existData = dt.filter(_.dataName != "nodata")

        val myData = existData.map{
          case (x) => (x.dataName -> x.data)
        }.toMap

        val hasData = existData.size match {
          case x if( x > 0 ) => ApiMessages.generalSuccessOneParam(myData)
          case x if( x == 0) => ApiMessages.generalSuccessNoData
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }
}
