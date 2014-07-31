package gr.gnostix.api.models

import gr.gnostix.api.utilities.SqlUtils
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.slick.jdbc.GetResult


object SocialChannelsTwitterDAO {
  implicit val getDtTwitterLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

/*
  def getLineDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, mySqlDynamic)
  }
*/



}
