package gr.gnostix.api.models

import java.sql.Timestamp

case class DataLineGraph(mesgNumb: Int, mesgDate: Timestamp)
case class SocialData(datasource: String, data: List[DataLineGraph])

case class DataResponse(status: String, message: String, payload: SocialData)
case class AllDataResponse(status: String, message: String, payload: List[SocialData])
