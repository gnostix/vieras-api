package gr.gnostix.api.models

import java.sql.Timestamp

abstract class Payload
case class DataLineGraph(mesgNumb: Int, mesgDate: Timestamp)
case class SocialData(datasource: String, data: List[DataLineGraph]) extends Payload

case class DataResponse(status: Int, message: String, payload: SocialData)
case class AllDataResponse(status: Int, message: String, payload: List[Payload])
