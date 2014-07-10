package gr.gnostix.api.models

import java.sql.Timestamp

abstract class Payload
abstract class DataGraph

case class DataLineGraph(msgNum: Int, msgDate: Timestamp) extends DataGraph
case class SocialData(datasource: String, data: List[DataGraph]) extends Payload

case class DataResponse(status: Int, message: String, payload: SocialData)
case class AllDataResponse(status: Int, message: String, payload: List[Payload])

case class FirstLevelData(msgId: String, text: String, fromUser: String, msgDate: Timestamp, queryId: Int, sentiment: String, msgUrl: String) extends DataGraph

// classes for the messages of each datasource
case class DataTwitterGraph(twId: Int, msgDate: Timestamp, twitterHandle: String, userId: Int, queryId: Int, tweetId: Long,
                            followers: Int, following: Int, listed: Int, text: String, userProfileImage: String, sentiment: String) extends DataGraph

case class DataFacebookGraph(twId: Int, msgDate: Timestamp)extends DataGraph // to be continued
