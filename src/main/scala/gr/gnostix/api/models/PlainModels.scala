package gr.gnostix.api.models

import java.sql.Timestamp

abstract class Payload
abstract class DataGraph

case class DataLineGraph(msgNum: Int, msgDate: Timestamp) extends DataGraph
case class SocialData(datasource: String, data: List[DataGraph]) extends Payload

case class DataResponse(status: Int, message: String, payload: SocialData)
case class AllDataResponse(status: Int, message: String, payload: List[Payload])

case class FirstLevelData(msgId: Int, text: String, fromUser: String, msgDate: Timestamp, queryId: Int, sentiment: String,
                          msgUrl: String) extends DataGraph

//get the second level data for each row
case class SecondLevelDataTwitter(userProfileImageUrl: String, fromUserId: Long, toUser: String, tweetId: Long,
                                   followers: Int, following: Int, tweetsNum: Int, userUrl: String, listed: Int) extends DataGraph

case class SecondLevelDataFacebook(fromId: Long, comments: Int, shares: Int, likes: String, iconLink: String, link: String,
                                   picture: String) extends DataGraph

case class SecondLevelDataGplus(itemId: String, plusoners: Int, resharers: Int, replies: Int, actorId: String,
                                actorUrl: String, actorImage: String, attachedUrl: String) extends DataGraph

case class SecondLevelDataYoutube(videoId: String, favoritesCount: Int, viewCount: Int, dislikeCount: Int, likeCount: Int,
                                  channelId: String) extends DataGraph

case class SecondLevelDataFeed(message: String) extends DataGraph
case class SecondLevelDataWeb(url: String, description: String) extends DataGraph


// classes for the messages of each datasource
case class DataTwitterGraph(twId: Int, msgDate: Timestamp, twitterHandle: String, userId: Int, queryId: Int, tweetId: Long,
                            followers: Int, following: Int, listed: Int, text: String, userProfileImage: String,
                            sentiment: String) extends DataGraph

case class DataFacebookGraph(twId: Int, msgDate: Timestamp)extends DataGraph // to be continued
