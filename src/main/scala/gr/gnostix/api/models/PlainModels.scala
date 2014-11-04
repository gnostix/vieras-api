package gr.gnostix.api.models

import java.sql.Timestamp
import java.util.Date

import org.joda.time.DateTime

abstract class Payload
abstract class DataGraph


case class DataLineGraph(msgNum: Int, msgDate: Timestamp) extends DataGraph
case class SocialData(datasource: String, data: List[DataGraph]) extends Payload
case class SocialDataSum(datasource: String, data: Int) extends Payload
case class SocialAccounts(datasource: String, data: List[DataGraph]) extends Payload
case class MsgNum(msgNum: Int) extends Payload

case class SimpleResponse(status: Int, message: String)
case class DataResponse(status: Int, message: String, payload: Payload)
case class DataResponseAccounts(status: Int, message: String, payload: List[Payload])
case class ErrorDataResponse(status: Int, message: String) extends Payload


case class AllDataResponse(status: Int, message: String, payload: List[Payload])


// first level data model - one model for all the datasources
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


// Third level data models
case class ThirdLevelDataTwitter(firstLevel: FirstLevelData, secondLevel: SecondLevelDataTwitter) extends DataGraph
case class ThirdLevelDataFacebook(firstLevel: FirstLevelData, secondLevel: SecondLevelDataFacebook) extends DataGraph
case class ThirdLevelDataGplus(firstLevel: FirstLevelData, secondLevel: SecondLevelDataGplus) extends DataGraph
case class ThirdLevelDataYoutube(firstLevel: FirstLevelData, secondLevel: SecondLevelDataYoutube) extends DataGraph
case class ThirdLevelDataFeed(firstLevel: FirstLevelData, secondLevel: SecondLevelDataFeed) extends DataGraph
case class ThirdLevelDataWeb(firstLevel: FirstLevelData, secondLevel: SecondLevelDataWeb) extends DataGraph


// classes for the messages of each datasource
case class DataTwitterGraph(twId: Int, msgDate: Timestamp, twitterHandle: String, userId: Int, queryId: Int, tweetId: Long,
                            followers: Int, following: Int, listed: Int, text: String, userProfileImage: String,
                            sentiment: String) extends DataGraph

case class DataFacebookGraph(fbId: Int, msgDate: Timestamp)extends DataGraph // to be continued



// Sentiment counts for each datasource
case class SentimentLine( sentiment: String, msgNum: Int) extends DataGraph

object SocialDatasources {
  val twitter: String = "twitter"
  val facebook: String = "facebook"
  val gplus: String = "gplus"
  val youtube: String = "youtube"
  val web: String = "web"
  val linkedin: String = "linkedin"
  val news: String = "news"
  val blog: String = "blog"
  val personal: String = "personal"

  val myDatasources = List("twitter", "facebook", "gplus", "youtube", "web", "linkedin", "news", "blog", "personal")
}

//facebook fan page
case class FacebookPage(pageName: String, pageId: String)
case class FacebookPageAuth(token: String, expires: Date, fanpages: List[FacebookPage]) extends Payload
case class FacebookToken(token: String)

// hotels
case class HotelAddUrl(dsId: Int, hotelUrl: String)