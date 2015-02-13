package gr.gnostix.api.models.plainModels

import java.sql.Timestamp
import java.util.Date


abstract class Payload
abstract class DataGraph


case class DataLineGraph(msgNum: Int, msgDate: Timestamp) extends DataGraph
case class SocialData(datasource: String, data: List[DataGraph]) extends Payload
object ApiData {

  def cleanDataResponse(dashboardData: Option[ApiData]) = {
    dashboardData match {
      case Some(dt) => {

        val hasData = dt.dataName match {
          case "nodata" => ApiMessages.generalSuccessNoData
          case _ => ApiMessages.generalSuccessOneParam(Map(dt.dataName -> dt.data))
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }
}
case class ApiData(dataName: String, data: Any) extends Payload
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
case class SecondLevelDataTwitter(userProfileImageUrl: String, fromUserId: String, toUser: String, tweetId: String,
                                   followers: Int, following: Int, tweetsNum: Int, userUrl: String, listed: Int) extends DataGraph

case class SecondLevelDataFacebook(fromId: String, comments: Int, shares: Int, likes: String, iconLink: String, link: String,
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
case class DataTwitterGraph(twId: Int, msgDate: Timestamp, twitterHandle: String, userId: Int, queryId: Int, tweetId: String,
                            followers: Int, following: Int, listed: Int, text: String, userProfileImage: String,
                            sentiment: String) extends DataGraph

case class DataFacebookGraph(fbId: Int, msgDate: Timestamp)extends DataGraph // to be continued
case class DemographicsDataFB(female: Int, male: Int, age: List[Int], rawData: List[FacebookDemographics])



// Sentiment counts for each datasource
case class SentimentLine( sentiment: String, msgNum: Int) extends DataGraph
case class CountriesLine( country: String, msgNum: Int) extends DataGraph

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
case class FacebookDemographics(queryId: Int, age17: Int, age24: Int, age34: Int, age44: Int, age54: Int, age64: Int, age65Plus: Int, gender: String, created: Timestamp)
case class FacebookStats(queryId: Int, created: Timestamp, pageLikes: Int, post: Int, postLikes: Int, postShares: Int, comments: Int, commentLikes: Int, talkingAbout: Int, reach: Int, views: Int, engaged: Int)
case class FacebookStatsTop(reach: Int, views: Int, engaged: Int, talkingAbout: Int, newLikes: Int, shares:Int)
case class FacebookStatsApi(facebookStatsTotals: FacebookStatsTop, facebookStatsData: List[FacebookStats])
case class Female(age17: Int, age24: Int, age34: Int, age44: Int, age54: Int, age64: Int, age65Plus: Int)
case class Male(age17: Int, age24: Int, age34: Int, age44: Int, age54: Int, age64: Int, age65Plus: Int)
case class FacebookComment(id: Int, message: String, created: Timestamp  , userName: String, userId: String, likes: Int, postId: String, engQueryId: Int, commentId: String, postUserId: String)
case class FacebookPost(id: Int, message: String, created: Timestamp  , userName: String, userId: String, likes: Int, comments: Int, engQueryId: Int, postId: String, postLink: String, shares: Int)

// twitter page
case class TwitterMentionFav(id: Int, created: Timestamp, actionUserHandler: String, actionUserId: String, actionUserFollowers: Int, actionUserListed: Int, text: String, queryId: Int, favorites: Int, statusId: String)
case class TwitterRetweets(id: Int, created: Timestamp, retweetStatusId: String, retweetedCount: Int, text: String, queryId: Int,handle: String)
case class TwitterStats(totalTweets: Int, totalFollowers: Int, totalFollowing: Int, totalFavorites: Int, totalListed: Int, handle: String, created: Timestamp)

// Youtube page
case class YoutubeStats(subscribers: Int, totalViews: Int, created: Timestamp, likes: Int, dislikes: Int, favorites: Int, videoViews: Int)
case class YoutubeVideoStats(likes: Int, dislikes: Int, favorites: Int, videoViews: Int)
case class YoutubeVideoData(title: String, url: String, thumbnail: String, favorites: Int, views: Int, dislikes: Int, likes: Int, text: String)
case class YoutubeLineData(subscribers: Int, totalViews: Int, videoViews: Int, likes: Int, dislikes: Int, favorites: Int, created: Timestamp)

// hotels
case class HotelAddUrl(dsId: Int, hotelUrl: String)
case class HotelReviewStats(reviewId: Int, reviewer: String,  stayType: String, country: String, vierasReviewRating: Double, datasourceHotelRating: Double, maxHotelScore: Int)
case class RevStat(service_name: String, score: Int, numMsg: Int)
case class HotelRatingStats(ratingName: String, ratingValue: Int)
case class HotelServicesLine(ratingName: String, ratingValue: Double, created: Timestamp)
case class HotelTextData(text: String, rating: Double, dsId: Int, daName: String, created: Timestamp)

case class UserAccount(company: String, firstName: String, lastName: String, email:String, address: String, password: String)