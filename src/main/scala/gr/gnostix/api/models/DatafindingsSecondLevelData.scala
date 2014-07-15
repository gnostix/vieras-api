package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory
import scala.slick.jdbc.{StaticQuery => Q, GetResult}

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
}

object DatafindingsSecondLevelDataDAO extends DatabaseAccessSupport {

  implicit val getSecondLevelDataTwitterResult = GetResult(r => SecondLevelDataTwitter(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getSecondLevelDataFacebookResult = GetResult(r => SecondLevelDataFacebook(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getSecondLevelDataGplusResult = GetResult(r => SecondLevelDataGplus(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getSecondLevelDataYoutubeResult = GetResult(r => SecondLevelDataYoutube(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getSecondLevelDataWebResult = GetResult(r => SecondLevelDataWeb(r.<<, r.<<))
  implicit val getSecondLevelDataFeed = GetResult(r => SecondLevelDataFeed(r.<<))


  val logger = LoggerFactory.getLogger(getClass)


  def getSecondLevelDataTwitterDefault(msgId: Int, datasource: String) = {
    //bring the actual data
    getSecondLevelData(msgId, datasource)
  }


  def getSecondLevelData(msgId: Int, datasource: String) = {

    val sqlQ = buildQuery(msgId, datasource)
    var myData = datasource match {
      case "twitter" => List[SecondLevelDataTwitter]()
      case "facebook" => List[SecondLevelDataFacebook]()
      case "youtube" => List[SecondLevelDataYoutube]()
      case "web" => List[SecondLevelDataWeb]()
      case "linkedin" => List[SecondLevelDataWeb]()
      case "blog" => List[SecondLevelDataFeed]()
      case "news" => List[SecondLevelDataFeed]()
      case "personal" => List[SecondLevelDataFeed]()
     }

    getConnection withSession {
      implicit session =>
        logger.info("getSecondLevelData tw ------------->" + sqlQ)
        val records = datasource match {
          case "twitter" => Q.queryNA[SecondLevelDataTwitter](sqlQ)
          case "facebook" => Q.queryNA[SecondLevelDataFacebook](sqlQ)
          case "gplus" => Q.queryNA[SecondLevelDataGplus](sqlQ)
          case "youtube" => Q.queryNA[SecondLevelDataYoutube](sqlQ)
          case "web" => Q.queryNA[SecondLevelDataWeb](sqlQ)
          case "linkedin" => Q.queryNA[SecondLevelDataWeb](sqlQ)
          case "blog" => Q.queryNA[SecondLevelDataFeed](sqlQ)
          case "news" => Q.queryNA[SecondLevelDataFeed](sqlQ)
          case "personal" => Q.queryNA[SecondLevelDataFeed](sqlQ)

        }
        myData = records.list()
    }


    val lineData = datasource match {
      case "twitter" => SocialData(SocialDatasources.twitter, myData)
      case "facebook" => SocialData(SocialDatasources.facebook, myData)
      case "gplus" => SocialData(SocialDatasources.gplus, myData)
      case "youtube" => SocialData(SocialDatasources.youtube, myData)
      case "web" => SocialData(SocialDatasources.web, myData)
      case "linkedin" => SocialData(SocialDatasources.linkedin, myData)
      case "blog" => SocialData(SocialDatasources.blog, myData)
      case "news" => SocialData(SocialDatasources.news, myData)
      case "personal" => SocialData(SocialDatasources.personal, myData)
    }

    lineData
  }


  def buildQuery(msgId: Int, datasource: String): String = {

    val sql = datasource match {
      case "twitter" => getSqlTW(msgId)
      case "facebook" => getSqlFB(msgId)
      case "gplus" => getSqlGplus(msgId)
      case "youtube" => getSqlYT(msgId)
      case "web" => getSqlWebByType(msgId)
      case "linkedin" => getSqlWebByType(msgId)
      case "news" => getSqlFeedByType(msgId)
      case "blog" => getSqlFeedByType(msgId)
      case "personal" => getSqlFeedByType(msgId)
      case _ => {
        logger.info("---------> For the First Level Data we don't match a datasource.. !")
        s"no sql code for this datasource ${datasource}"
      }
    }
    sql
  }

  def getSqlTW(msgId: Int): String = {
    val sql = s"""select T_PROFILE_IMAGE_URL, T_FROM_USER_ID, T_TO_USER, T_TWEET_ID, FOLLOWERS,
                          FOLLOWING, TWEETS_NUM, USER_URL, LISTED
                    from twitter_results t
                    where t.tw_id = ${msgId}"""
    sql
    //logger.info("------------>" + sql)
  }

  def getSqlFB(msgId: Int): String = {
    val sql = s"""select f_from_id, f_comments, shares, f_likes, f_icon_link, f_link, f_picture
                    from facebook_results where fb_id = ${msgId}"""
    sql

  }

  def getSqlGplus(msgId: Int): String = {
    val sql = s"""select ITEMID, PLUSONERS, RESHARERS, REPLIES, ACTORID, ACTORURL, ACTORIMAGE, ATTACHURL
                    from googleplus_results where gplus_id = ${msgId}"""
    sql

  }

  def getSqlYT(msgId: Int): String = {
    val sql = s"""select y_video_id, y_favorite_count, y_view_count, y_dislike_count, y_like_count, channel_id
                    from youtube_results where you_id = ${msgId}"""
    sql

  }

  def getSqlWebByType(msgId: Int): String = {
    val sql = s"""select url, description from web_results where web_id = ${msgId}"""
    sql

  }


  def getSqlFeedByType(msgId: Int): String = {
    val sql = s"""select SUBSTR(message,1,1000) || '..'
                    from feed_results
                    where feed_id = ${msgId}"""
    sql

  }


}
