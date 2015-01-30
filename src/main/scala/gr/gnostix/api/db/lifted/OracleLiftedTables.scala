package gr.gnostix.api.db.lifted

import java.sql.Clob

import gr.gnostix.api.db.plainsql.DatabaseAccessOra

/**
 * Created by rebel on 12/5/14.
 */
object OracleLiftedTables {

  import java.sql.Date

import com.typesafe.slick.driver.oracle.OracleDriver.simple._

import scala.slick.lifted.TableQuery


  class DSGroups(tag: Tag) extends Table[(Int, String)](tag, "DS_GROUPS") {
    def groupId = column[Int]("G_ID", O.PrimaryKey)

    def groupName = column[String]("GROUP_NAME")

    // Every table needs a * projection with the same type as the table's type parameter
    def * = (groupId, groupName)
  }

  class BetaUsers(tag: Tag) extends Table[(Int, String, String, String, String, Int, Date, Date)](tag, "BETA_TESTING") {
    def betaTesterId = column[Int]("BETA_TEST_ID")

    def firstName = column[String]("FIRST_NAME")

    def lastName = column[String]("LAST_NAME")

    def companyName = column[String]("COMPANY_NAME")

    def email = column[String]("EMAIL", O.PrimaryKey)

    def newsLetter = column[Int]("NEWSLETTER")

    def signupDate = column[Date]("SIGNUP_DATE")

    def expirationDate = column[Date]("EXPIRATION_DATE")

    def * = (betaTesterId, firstName, lastName, companyName, email, newsLetter, signupDate, expirationDate)

  }

  val db: Database = DatabaseAccessOra.database

  val betaUsers = TableQuery[BetaUsers]

  def getBetaUsers = {
    val query = for (c <- betaUsers) yield c.email
    val result = db.withSession {
      session =>
        query.list()(session)
    }
    result
  }

  class SocialCredentials(tag: Tag) extends Table[(Int, String, String, Int, Int, String, String, String, String,
    String, String, Date, String, Int, Clob, String, String, String, String)](tag, "ENG_CUST_SOCIAL_CREDENTIALS") {

    def id = column[Int]("ID", O.PrimaryKey, O.AutoInc)

    def token = column[String]("TOKEN")

    def tokenSecret = column[String]("TOKENSECRET")

    def datasourceId = column[Int]("FK_DATASOURCE_ID")

    def customerId = column[Int]("FK_CUST_ID")

    def fanPage = column[String]("FB_FAN_PAGE")

    def fbGroup = column[String]("FB_GROUP")

    def fbUser = column[String]("FB_USER")

    def youtubeUser = column[String]("YOUTUBE_USER")

    def twHandler = column[String]("TWITTER_HANDLE")

    def tripadvisor = column[String]("TRIPADVISOR")

    def validated = column[Date]("VALIDATED")

    def venueId = column[String]("FS_VENUE_ID")

    def fbExpires = column[Int]("FACEBOOK_EXPIRES_SEC")

    def ganalytics = column[Clob]("G_ANALYTICS_AUTH_FILE")

    def bookingCom = column[String]("BOOKING_COM")

    def gaAccountName = column[String]("GA_ACCOUNT_NAME")

    def hotels = column[String]("HOTELS")

    def youtubeChannelId = column[String]("YOUTUBE_CHANNELID")


    def * = (id, token, tokenSecret, datasourceId, customerId, fanPage, fbGroup, fbUser, youtubeUser, twHandler, tripadvisor,
      validated, venueId, fbExpires, ganalytics, bookingCom, gaAccountName, hotels, youtubeChannelId)

  }

  val socialCredentials = TableQuery[SocialCredentials]

  def getSocialCredentials = {
    val query = for (c <- socialCredentials) yield c.id
    val result = db.withSession {
      session =>
        query.list()(session)
    }
    result
  }


  /*  val userId =
      (socialCredentials returning socialCredentials.map(_.id)) += ("SEQ_ENG_CUST_SOCIAL_CREDENTIAL.nextval","we","sdsd",23,45,"er","er","err","dfdf","er","er","err","dfdf",23,
        "er","er","err","dfdf","oioioioi")*/
  /*val date = new DateTime()
    socialCredentials += (665555,"we","sdsd",23,45,"er","er","err","dfdf","er","er",date,"dfdf",23
      ,"er","er","err","dfdf","oioioioi")*/

  def addTW = {
    val result = db.withSession {
      implicit session =>
        socialCredentials.map(c => (c.id, c.token, c.tokenSecret, c.twHandler)) +=("SEQ_ENG_CUST_SOCIAL_CREDENTIAL.nextval".toInt,
          "sfsdsdsddssd", "sdghowtgqw8ert348rt8ert78e", "rikoko")
        socialCredentials.insertStatement
    }
    result.toList
  }

  class ApiKeys(tag: Tag) extends Table[(String, Int, Int, Int)](tag, "API_KEYS") {
    def apiKey = column[String]("API_KEY")

    //def creationDate = column[String]("CREATION_DATE")

    def isActive = column[Int]("IS_ACTIVE")

    def customerId = column[Int]("FK_CUST_ID")

    def id = column[Int]("ID", O.AutoInc)

    def * = (apiKey, isActive, customerId, id)
  }

  val apiKeys = TableQuery[ApiKeys]
  val mySequence = Sequence[Int]("SEQ_ENG_CUST_SOCIAL_CREDENTIAL") start 405 inc 1

  def insertApi = {
    val result = db.withSession {
      implicit session =>
        apiKeys += ("Alexxxxxxxqwqwqwqwqwqw3qwx2xx1212", 1, 4352145, 2323)
        //apiKeys.map(c => (c.customerId, c.apiKey, c.id)) += (101, "iuyuyytytrtty", 4343)
      }
    result
   }

  def getApiKeys = {
    val query = for (c <- apiKeys) yield c
    val result = db.withSession {
      implicit session =>
        query.list()
    }
    result
  }

}

