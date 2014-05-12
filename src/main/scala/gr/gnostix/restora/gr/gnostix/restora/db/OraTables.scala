package gr.gnostix.restora.gr.gnostix.restora.db

import scala.slick.lifted.{TableQuery, Tag}
import com.typesafe.slick.driver.oracle.OracleDriver.simple._
import java.sql.Date

/**
 * Created by rebel on 12/5/14.
 */
object OraTables {


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

  val betaUsers = TableQuery[BetaUsers]


}