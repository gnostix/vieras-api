package gr.gnostix.api.models.pgDao


import java.sql.{CallableStatement, Timestamp}

import gr.gnostix.api.db.plainsql.{DatabaseAccessSupportPg}
import gr.gnostix.api.models.plainModels.{UserAccount, ApiMessages, Payload}
import gr.gnostix.api.utilities.{EmailUtils, HelperFunctions}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


case class User(userId: Int, username: String, var password: String,
                userLevel: Int,
                userDetails: UserDetails
                //                userTotals: UserTotals
               ) extends Payload

case class UserDetails(firstName: String, lastName: String,
                       registrationDate: Timestamp, email: String, streetAddress: String,
                       streetNum: String, postalCode: String, city: String,
                       companyName: String,
                       language: String,
                       expirationDate: Timestamp)

/*
case class UserTotals(totalCounts: Int, totalKeywords: Int,
                      enabled: Int, totalProfiles: Int,
                      totalTopicProfiles: Int, totalSocialAccounts: Int,
                      totalHotelAccounts: Int)
*/

case class UserRegistration(username: String, password: String, name: String, lastname: String, email: String, token: String)

object UserDao extends DatabaseAccessSupportPg {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val getUserResult = GetResult(r => User(r.<<, r.<<, r.<<, r.<<,
    UserDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))

  def findById(userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User]( s"""
          select id, username, password, userlevel, user_firstname, user_lastname, registration_date,
            email, street_address, street_no, postal_code, city, company, language, expiration_date
          from vieras.users where id =  $userId
          """)
        records.first
    }
  }

  def findByUsername(username: String): Option[User] = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[User]( s"""
        select id, username, password, userlevel, user_firstname, user_lastname, registration_date,
            email, street_address, street_no, postal_code, city, company, language, expiration_date
          from vieras.users where username = '$username'
          """)
          // and expiration_date >= now()::timestamp(0)

          records.firstOption()
        } catch {
          case e: Exception => e.printStackTrace()
            None
        }

    }
  }

  def findByTokenKey(hostname: String, token: String): Option[User] = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[User]( s"""
        select u.id, u.username, u.password, u.userlevel, u.user_firstname, u.user_lastname, u.registration_date,
             u.email, u.street_address, u.street_no, u.postal_code, u.city, u.company, u.language, u.expiration_date
          from vieras.users u, vieras.api_allowed_hosts a
           where u.token = '$token'
           and u.id = a.fk_user_id
           and a.host = '$hostname'
          """)
          // and expiration_date >= now()::timestamp(0)

          records.firstOption()
        } catch {
          case e: Exception => e.printStackTrace()
            None
        }

    }
  }


  def getUsers = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Int]( """select count(*) from vieras.users""")
        records.first
    }
  }

  def resetPassword(user: User): Option[Int] = {

    try {
      // generate new password
      val newPassword = HelperFunctions.randomAlphaNumericString(10)
      val hashedPassword = HelperFunctions.sha1Hash(user.username + newPassword)

      //update password in database for this username/email
      getConnection withSession {
        implicit session =>
          Q.updateNA(
            s""" update vieras.users set password = '$hashedPassword'
             where  username = '${user.username}'
               and id = ${user.userId}""").execute()
      }

      val message = s""" You temporary password is ${newPassword}. \r\nPlease change it in your next Login"""
      val subject = "Vieras support"
      //send email to the user with the new password
      EmailUtils.sendMailOneRecipient(user.userDetails.email, message, subject)


      Some(200)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }


  def createUser(userReg: UserRegistration): Option[Int] = {
    try {

      val sql = "{call vieras.create_user(?, ?, ?, ?, ?, ?, ?)}"

      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setNull(1, java.sql.Types.INTEGER);
      callableStatement.setString(2, userReg.email);
      callableStatement.setString(3, userReg.name);
      callableStatement.setString(4, userReg.lastname);
      callableStatement.setString(5, userReg.password);
      callableStatement.setInt(6, 2);
      callableStatement.setString(7, userReg.username);

      callableStatement.executeUpdate()

      callableStatement.close()
      //connection.commit()
      connection.close()

      val status: Int = 200
      Some(status)

    } catch {
      case e: Exception => {
        logger.error("---------->  error on account creation " + e.printStackTrace())
        val status: Int = 400
        None
      }
    }
  }

  def updateUserAccount(account: UserAccount, userId: Int): Option[Int] = {
    try {

      if (account.password != null) {
        // generate new password
        val hashedPassword = HelperFunctions.sha1Hash(account.email + account.password)
        logger.info("---------->  account update with password " + account.password)
        //update password in database for this username/email
        getConnection withSession {
          implicit session =>
            Q.updateNA(
              s""" update vieras.users set password = '$hashedPassword',  user_firstname = '${account.firstName}',
                       user_lastname = '${account.lastName}',  company = '${account.companyName}',
                       street_address = '${account.streetAddress}'
             where  username = '${account.email}'
               and id = ${userId}""").execute()
        }

      } else {
        logger.info("---------->  account update with password")
        getConnection withSession {
          implicit session =>
            Q.updateNA(
              s""" update vieras.users set user_firstname = '${account.firstName}',
                       user_lastname = '${account.lastName}',  company = '${account.companyName}',
                      street_address = '${account.streetAddress}'
             where  username = '${account.email}'
               and id = ${userId}""").execute()
        }

      }

      Some(200)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }
}

