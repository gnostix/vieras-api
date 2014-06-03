package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{StaticQuery => Q, GetResult}

case class Topic(topicId: Int, topicName: String, topicAttributes: String, profileId: Int,
                 creationDate: Timestamp, topicDesc: String)

 object TopicDao extends DatabaseAccessSupport {

  implicit val getTopicResult = GetResult(r => Topic(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<))

  def findById(topicId: String) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Topic]( s"""select * from search_domains where SD_ID = $topicId""")
        records.first
    }
  }

  def getAllTopics(profileId: String) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Topic]( s"""select * from search_domains where FK_CUSTOMER_ID = $profileId""")
        records.list
    }
  }
}