package gr.gnostix.api.models.publicSearch

import java.sql.Timestamp

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

case class Topic(topicId: Int, topicName: String, topicAttributes: String, profileId: Int,
                 creationDate: Timestamp, topicDesc: String)

object TopicDao extends DatabaseAccessSupport {

  implicit val getTopicResult = GetResult(r => Topic(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def findById(topicId: Int, profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Topic]( s"""
                           select  s.sd_id, s.sd_name, s.sd_attr, s.fk_profile_id, s.creation_date, s.sd_description
                                  from search_domains s,profiles c
                                    where s.FK_profile_ID = $profileId
                                      and s.FK_profile_ID = c.profile_id
                                      and c.fk_user_id = $userId
                                      and s.SD_ID = $topicId
          """)
        records.list
    }
  }

  //na ta ftiakso ana customer apo tin basi user.userId


  def getAllTopics(profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Topic]( s"""
          select  s.sd_id, s.sd_name, s.sd_attr, s.fk_profile_id, s.creation_date, s.sd_description
                                              from search_domains s,profiles c
                                                where s.FK_profile_ID = $profileId
                                                  and s.FK_profile_ID = c.profile_id
                                                  and c.fk_user_id = $userId """)
        records.list
    }
  }

  def addTopic(topic: Topic) = {
    getConnection withSession {

      implicit session =>
        try {
          (Q.u + s"""insert into search_domains  (sd_id, sd_name, sd_attr, FK_PROFILE_ID, creation_date, sd_description)
                        values (SEQ_SEARCH_DOMAINS_ID.nextval, '${topic.topicName}', '${topic.topicAttributes}',
                        ${topic.profileId}, sysdate, '${topic.topicDesc}') """).execute()
        } catch {
          case e: Exception => logger.error("---------->  Not able to insert the topic " + e.printStackTrace())
        }
    }
  }

  def updateTopic(topic: Topic, profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s"""update search_domains s
                        set s.SD_NAME = '${topic.topicName}', s.SD_ATTR = '${topic.topicAttributes}',
                        s.SD_DESCRIPTION = '${topic.topicDesc}'
                        where s.SD_ID = ${topic.topicId}
                        and s.FK_PROFILE_ID = $profileId
                        and s.FK_PROFILE_ID in (
                          select c.profile_id from search_domains s, profiles p, users u
                            where  s.FK_PROFILE_ID = $profileId and s.FK_PROFILE_ID = p.PROFILE_ID
                              and p.FK_USER_ID = u.user_id
                              and u.user_id = $userId
                        ) """).execute()
        } catch {
          case e: Exception => logger.error("---------->  Not able to insert the keyword " + e.printStackTrace())
        }
    }
  }


  def deleteTopics(topicIds: List[Int], profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s"""delete from search_domains s
                        where s.SD_ID in (${topicIds.mkString(",")})
                          and s.FK_profile_ID = $profileId
                          and s.fk_profile_id in (
                            select c.profile_id from profiles c
                              where  c.profile_ID = $profileId
                                and c.fk_user_id =$userId """).execute()
        } catch {
          case e: Exception => logger.error("---------->  Not able to insert the keyword " + e.printStackTrace())
        }
    }
  }


}