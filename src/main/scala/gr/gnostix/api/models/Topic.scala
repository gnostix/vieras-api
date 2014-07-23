package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.slf4j.LoggerFactory
import scala.slick.jdbc.{StaticQuery => Q, GetResult}

case class Topic(topicId: Int, topicName: String, topicAttributes: String, profileId: Int,
                 creationDate: Timestamp, topicDesc: String)

 object TopicDao extends DatabaseAccessSupport {

  implicit val getTopicResult = GetResult(r => Topic(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<))

   val logger = LoggerFactory.getLogger(getClass)

  def findById(topicId: Int, profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Topic]( s"""select  s.sd_id, s.sd_name, s.sd_attr, s.fk_customer_id, s.creation_date, s.sd_description
                                              from search_domains s, customers c, user_customer_map m
                                                where FK_CUSTOMER_ID = $profileId and c.customer_id = m.customer_id
                                                  and m.user_id = $userId and s.SD_ID = $topicId""")
        records.list
    }
  }

   //na ta ftiakso ana customer apo tin basi user.userId


  def getAllTopics(profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Topic]( s"""select  s.sd_id, s.sd_name, s.sd_attr, s.fk_customer_id, s.creation_date, s.sd_description
                                              from search_domains s, customers c, user_customer_map m
                                                where s.FK_CUSTOMER_ID = $profileId
                                                  and s.FK_CUSTOMER_ID = c.customer_id
                                                  and c.customer_id = m.customer_id
                                                  and m.user_id = $userId""")
        records.list
    }
  }

   def addTopic(topic: Topic) = {
     getConnection withSession {

       implicit session =>
         try {
           (Q.u + s"""insert into search_domains  (sd_id, sd_name, sd_attr, fk_customer_id, creation_date, sd_description)
                        values (SEQ_SEARCH_DOMAINS_ID.nextval, '${topic.topicName}', '${topic.topicAttributes}', ${topic.profileId},
                        sysdate, '${topic.topicDesc}') """).execute()
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
                        set s.SD_NAME = '${topic.topicName}', s.SD_ATTR = '${topic.topicAttributes}', s.SD_DESCRIPTION = '${topic.topicDesc}'
                        where s.SD_ID = ${topic.topicId}
                        and s.fk_customer_id = $profileId
                        and s.fk_customer_id in (
                          select c.customer_id from search_domains s, customers c, user_customer_map m
                            where  s.fk_customer_id = $profileId and s.fk_customer_id = c.CUSTOMER_ID
                              and c.customer_id = m.customer_id and m.user_id = $userId
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
                          and s.FK_CUSTOMER_ID = $profileId
                          and s.fk_customer_id in (
                            select c.customer_id from customers c, user_customer_map m
                              where  c.CUSTOMER_ID = $profileId
                                and c.customer_id = m.customer_id and m.user_id = $userId
                          ) """).execute()
         } catch {
           case e: Exception => logger.error("---------->  Not able to insert the keyword " + e.printStackTrace())
         }
     }
   }


}