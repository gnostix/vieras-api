package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.slf4j.LoggerFactory
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import java.sql.Timestamp


case class Keyword(keywordId: Int,
                   keyInclude: String,
                   keyExclude: String,
                   topicId: Int,
                   creationDate: Timestamp,
                   langId: Int,
                   updatedDate: Timestamp)

object KeywordDao extends DatabaseAccessSupport {

  implicit val getKeywordResult = GetResult(r => Keyword(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  val logger = LoggerFactory.getLogger(getClass)

  def findById(keywordId: Int, topicId: Int, profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Keyword]( s"""select k.K_ID, k.KEY_INCLUDE, k.KEY_EXCLUDE, k.FK_SD_ID, k.CREATION_DATE, k.FK_LANG_ID, k.UPDATED_TIME
                                                 from keywords k
                                                 where k.k_id = ${keywordId} and k.fk_sd_id = ${topicId} and k.fk_sd_id in
                                                     (select sd_id from search_domains s,customers c, user_customer_map m
                                                      where  s.fk_customer_id = ${profileId} and s.fk_customer_id = s.FK_CUSTOMER_ID
                                                        and c.customer_id = m.customer_id and m.user_id = ${userId} )""")
        records.list
    }
  }

  def getAllKeywords(topicId: Int, profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Keyword]( s"""select k.K_ID, k.KEY_INCLUDE, k.KEY_EXCLUDE, k.FK_SD_ID, k.CREATION_DATE, k.FK_LANG_ID, k.UPDATED_TIME
                                                from keywords k
                                                 where k.fk_sd_id = ${topicId} and k.fk_sd_id in
                                                     (select sd_id from search_domains s,customers c, user_customer_map m
                                                      where  s.fk_customer_id = ${profileId} and s.fk_customer_id = s.FK_CUSTOMER_ID
                                                        and c.customer_id = m.customer_id and m.user_id = ${userId} ) """)
        records.list
    }
  }

   def addKeyword(keyword: Keyword) {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s"""insert into keywords (K_ID, KEY_INCLUDE, KEY_EXCLUDE, FK_SD_ID, CREATION_DATE, FK_LANG_ID, UPDATED_TIME)
          values (SEQ_KEYWORDS_ID.nextval, '${keyword.keyInclude}', '${keyword.keyExclude}', ${keyword.topicId} , sysdate,
                    ${keyword.langId}, sysdate)""").execute()
        } catch {
          case e: Exception => logger.error("---------->  Not able to insert the keyword " + e.printStackTrace())
        }
    }
  }

  def updateKeyword(keyword: Keyword) {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s"""update keywords set KEY_INCLUDE = '${keyword.keyInclude}', KEY_EXCLUDE = '${keyword.keyExclude}',
                          FK_LANG_ID = ${keyword.langId}, UPDATED_TIME = sysdate
                        where k_id = ${keyword.keywordId}""").execute()
        } catch {
          case e: Exception => logger.error("---------->  Not able to insert the keyword " + e.printStackTrace())
        }
    }
  }

  def deleteKeyword(keywordIds: List[Int]) {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s"""delete from  keywords
                        where k_id in (${keywordIds.mkString(",")}) """).execute()
          //delete also the queries for these keywords
          //deleteQueries(keywordIds)
        } catch {
          case e: Exception => logger.error("---------->  Not able to insert the keyword " + e.printStackTrace())
        }
    }
  }

  // this currently is doen with a trigger in the database
  private def deleteQueries(keywordIds: List[Int]) {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s"""delete from  queries
                        where fk_k_id in (${keywordIds.mkString(",")}) """).execute()
        } catch {
          case e: Exception => logger.error("---------->  Not able to insert the keyword " + e.printStackTrace())
        }
    }
  }

}
