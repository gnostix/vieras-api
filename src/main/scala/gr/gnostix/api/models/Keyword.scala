package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import java.sql.Timestamp


case class Keyword(keywordId: Int,
                   keyInclude: String,
                   keyExclude: String,
                   topicId: Int,
                   isActive: Int,
                   creationDate: Timestamp,
                   langId: Int,
                   updatedDate: Timestamp)

object KeywordDao extends DatabaseAccessSupport {

  implicit val getKeywordResult = GetResult(r => Keyword(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<))

  def findById(keywordId: String) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Keyword](s"""select * from keywords where K_ID = $keywordId""")
        records.first
    }
  }

  def getAllKeywords(topicId: String) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Keyword](s"""select * from keywords k where k.fk_sd_id = $topicId""")
        records.list
    }
  }

}
