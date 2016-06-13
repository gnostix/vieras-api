package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{StaticQuery => Q, GetResult}

/**
 * Created by rebel on 6/3/15.
 */
object AppVersionDao extends DatabaseAccessSupportPg {

  val logger = LoggerFactory.getLogger(getClass)

  def getWebAppVersion: String = {
    try {
      getConnection withSession {
        implicit session =>
          val records = Q.queryNA[String]( s"""
          select web_version from vieras.app_versions order by created desc limit 1
          """)
          records.first
      }
    } catch {
      case e: Exception => e.printStackTrace()
        "api version error"
    }

  }

  val webVersionHeader = "Web-Version"

}
