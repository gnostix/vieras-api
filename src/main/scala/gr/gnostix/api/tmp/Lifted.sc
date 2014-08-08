import com.typesafe.slick.driver.oracle.OracleDriver
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import oracle.sql.TIMESTAMP

import scala.slick.lifted.Tag
import scala.slick.model.Table

/*object SATWLiftedextends extends DatabaseAccessSupport {
  class SocialCredentials(tag: Tag) extends Table[(Int, String, String, Int, Int, String, String, String, String, String,
    String, TIMESTAMP, String, Int, String, String, String, String, String)](tag, "SocialCredentials"){

  }
}*/

import gr.gnostix.api.db.lifted.OracleLiftedQueries


var koko = OracleLiftedQueries.getBetaUsers
