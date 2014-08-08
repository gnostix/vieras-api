package gr.gnostix.api.db.lifted

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport


object OracleLiftedQueries extends DatabaseAccessSupport {
	import com.typesafe.slick.driver.oracle.OracleDriver.simple._

  val db: Database = getConnection

  def getBetaUsers = {
    val query = for (c <- OracleLiftedTables.betaUsers) yield c.email
    val result = db.withSession {
      session =>
        query.list()( session )
    }
    result
  }
}

