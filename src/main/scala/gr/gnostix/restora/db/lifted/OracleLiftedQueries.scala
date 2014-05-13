package gr.gnostix.restora.db.lifted


trait OracleLiftedQueries {
	import com.typesafe.slick.driver.oracle.OracleDriver.simple._

  val db: Database

  def getBetaUsers = {
    val query = for (c <- OracleLiftedTables.betaUsers) yield c.email
    val result = db.withSession {
      session =>
        query.list()( session )
    }
    result
  }
}

