package gr.gnostix.restora.gr.gnostix.restora.db

import com.typesafe.slick.driver.oracle.OracleDriver.simple._


trait OraQueries {

  val db: Database

  def getBetaUsers = {
    val query = for (c <- OraTables.betaUsers) yield c.email
    val result = db.withSession {
      session =>
        query.list()( session )
    }
    result
  }


}
