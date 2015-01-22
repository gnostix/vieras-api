package gr.gnostix.api.db.plainsql

import com.mchange.v2.c3p0.ComboPooledDataSource

import scala.slick.jdbc.JdbcBackend._

/**
 * Created by rebel on 29/5/14.
 */
object DatabaseAccess {

  def createDatasource :ComboPooledDataSource = {
    val ds = new ComboPooledDataSource
    ds.setDriverClass("oracle.jdbc.driver.OracleDriver")
    ds.setJdbcUrl("jdbc:oracle:thin:@db.gnstx.gr:1521:ora")
    ds.setUser("vieras")
    ds.setPassword("vieras11031977")
    ds.setMinPoolSize(1)
    ds.setAcquireIncrement(1)
    ds.setMaxPoolSize(5)
    ds.setInitialPoolSize(1)
    ds.setAutoCommitOnClose(true)
    ds
  }
  val myDS = createDatasource

  val database = Database.forDataSource(myDS)

  def closeDBPool {myDS.close()}

}

trait DatabaseAccessSupport {

  // for dev local db pool. Comment out this for production
  def getConnection = DatabaseAccess.database

  // with jndi support. Uncomment this for production
  //def getConnection = Database.forName("jdbc/myOracleDB_GER")
}