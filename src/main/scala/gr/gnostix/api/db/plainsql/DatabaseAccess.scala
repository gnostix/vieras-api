package gr.gnostix.api.db.plainsql

import com.mchange.v2.c3p0.ComboPooledDataSource
import scala.slick.driver.PostgresDriver.simple._

//import scala.slick.jdbc.JdbcBackend._

/**
 * Created by rebel on 29/5/14.
 */
object DatabaseAccessOra {

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

trait DatabaseAccessSupportOra {

  // for dev local db pool. Comment out this for production
  def getConnection = DatabaseAccessOra.database

  // with jndi support. Uncomment this for production
  //def getConnection = Database.forName("jdbc/myOracleDB_GER")
}

object DatabaseAccessPg {

  def createDatasource :ComboPooledDataSource = {
    val ds = new ComboPooledDataSource
    ds.setDriverClass("org.postgresql.Driver")
    ds.setJdbcUrl("jdbc:postgresql://db.vieras.eu:5432/vieras")
    ds.setUser("vierasdev")
    ds.setPassword("11031977vieras")
    ds.setMinPoolSize(5)
    ds.setAcquireIncrement(3)
    ds.setMaxPoolSize(150)
    ds.setInitialPoolSize(5)
    ds
  }
  val myDS = createDatasource

  val database = Database.forDataSource(myDS)

  def closeDBPool {myDS.close()}

}

trait DatabaseAccessSupportPg {

  // for dev local db pool. Comment out this for production
  def getConnection = DatabaseAccessPg.database

  // with jndi support. Uncomment this for production
  //def getConnection = Database.forName("jdbc/myOracleDB_GER")
}