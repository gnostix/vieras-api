package gr.gnostix.restora

import _root_.gr.gnostix.restora.db.OraQueries
import org.scalatra._
import scalate.ScalateSupport
import scala.slick.lifted.{TableQuery, Tag}
import com.typesafe.slick.driver.oracle.OracleDriver.simple._

  class DSGroups(tag: Tag) extends Table[(Int, String)]( tag, "DS_GROUPS" ) {
    def groupId = column[Int]( "G_ID", O.PrimaryKey )
    def groupName = column[String]( "GROUP_NAME" )

    // Every table needs a * projection with the same type as the table's type parameter
    def * = ( groupId, groupName )
  }

  trait RestApiRoutes extends ScalatraServlet with OraQueries{

    val grp = TableQuery[DSGroups]
    val db: Database

    get("/groups") {

     val query = for (c <- grp) yield c.groupName
      val result = db.withSession {
        session =>
          query.list()( session )
      }
      result foreach println // works for one column
      result.toString()

/*      grp foreach { case (groupId, groupName) =>
        println("Group Id " + groupId + " Group Name: " + groupName)
      }
      grp.toString()*/
    }

    get("/beta"){
      getBetaUsers.toString()
    }



    get("/") {
      <html>
        <body>
          <h1>Hello, world! Alex</h1>
          Say <a href="hello-scalate">hello to Scalate</a>.
        </body>
      </html>
    }

}

case class MyScalatraServlet(db: Database) extends ScalatraoraStack with RestApiRoutes

