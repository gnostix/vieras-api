package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{GetResult, StaticQuery => Q}


case class DtTwitterLineGraph(tweetsNumb: Int, tweetDate: Timestamp)

object DtTwitterLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtTwitterLineGraphResult = GetResult(r => DtTwitterLineGraph(r.<<, r.<<))

  val twSqlLineByDay = """select count(*), trunc(t_created_at,'HH') from twitter_results i
                           where t_created_at between TO_DATE('2014-02-27', 'YYYY/MM/DD')
                           and TO_DATE('2014-02-27', 'YYYY/MM/DD')
                           group  BY trunc(t_created_at,'HH')
                           order by trunc(t_created_at, 'HH') asc"""

  def getTWLineDataByDay = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[DtTwitterLineGraph](twSqlLineByDay)
        records.list()
    }
  }

}

