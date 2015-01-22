

/*object SATWLiftedextends extends DatabaseAccessSupport {
  class SocialCredentials(tag: Tag) extends Table[(Int, String, String, Int, Int, String, String, String, String, String,
    String, TIMESTAMP, String, Int, String, String, String, String, String)](tag, "SocialCredentials"){

  }
}*/

import gr.gnostix.api.db.lifted.OracleLiftedQueries


var koko = OracleLiftedQueries.getBetaUsers
