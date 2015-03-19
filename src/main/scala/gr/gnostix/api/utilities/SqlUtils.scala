package gr.gnostix.api.utilities

/**
 * Created by rebel on 16/6/14.
 */
object SqlUtils {

  def buildHotelCredentialsQuery(profileId: Int, companyId: Int): String = {

        s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co
            WHERE cr.FK_company_ID = $companyId and cr.FK_company_ID = co.id
              and co.fk_profile_id=$profileId
        """
  }

  def buildHotelDatasourceQuery(profileId: Int, companyId: Int, datasourceId: Int): String = {
    s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co
            WHERE cr.FK_company_ID = $companyId and cr.FK_DATASOURCE_ID=${datasourceId} and cr.FK_company_ID = co.id
              and co.fk_profile_id=$profileId
        """
  }

  def buildHotelCredIdQuery(profileId: Int, companyId: Int, credId: Int): String = {
    s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co
            WHERE cr.FK_company_ID = $companyId and cr.ID=${credId} and cr.FK_company_ID = co.id
              and co.fk_profile_id=$profileId
        """
  }


  def buildSocialCredentialsQuery(profileId: Int, companyId: Int, datasourceId: Int, credId: Option[Int]): String = {
    credId match {
      case Some(x) => x + " )"
      case None =>
        s"""select s.id from vieras.eng_profile_social_credentials s , vieras.eng_company co
          where s.fk_company_id in ( $companyId )
          and s.fk_datasource_id = $datasourceId and s.fk_company_id = co.id and co.fk_profile_id = $profileId
        """
    }
  }

  def getDataDefaultObj(profileId: Int): String = {
    val mySqlDyn = s"""fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in (select sd_id from TOPICS where fk_profile_id=${profileId}))"""
    mySqlDyn
  }

  def getDataByKeywordsObj(profileId: Int, keywords: List[Int]): String = {
    val mySqlDyn = keywords match {
      case List() => s"""fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in (select TOPIC_id from topics where fk_profile_id=${profileId}))"""
      case List(x) => s""" fk_k_id = ${keywords.head}"""
      case x :: xs => s"""fk_k_id in ( ${keywords.mkString(",")} )"""
    }
    mySqlDyn
  }

  def getDataByTopicsObj(profileId: Int, topics: List[Int]): String = {
    val mySqlDyn = topics match {
      case List() => s""" fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in (select TOPIC_id from TOPICS where fk_profile_id=${profileId})) """
      case List(x) => s""" fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id = ${topics.head} ) """
      case x :: xs => s""" fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in ( ${topics.mkString(",")} )) """
    }
    mySqlDyn
  }
}
