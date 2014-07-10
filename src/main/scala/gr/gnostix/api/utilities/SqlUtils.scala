package gr.gnostix.api.utilities

import gr.gnostix.api.models.SocialData
import org.joda.time.DateTime

/**
 * Created by rebel on 16/6/14.
 */
object SqlUtils {


  def getDataDefaultObj(profileId: Int): String = {
    val mySqlDyn = s"""fk_k_id in (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId}))"""
    mySqlDyn
  }

  def getDataByKeywordsObj(profileId: Int, keywords: List[Int]): String = {
    val mySqlDyn = keywords match {
      case List() => s"""fk_k_id in (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId}))"""
      case List(x) => s""" fk_k_id = ${keywords.head}"""
      case x :: xs => s"""fk_k_id in ( ${keywords.mkString(",")} )"""
    }
    mySqlDyn
  }

  def getDataByTopicsObj(profileId: Int, topics: List[Int]): String = {
    val mySqlDyn = topics match {
      case List() => s""" fk_k_id in (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})) """
      case List(x) => s""" fk_k_id in (select k_id from KEYWORDS where fk_sd_id = ${topics.head} ) """
      case x :: xs => s""" fk_k_id in (select k_id from KEYWORDS where fk_sd_id in ( ${topics.mkString(",")} )) """
    }
    mySqlDyn
  }
}
