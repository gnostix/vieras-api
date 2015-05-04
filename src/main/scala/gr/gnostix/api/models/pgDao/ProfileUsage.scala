package gr.gnostix.api.models.pgDao


import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 12/6/14.
 */
case class ProfilesUsage(userLevel: Int, maxCounts: Int, description: String, maxKeywords: Int,
         maxTopics: Int, socialAccounts: Int, maxProfiles: Int, companies: Int, companyUrls: Int, competitorUrls: Int)


object ProfilesUsage extends DatabaseAccessSupportPg {

  implicit val getProfilesUsageResult = GetResult(r => ProfilesUsage(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  def findByUserlevel(userLevel: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[ProfilesUsage](s"""
          select ID,MAX_COUNTS,DESCRIPTION,MAX_KEYWORDS,MAX_TOPICS,
          social_accounts,profiles,companies,company_urls,competitor_urls
            from vieras.user_level_counts where id = $userLevel
          """)
        records.first
    }
  }
}
