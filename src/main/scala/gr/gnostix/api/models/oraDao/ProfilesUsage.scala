package gr.gnostix.api.models.oraDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 12/6/14.
 */
case class ProfilesUsage(id: Int, userLevel: Int, maxCounts: Int, description: String, maxKeywords: Int,
                    maxTopics: Int, maxFanPages: Int, maxTopicProfiles: Int,
                     maxHospitalityAccounts: Int, maxSocialAnalAccounts: Int)


object ProfilesUsage extends DatabaseAccessSupport{

  implicit val getProfilesUsageResult = GetResult(r => ProfilesUsage(r.<<, r.<<, r.<<, r.<<,
     r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  def findByUserlevel(userLevel: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[ProfilesUsage](s"""select ID,USER_LEVEL,MAX_COUNTS,DESCRIPTION,MAX_KEYWORDS,
            MAX_TOPICS,FAN_PAGES,MAX_TOPICS,MAX_HOSPITALITY,MAX_SOCIAL
            from user_level_counts where user_level = $userLevel""")
        records.first
    }
  }
}