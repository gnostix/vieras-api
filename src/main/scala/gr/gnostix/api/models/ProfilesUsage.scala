package gr.gnostix.api.models

import scala.slick.jdbc.{StaticQuery => Q, GetResult}
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport

/**
 * Created by rebel on 12/6/14.
 */
case class ProfilesUsage(id: Int, userLevel: Int, maxCounts: Int, description: String, maxKeywords: Int,
                    maxTopics: Int, maxFanPages: Int, maxTwitterAccounts: Int, maxYoutubeAccounts: Int,
                     maxTripAdvisorAccounts: Int, maxGoogleAnalAccounts: Int, maFSAccounts: Int,
                     maxBookingCom: Int, maxTopicProfiles: Int)


object ProfilesUsage extends DatabaseAccessSupport{

  implicit val getProfilesUsageResult = GetResult(r => ProfilesUsage(r.<<, r.<<, r.<<, r.<<,
     r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<< ))

  def findByUserlevel(userLevel: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[ProfilesUsage](s"""select ID,USER_LEVEL,MAX_COUNTS,DESCRIPTION,MAX_KEYWORDS,
            MAX_TOPICS,FAN_PAGES,TWITTER_ACCOUNTS,YOUTUBE,TRIP_ADVISOR,GOOGLE_ANALYTICS,FOUR_SQUARE,
            BOOKING_COM,TOPIC_PROFILES
      from user_level_counts where user_level = $userLevel""")
        records.first
    }
  }
}