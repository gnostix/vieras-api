package gr.gnostix.api.models

import java.sql.Date

case class User(name: String)
case class Customer(id: Int, name: String, lastname: String, t_results: Int)


/*
case class User1(userId: Int, username: String, password: String, userLevel: Int, firstname: String, lastname: String,
          registrationDate: Date, email: String, streetAddress: String, StreetNum: Int, postalCode: Int, city: String,
          company: String, language: String, expirationDate: Date, totalCounts: Int, totalKeywords: Int, enabled: Int,
          sentEmail: Int, totalProfiles: Int, totalFbFanPages: Int, totalTwitterAccounts: Int, totalTopicProfiles: String)
*/

