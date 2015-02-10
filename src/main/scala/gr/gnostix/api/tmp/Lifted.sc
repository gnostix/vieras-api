import java.security.MessageDigest

import scala.util.Random

/*object SATWLiftedextends extends DatabaseAccessSupportOra {
  class SocialCredentials(tag: Tag) extends Table[(Int, String, String, Int, Int, String, String, String, String, String,
    String, TIMESTAMP, String, Int, String, String, String, String, String)](tag, "SocialCredentials"){

  }
}*/

val r = Random.alphanumeric(10)
r
def randomAlphaNumericString(length: Int): String = {
  val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
  randomStringFromCharList(length, chars)
}

def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
  val sb = new StringBuilder
  for (i <- 1 to length) {
    val randomNum = util.Random.nextInt(chars.length)
    sb.append(chars(randomNum))
  }
  sb.toString
}

randomAlphaNumericString(10)

MessageDigest.getInstance("MD5").digest("testgn0st1x".getBytes())

java.security.MessageDigest.getInstance("SHA").digest("p_alx@hotmail.comoXXL3tUsin".getBytes())
    .map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}


java.security.MessageDigest.getInstance("SHA").digest(("p_alx@hotmail.comoXXL3tUsin").getBytes())
  .map(0xFF & _).map {
  "%02x".format(_)
}.foldLeft("") {
  _ + _
}