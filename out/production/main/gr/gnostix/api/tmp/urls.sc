import java.net.URL

val u23 = "http://www.booking.com"
val u = "http://www.booking.com/hotel/gr/njvathensplaza.html?sid=448d2139664b0a83987992b3adb87fa8;dcid=1;dist=0;type=total&#tab-reviews"
val uGR = "http://www.booking.com.gr/hotel/gr/njvathensplaza.html?sid=448d2139664b0a83987992b3adb87fa8;dcid=1;dist=0;type=total&#tab-reviews"
val l = new URL(u)

l.getAuthority
l.getHost
l.getPath

if(l.getHost.contains(".com.") ){
  val j = l.getHost.split("\\.").toList.reverse.
    drop(1).reverse.map(x => x + ".").mkString.reverse.drop(1).reverse
  l.getProtocol + "://" + j + l.getPath
} else {
  l.getProtocol + "://" + l.getHost + l.getPath
}
//must check if the url opens
val l2 = new URL(u23)
l2.getPath.length > 3


val rr = """(http|ftp)://(.*)\.([a-z]+)""".r

def splitURL(url : String) = url match {
  case rr(protocol, domain, tld) => println((protocol, domain, tld))
}

//splitURL("http://www.google.com/kjkjkji_oioioi")

val s = List("Location", "Room", "Sleep", "Cleanliness", "Value", "Staff")
val s2 = List("Cleanliness", "Value", "Staff")

s.diff(s2)


private def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
  val sb = new StringBuilder
  for (i <- 1 to length) {
    val randomNum = util.Random.nextInt(chars.length)
    sb.append(chars(randomNum))
  }
  sb.toString
}
