import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

val toDateStr = "25-02-2014 12:34:56"
val pattern: String = "dd-MM-yyyy HH:mm:ss"


//DateTime.parse(toDateStr,
//  DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
/*

var str12: String = "Alex"
println(str12)
str12 = "Bil"
println(str12)
*/
val myList = List(1,2)
//println("myList: " + myList.mkString(","))
val koko = myList match {
  case List() => println("the list is empty")
  case List(x)  => println("the list has one element " + myList.head)
  case x :: xs  => println("the list has many elements")

}

val dt = "gplus"
dt match {
  case "twitter" => println("twitter")
  case "facebook" => println("facebook")
  case "gplus" => println("gplus")
  case _ => println("unknown")
}