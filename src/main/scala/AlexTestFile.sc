import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

import scala.concurrent.Future

val toDateStr = "25-02-2014 12:34:56"
val pattern: String = "dd-MM-yyyy HH:mm:ss"


def toInt1(in: String): Option[Int] = {
  try {
    Some(Integer.parseInt(in.trim))
  } catch {
    case e: NumberFormatException => None
  }
}

toInt1("321") match {
  case Some(i) => {
    println(i)
    val a = i * i
    //println(a)
    i
  }
  case None => println("That didn't work.")
}

val s = List("1","44","abc")
s.flatMap(toInt1).sum

/*
val c: Future[Int] = Future {1+1}
val f: Future[String] = Future { "Hello world!" }*/

val k1 = List(("positive",23), ("negative", 56), ("neutral", 89))
val k2 = List(("positive",123), ("negative", 156), ("neutral", 189))
val k3 = List(("positive",3), ("negative", 6), ("neutral", 9))
val z = k1.zip(k2).zip(k3)

z.foreach(x => println(x))

z.foreach(x => println(x._2))

val d = List(k1 ::: k2 ::: k3)


d.map(x => println(x))

( k1 ++ k2 ++ k3).groupBy( _._1 ).map( kv => (kv._1, kv._2.map( _._2).sum ) ).toList

( k1 ++ k2 ).groupBy( _._1 )


( k1 ++ k2 ).groupBy( _._1 ).map(x => (x._1, x._2.map(_._2).sum)).toList

k1.map(x => x._2).sum


( k1 ++ k2 ++ k3 ).groupBy( _._1 ).map{
  case (key,tuples) => (key, tuples.map( _._2).sum )
}.toList


(k1 ::: k2).groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortWith(_._2 < _._2)






