

case class Rating(name: String, valu: Double)
case class HotelRatingStats(ratingName: String, ratingValue: Double)
case class RevStat(service_name: String, score: Double, numMsg: Int)
val li = List(HotelRatingStats("Value", 10), HotelRatingStats("Value", 8.0),
  HotelRatingStats("Value", 10.0), HotelRatingStats("Value", 8.0), HotelRatingStats("Value", 6),
  HotelRatingStats("sleep", 6.0), HotelRatingStats("staff", 6.0), HotelRatingStats("room", 8),
  HotelRatingStats("location", 10.0), HotelRatingStats("room", 4.0), HotelRatingStats("staff", 6),
  HotelRatingStats("room", 4.0), HotelRatingStats("sleep", 8.0), HotelRatingStats("Value", 10),
  HotelRatingStats("location", 8.0), HotelRatingStats("staff", 10.0), HotelRatingStats("staff", 6),
  HotelRatingStats("clean", 1.0), HotelRatingStats("staff", 10.0), HotelRatingStats("location", 6),
  HotelRatingStats("staff", 4.0), HotelRatingStats("sleep", 6.0), HotelRatingStats("staff", 8),
  HotelRatingStats("sleep", 6.0), HotelRatingStats("location", 8.0), HotelRatingStats("Value", 8),
  HotelRatingStats("clean", 4.0), HotelRatingStats("clean", 8.0), HotelRatingStats("staff", 6),
  HotelRatingStats("sleep", 8.0), HotelRatingStats("clean", 10), HotelRatingStats("location", 10),
  HotelRatingStats("room", 10.0), HotelRatingStats("sleep", 4.0), HotelRatingStats("staff", 6),
  HotelRatingStats("location", 10.0), HotelRatingStats("staff", 8.0), HotelRatingStats("sleep", 8))

li.groupBy(_.ratingName).map{
  case (x, y) => (x -> Map("positive" -> y.filter(a => a.ratingValue >= 7).size,
                           "negative" -> y.filter(a => a.ratingValue <= 4).size,
                           "neutral" -> y.filter(a => a.ratingValue > 4 && a.ratingValue < 7).size,
                           "score" -> (y.map(x => x.ratingValue).sum / y.size)
                          ).toList )
}

li.groupBy(_.ratingName).map {
  case (x, y) => (x, y.map(_.ratingValue).sum / y.size)
}

li.groupBy(_.ratingName).map {
  case (x, y) => (x, y.groupBy(_.ratingValue).map {
    case (a, s) => RevStat(s.head.ratingName, a, s.size)
  })._2
}




li.
  groupBy(_.ratingName).map {
  case (x, y) => (x, y.groupBy(_.ratingValue).map {
      case (a, s) => RevStat(s.head.ratingName, a, s.size)
    })
}.map {
  case (q, w) => {
    List(w.toList.sortBy(r => (r.score, r.numMsg)).head,
      w.toList.sortBy(r => (r.score, r.numMsg)).reverse.head)
  }
}.toList.flatten.sortBy(n => (n.score, n.numMsg))


val first = li.
  groupBy(_.ratingName).map {
  case (x, y) => (x, y.groupBy(_.ratingValue).map {
    case (a, s) => RevStat(s.head.ratingName, a, s.size)
  })
}

val second = first.toStream.map {
  case (q, w) => {
    List(w.toList.sortBy(r => (r.score, r.numMsg)).head,
      w.toList.sortBy(r => (r.score, r.numMsg)).reverse.head)
  }
}

second.toList.flatten.sortBy(n => (n.score, n.numMsg))



val sleep = li.filter(x => x.ratingName.equalsIgnoreCase("sleep"))
  .groupBy(_.ratingValue).map {
  case (x, y) => RevStat(y.head.ratingName, x, y.size)
}.toList

val sleep1 = sleep.sortBy(x => x.score) match {
  case Nil => Nil
  case (h :: Nil) => List(h)
  case (h :: t) => List(h, t.reverse.head)
}

//
//val value = k.filter(x => x.name.equalsIgnoreCase("value")).size
//
//case class RevStat(service_name: String, score: Int, numMsg: Int)
////k.filter(x => x.name.equals("sleep"))
//val sleep = k.filter(x => x.name.equals("sleep")).groupBy(_.valu).map {
//  case (x,y) => RevStat(y.head.name,x,y.size)
//}.toList
//
//val sleep1 = sleep.sortBy(x => x.score) match {
//  case (h :: Nil) => List(h)
//  case (h :: t) => List(h, t.reverse.head)
//}
//
//val room = k.filter(x => x.name.equals("room")).groupBy(_.valu).map {
//  case (x,y) => RevStat(y.head.name,x,y.size)
//}.toList
//
//val room1 = room.sortBy(x => x.score)  match {
//  case (h :: Nil) => List(h)
//  case (h :: t) => List(h, t.reverse.head)
//}
//
//val clean = k.filter(x => x.name.equals("clean")).groupBy(_.valu).map {
//  case (x,y) => RevStat(y.head.name,x,y.size)
//}.toList
//
//val clean1 = clean.sortBy(x => x.score)  match {
//  case (h :: Nil) => List(h)
//  case (h :: t) => List(h, t.reverse.head)
//}
//
//val aa1 = List(sleep1, room1, clean1).flatMap(x => x).sortBy(r => (r.score, r.numMsg))
//val neg = aa1.take(3)
//val pos = aa1.reverse.take(2)
//
//
//val aa = List(sleep, room, clean).flatMap(x => x).sortBy(r => (r.score, r.numMsg))
//
//val negative = aa.take(3)
//val positive = aa.reverse.take(2)
