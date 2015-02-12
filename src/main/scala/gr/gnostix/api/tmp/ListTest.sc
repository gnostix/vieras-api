case class HotelServicesLineTest(rating: String, value: Double, created: String)

val k1 = List(("positive", 23), ("negative", 56), ("neutral", 89))
val k2 = List(("positive", 123), ("negative", 156), ("neutral", 189))
val k3 = List(("positive", 3), ("negative", 6), ("neutral", 9))

(k1 ++ k2 ++ k3).groupBy(_._1)

(k1 ++ k2 ++ k3).groupBy(_._1).map {
  case (key, value) => (key, value.map(_._2).sum)
}.toList

(k1 ++ k2 ++ k3).groupBy(_._1)

val z = List((17, "2014-10-25 00:00:00.0"),
  (27, "2014-10-27 00:00:00.0"))

val s = List((32, "2014-10-26 00:00:00.0"),
  (34, "2014-10-28 00:00:00.0"),
  (56, "2014-10-29 00:00:00.0"))


val j = (z ++ s)

j.sortWith(_._2 > _._2)

val t1 = List(("positive",23),
  ("negative", 56), ("neutral", 89))
val t2 = List(("positive",253),
  ("negative", 536), ("neutral", 289))


val d = t1 :+ t2

val grp = d

val dtd = List(HotelServicesLineTest("Value", 5.6, "21-01-2015"),
  HotelServicesLineTest("Value", 5.6, "21-01-2015"),
  HotelServicesLineTest("Location", 5.6, "21-01-2015"),
  HotelServicesLineTest("Sleep", 5, "21-01-2015"),
  HotelServicesLineTest("Sleep", 5, "21-01-2015"),
  HotelServicesLineTest("Value", 5.6, "21-01-2015"),
  HotelServicesLineTest("Value", 5.6, "21-01-2015"),
  HotelServicesLineTest("Value", 5.6, "22-01-2015"),
  HotelServicesLineTest("Value", 5.6, "22-01-2015"),
  HotelServicesLineTest("Value", 5.6, "21-01-2015"))

dtd.groupBy(x => x.created).map{
  case (x,y) => (x, y.groupBy(r => r.rating).map{
    case (w,s) => (w, (s.map(_.value).sum/s.size))
  })
}.toList

