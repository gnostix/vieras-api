import gr.gnostix.api.models.{SentimentLine, DataLineGraph, SocialDataSum}

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


