import gr.gnostix.api.models.SocialDataSum

val k1 = List(("positive",23), ("negative", 56), ("neutral", 89))
val k2 = List(("positive",123), ("negative", 156), ("neutral", 189))
val k3 = List(("positive",3), ("negative", 6), ("neutral", 9))

(k1 ++ k2 ++ k3).groupBy(_._1).map{
  case (key, value) => (key, value.map(_._2).sum)
}.toList

(k1 ++ k2 ++ k3).groupBy(_._1)



/*

val k4 = List(1,2,3,4,5)

val k5 = List((SocialDataSum("positive",3),
  SocialDataSum("positive",3), SocialDataSum("positive",4)))
val k6 = List(("positive",3), ("negative", 6), ("neutral", 9))

val q = k1.dropWhile(x => x._2 >= 50)
k1.span(y => y._1 != "positive")
k1.splitAt(2)
k1.foreach(x => x._2 * 3)

def ff1( a: Int ) = {
  a * 2
}
k4.foreach(_ * 8)
k1.map(x => x._2 * 6)

k1.map(_._1).stringPrefix
k1.filter(x => x._1 != "positi")

k1.groupBy(_._1).map(_._1)
k1.find(x => x._2 > 50)
k1.map(_._2).max
k1.map(_._2).sum
(k1 ++ k2 ++ k3).map(_._2)
(k1 ++ k2 ++ k3).map(_._2).max



k6.groupBy(_._1).map
  {
  case (key, value) => (key, value.map(_._2).sum)
}.toList

//k5.map(_._2).sum




*/
