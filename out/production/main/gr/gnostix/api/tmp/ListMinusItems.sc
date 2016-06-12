import gr.gnostix.api.models.plainModels.{RevStat, HotelRatingStats}


val li = List(HotelRatingStats("value", 10), HotelRatingStats("value", 8), HotelRatingStats("value", 6),
  HotelRatingStats("location", 7), HotelRatingStats("staff", 6), HotelRatingStats("room", 8),
  HotelRatingStats("value", 3), HotelRatingStats("value", 4), HotelRatingStats("value", 4),
  HotelRatingStats("value", 2), HotelRatingStats("sleep", 8), HotelRatingStats("value", 10),
  HotelRatingStats("location", 8), HotelRatingStats("sleep", 10), HotelRatingStats("staff", 6),
  HotelRatingStats("location", 9), HotelRatingStats("sleep", 4), HotelRatingStats("sleep", 3),
  HotelRatingStats("room", 8), HotelRatingStats("sleep", 4), HotelRatingStats("staff", 6),
  HotelRatingStats("location", 9), HotelRatingStats("staff", 8), HotelRatingStats("staff", 6))
case class ServiceSentiment(serviceName: String, msgNum: Int)

val p1 = li.filter(_.ratingValue > 7).groupBy(_.ratingName).map{
  case (x,y) => ServiceSentiment(x, y.size)
}.toList.sortBy(_.msgNum).reverse.take(5)

val n1 = li.filter(_.ratingValue < 4).groupBy(_.ratingName).map{
  case (x,y) => ServiceSentiment(x, y.size)
}.toList.sortBy(_.msgNum).reverse.take(5)


val firstStep = li.
  groupBy(_.ratingName).map {
  case (x, y) => (x, y.groupBy(_.ratingValue).map {
    case (a, s) => RevStat(s.head.ratingName, a, s.size)
  })
}

val secondStep = firstStep.map {
  case (q, w) => {
    List(w.toList.sortBy(r => (r.score, r.numMsg)).head,
      w.toList.sortBy(r => (r.score, r.numMsg)).reverse.head)
  }
}

val ko = secondStep.flatten.groupBy(_.service_name).map{
  case (x,y) => y.toList.distinct
}

val p  = ko.flatten.filter(_.score > 6)

val massagedData =
  secondStep.toList.flatten.sortBy(n => (n.score, n.numMsg)).distinct
massagedData.distinct

val neg = massagedData.filter(_.score < 5).take(5).toList.sortBy(x => x.service_name)
val pos = massagedData.filter(_.score > 6).reverse.take(5).toList.sortBy(x => x.service_name)

val intList = List(2, 7, 9, 1, 6, 5, 8, 2, 4, 6, 2, 9, 8)
val (big, small) = li partition (_.ratingValue > 7)
/*
case class HotelTextDataRatingTmp(id: Int, ratingName: String, ratingValue: Double)
val a = List(HotelTextDataRatingTmp(72499,null,0.0),
  HotelTextDataRatingTmp(72499,null,0.0),
  HotelTextDataRatingTmp(72499,null,0.0),
  HotelTextDataRatingTmp(72499,null,0.0),
  HotelTextDataRatingTmp(72499,"Location",0.0),
  HotelTextDataRatingTmp(72499,"Service",0.0),
  HotelTextDataRatingTmp(7249,null,0.0),
  HotelTextDataRatingTmp(7249,null,0.0),
  HotelTextDataRatingTmp(7249,null,0.0),
  HotelTextDataRatingTmp(7249,null,0.0),
  HotelTextDataRatingTmp(7249,"Location",0.0),
  HotelTextDataRatingTmp(7249,"Service",0.0))

a.groupBy(_.id).map{
  case (s,d) => HotelTextDataRatingTmp(s, d.filter(_.ratingName != null).head.ratingName, 0.0)
}.toList
*/


/*import gr.gnostix.api.models.plainModels.HotelTextDataRating
import scala.collection.mutable.ListBuffer
val lii = List(3, 2, 4, 4, 5)
lii match {
  case Nil => Nil
  case x :: Nil => Nil
  case (x :: xs) => x * 2
}
case class YTdata(views: Int, favs: Int)

val ytD = List(YTdata(1, 2), YTdata(2, 3), YTdata(3, 4), YTdata(4, 5))


def dataMinus(li: List[YTdata]): List[YTdata] = {
  val buf = new ListBuffer[YTdata]

  @annotation.tailrec
  def go(li: List[YTdata]): List[YTdata] = {
    li match {
      case Nil => Nil
      case x :: Nil => List(buf.toList: _*)
      case x :: y :: Nil => buf += YTdata(y.views - x.views, y.favs - x.favs); go(y :: Nil)
      case x :: y :: xs => buf += YTdata(y.views - x.views, y.favs - x.favs); go(y :: xs)
    }
  }

  go(li)
}

dataMinus(ytD)


def koko(li: List[Int]): List[Int] = {
  val buf = new ListBuffer[Int]
  @annotation.tailrec
  def go(li: List[Int]): List[Int] = {
    li match {
      case Nil => Nil
      case (_ :: Nil) => List(buf.toList: _*)
      case (x :: xs) => (buf += xs.head - x); go(xs)
    }
  }

  go(li)
}*/

