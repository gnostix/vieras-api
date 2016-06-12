import java.net.URL

import dispatch.url
import gr.gnostix.api.models.plainModels.HotelRatingStats

val k = List(HotelRatingStats("Value", 10), HotelRatingStats("Value", 8), HotelRatingStats("Value", 6),
  HotelRatingStats("sleep", 6), HotelRatingStats("staff", 6), HotelRatingStats("room", 8),
  HotelRatingStats("location", 10), HotelRatingStats("room", 4), HotelRatingStats("staff", 6),
  HotelRatingStats("room", 4), HotelRatingStats("sleep", 8), HotelRatingStats("Value", 10),
  HotelRatingStats("location", 8), HotelRatingStats("staff", 10), HotelRatingStats("staff", 6),
  HotelRatingStats("clean", 1), HotelRatingStats("staff", 10), HotelRatingStats("location", 6),
  HotelRatingStats("staff", 4), HotelRatingStats("sleep", 6), HotelRatingStats("staff", 8),
  HotelRatingStats("sleep", 6), HotelRatingStats("location", 8), HotelRatingStats("Value", 8),
  HotelRatingStats("clean", 4), HotelRatingStats("clean", 8), HotelRatingStats("staff", 6),
  HotelRatingStats("sleep", 8), HotelRatingStats("clean", 1), HotelRatingStats("location", 10),
  HotelRatingStats("room", 10), HotelRatingStats("sleep", 4), HotelRatingStats("staff", 6),
  HotelRatingStats("location", 10), HotelRatingStats("staff", 8), HotelRatingStats("sleep", 8))

k.groupBy(_.ratingName).map {
  case (x, y) => (x -> Map("positive" -> y.filter(a => a.ratingValue >= 7).size,
    "negative" -> y.filter(a => a.ratingValue <= 4).size,
    "neutral" -> y.filter(a => a.ratingValue > 4 && a.ratingValue < 7).size,
    "score" -> {
      val p =  y.filter(a => a.ratingValue >= 7).size
      val n = y.filter(a => a.ratingValue <= 4).size
      println(p)
      p - n
    }
  ))
}

def minus(i: Int, a: Int) = i - a

k.groupBy(_.ratingName).map{
  case (x, y) => {
    val pos =  y.filter(a => a.ratingValue >= 7).size.min(2)
    val neg = y.filter(a => a.ratingValue <= 4).size
    val neu = y.filter(a => a.ratingValue > 4 && a.ratingValue < 7).size
    (pos, neg, neu)
  }
}

  k.groupBy(_.ratingName).map {
  case (x, y) => (x -> Map("positive" -> y.filter(a => a.ratingValue >= 7).size,
    "negative" -> y.filter(a => a.ratingValue <= 4).size,
    "neutral" -> y.filter(a => a.ratingValue > 4 && a.ratingValue < 7).size,
    "score" -> minus( y.filter(a => a.ratingValue >= 7).size, y.filter(a => a.ratingValue <= 4).size)
  ))
}

k.groupBy(_.ratingName).map {
  case (x, y) => (x -> Map("positive" -> y.filter(a => a.ratingValue >= 7).size,
    "negative" -> y.filter(a => a.ratingValue <= 4).size,
    "neutral" -> y.filter(a => a.ratingValue > 4 && a.ratingValue < 7).size,
    "score" -> (y.filter(a => a.ratingValue >= 7).size - y.filter(a => a.ratingValue <= 4).size)
  ))
}


val l = "www.tripadvisor.com/Hotel_Review-g189400-d197721-Reviews-Plaka_Hotel-Athens_Attica.html"

if(l.startsWith("http://www."))
  l.split("\\.").toList.tail.head.capitalize
else if (l.startsWith("http://"))
  l.drop(7).split("\\.").toList.head.capitalize
else if (l.startsWith("www."))
  l.split("\\.").toList.tail.head.capitalize
