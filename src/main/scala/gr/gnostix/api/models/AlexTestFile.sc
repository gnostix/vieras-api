import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

val toDateStr = "25-02-2014 12:34:56"
val pattern: String = "dd-MM-yyyy HH:mm:ss"

//val toDate: DateTime = DateTime.parse(toDateStr,DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))

val toDate: DateTime = new DateTime(toDateStr)

val fmt: DateTimeFormatter  = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
val sqlTimeString: String  = fmt.print(toDate);