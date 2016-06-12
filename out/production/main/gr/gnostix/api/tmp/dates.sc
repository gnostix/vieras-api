import java.sql.Timestamp
import java.util.{GregorianCalendar, Calendar, Date}

import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.joda.time.{LocalDate, DateTime, DateTimeConstants}
val expiration : Timestamp = new Timestamp(new Date().getTime)
expiration.after(new Date())
val c:Calendar = Calendar.getInstance();   // this takes current date
c.set(Calendar.DAY_OF_MONTH, 1);
System.out.println(c.getActualMaximum(5));
val formatterFecha: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy");
val primerDiaDelMes: DateTime = new DateTime();
//val desde: String = new LocalDate(primerDiaDelMes).toString(formatterFecha);
//
//val ultimoDiaDelMes: DateTime = new DateTime().dayOfMonth().withMaximumValue();
//val hasta: String = new LocalDate(ultimoDiaDelMes).toString(formatterFecha);