import java.net.{URL, URI, URLEncoder}
import java.util
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import org.fusesource.scalate.util.Resource

case class Person(firstName: String, lastName: String, age: Int)
object HttpJsonPostTest extends App {
  // create our object as a json string
  val spock = new Person("Leonard", "Nimoy", 82)
  val authToken = "2b4949b29ed1e8ed33b75e0017372d43"
  // add name value pairs to a post object
  val url = "https://crm.zoho.com/crm/private/xml/Leads/insertRecords?"
  val newFormat = "1"
  val scope = "crmapi"
  val xmlData = "xmlData"
  val data = "<Leads> <row no=\"1\">" +
    "<FL val=\"Company\">Zohohohoho</FL>"
  " <FL val=\"First Name\">Roberta</FL> " +
    "<FL val=\"Last Name\">Gogogogos</FL>" +
    " <FL val=\"Designation\">CMO</FL>" +
    " <FL val=\"Email\">roberta@test.com</FL>" +
    " </row> </Leads>"
  val post = new HttpPost(url + "newFormat=1" + "&authToken="+authToken
      + "&scope=crmapi" + "&xmlData="+ data)
  val params = client.getParams
  lazy val client = new DefaultHttpClient

  // set the Content-type
  post.setHeader("Content-type", "application/xml")
/*  params.setParameter("newFormat", "1")
  params.setParameter("authtoken", authToken)
  params.setParameter("scope", "crmapi")
  params.setParameter("xmlData", data)*/
/*  val nameValuePairs = new util.ArrayList[NameValuePair](1)
  nameValuePairs.add(new BasicNameValuePair("newFormat", "1"))
  nameValuePairs.add(new BasicNameValuePair("authtoken", authToken))
  nameValuePairs.add(new BasicNameValuePair("scope", "crmapi"))
  nameValuePairs.add(new BasicNameValuePair("xmlData", data))
  post.setEntity(new UrlEncodedFormEntity(nameValuePairs))*/
  // send the post request

  String urlStr = "http://www.example.com/CEREC® Materials & Accessories/IPS Empress® CAD.pdf"
  val url: URL = new URL(url);
  val uri:URI = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());

  post.toString
  val response = client.execute(post)
  println("--- HEADERS ---")
  response.getAllHeaders.map(println)
  println("--- CONTENT ---")
  response.getEntity.getContent
}