package gr.gnostix.api.utilities

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary.Base64

/**
  * Created by rebel on 28/5/16.
  */
object SecurityUtil {

  def checkHMAC(secret: String, applicationName: String, hostname: String, hmac: String): Boolean = {
    return calculateHMAC(secret, applicationName, hostname) == hmac;
  }

  def calculateHMAC(secret: String, applicationName: String, hostname: String): String = {
    val signingKey = new SecretKeySpec(secret.getBytes(), "HmacSHA1");
    val mac = Mac.getInstance("HmacSHA1");
    mac.init(signingKey);
    val rawHmac = mac.doFinal((applicationName + "|" + hostname).getBytes());

    new String(Base64.encodeBase64(rawHmac));
  }
}
