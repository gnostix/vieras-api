package gr.gnostix.api.utilities;

import com.likethecolor.alchemy.api.Client;
import com.likethecolor.alchemy.api.call.LanguageCall;
import com.likethecolor.alchemy.api.call.type.CallTypeUrl;

import java.io.IOException;

/**
 * Created by rebel on 5/2/16.
 */
public class AlchemyApiClient {
    final static String apiKey = "6456ab27ba0ee055b087154707596eb205776c89";
    Client client = new Client(apiKey);

    public String findUrlLang(String url) {
        String urlLanguage = null;
        try {
            CallTypeUrl callType = new CallTypeUrl(url);
            urlLanguage = client.call(new LanguageCall(callType))
                    .getLanguage();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urlLanguage;
    }

    public boolean isEnglishLang(String url) {
        String urlLanguage = null;
        try {
            CallTypeUrl callType = new CallTypeUrl(url);
            urlLanguage = client.call(new LanguageCall(callType))
                    .getLanguage();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (urlLanguage != null)
            return urlLanguage.toLowerCase().equals("english");

        return false;
    }

}
