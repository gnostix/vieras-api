package gr.gnostix.api.utilities;

import org.omg.CORBA.Request;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Created by rebel on 23/10/14.
 */
public class TwOauth {

    private static final String TwitterConsumerKey = "lJ43ewh5JHU5wehbfOgDrw";
    private static final String TwitterConsumerSecret = "i9FRRM5JUjLm3amoYyPjCc4dVx50U21eSaUghYGaJI0";
    private static RequestToken requestToken;
    private static Twitter twitter;

    public static String getUrlAuth() {
        String url = null;
        RequestToken requestToken = null;
        try {
            requestToken = getTwitter().getOAuthRequestToken();
            setRequestToken(requestToken);
            url = requestToken.getAuthorizationURL();
            System.out.println("------------> " + url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }

    public static String getUserToken(String pin) {
        AccessToken accessToken = null;

        try {
            System.out.println("------------> " + TwOauth.requestToken);
            accessToken = getTwitter().getOAuthAccessToken(TwOauth.requestToken, pin);
            System.out.println("------------> " + accessToken.getToken() + " " + accessToken.getScreenName() + " "
                    + accessToken.getTokenSecret());
            // save the user account
        } catch (Exception e) {
            e.printStackTrace();
            return "error on auth";
        }
        return accessToken.getScreenName();
    }

    private static void setRequestToken(RequestToken requestToken) {
        TwOauth.requestToken = requestToken;
        System.out.println("------------> set token " + TwOauth.requestToken);
    }

    private RequestToken getRequestToken() {
        System.out.println("------------> set token " + TwOauth.requestToken);
        return TwOauth.requestToken;
    }

    private static void setTwitter() {
        TwOauth.twitter = TwitterFactory.getSingleton();
        twitter.setOAuthConsumer(TwitterConsumerKey, TwitterConsumerSecret);
    }

    private static Twitter getTwitter() {
        if (TwOauth.twitter == null) {
            TwOauth.setTwitter();
        }
        return TwOauth.twitter;
    }

}
