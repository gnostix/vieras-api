package gr.gnostix.api.utilities;

import gr.gnostix.api.models.SocialCredentialsSimple;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;

/**
 * Created by rebel on 23/10/14.
 */
public class TwOauth {

    private static final String TwitterConsumerKey = "lJ43ewh5JHU5wehbfOgDrw";
    private static final String TwitterConsumerSecret = "i9FRRM5JUjLm3amoYyPjCc4dVx50U21eSaUghYGaJI0";
    private RequestToken requestToken = null;
    private static Twitter twitter = null;

    public String getUrlAuth(RequestToken requestToken) {
        String url = null;

        try {
            url = requestToken.getAuthorizationURL();

            System.out.println("------------> getUrlAuth requestToken " + requestToken);
            System.out.println("------------> " + url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }

    public AccessToken getUserToken(String pin, int profileId, RequestToken requestToken) {
        AccessToken accessToken = null;
        SocialCredentialsSimple twAccount = null;

        try {
            //requestToken = twitter.getOAuthRequestToken();
            System.out.println("------------> getUserToken " + requestToken);
            accessToken = twitter.getOAuthAccessToken(requestToken, pin);
            System.out.println("------------> getUserToken " + accessToken.getToken() + " " + accessToken.getScreenName() + " "
                    + accessToken.getTokenSecret());

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return accessToken;
    }

    public RequestToken getRequestToken() {
        try {
            // RequestToken requestToken = null;
            TwOauth.twitter = getTwitter();
            requestToken = TwOauth.twitter.getOAuthRequestToken();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return requestToken;
    }

    public Twitter getTwitter() {
        try {
            if (TwOauth.twitter != null) {
                System.out.println("------------> getTwitter OLD " + TwOauth.twitter);
                return TwOauth.twitter;
            }
            TwOauth.twitter = TwitterFactory.getSingleton();
            //twitter = new TwitterFactory().getInstance();
            if (TwOauth.twitter.getConfiguration().getOAuthConsumerKey() == null)
                TwOauth.twitter.setOAuthConsumer(TwitterConsumerKey, TwitterConsumerSecret);

            System.out.println("------------> getTwitter TwitterFactory().getInstance " + TwOauth.twitter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return TwOauth.twitter;
    }


}
