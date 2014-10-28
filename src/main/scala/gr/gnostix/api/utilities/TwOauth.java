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
    private static RequestToken requestToken;
    private static Twitter twitter;

    public static String getUrlAuth() {
        String url = null;
        try {
            if (getTwitter() == null || TwOauth.requestToken == null) {
                TwOauth.requestToken = getTwitter().getOAuthRequestToken();
                url = TwOauth.requestToken.getAuthorizationURL();
            } else {
                url = TwOauth.requestToken.getAuthorizationURL();
            }
            System.out.println("------------> " + url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }

    public static AccessToken getUserToken(String pin, int profileId)  {
        AccessToken accessToken = null;
        SocialCredentialsSimple twAccount = null;

        try {
            System.out.println("------------> " + TwOauth.requestToken);
            accessToken = getTwitter().getOAuthAccessToken(TwOauth.requestToken, pin);
            System.out.println("------------> " + accessToken.getToken() + " " + accessToken.getScreenName() + " "
                    + accessToken.getTokenSecret());
            // save the user account
            //SocialAccountsTwitterDao.addAccount(profileId, accessToken.getToken(), accessToken.getTokenSecret(), accessToken.getScreenName());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return accessToken;
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
