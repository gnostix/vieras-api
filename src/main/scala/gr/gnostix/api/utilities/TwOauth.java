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

    public TwOauth() {
        setTwitter();
        setRequestToken();
    }

    private static final String TwitterConsumerKey = "lJ43ewh5JHU5wehbfOgDrw";
    private static final String TwitterConsumerSecret = "i9FRRM5JUjLm3amoYyPjCc4dVx50U21eSaUghYGaJI0";
    private RequestToken requestToken = null;
    private Twitter twitter = null;

    public AccessToken getUserToken(String pin, int profileId) {
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

    public void setRequestToken() {
        try {
            requestToken = twitter.getOAuthRequestToken();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public RequestToken getRequestToken() {
        return this.requestToken;
    }

    public void setTwitter() {
        try {
            //twitter = TwitterFactory.getSingleton();
            twitter = new TwitterFactory().getInstance();
            twitter.setOAuthConsumer(TwitterConsumerKey, TwitterConsumerSecret);

            System.out.println("------------> getTwitter TwitterFactory().getInstance " + twitter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Twitter getTwitter() {
        return this.twitter;
    }


}
