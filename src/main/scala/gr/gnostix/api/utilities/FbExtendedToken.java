package gr.gnostix.api.utilities;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.types.Page;
import gr.gnostix.api.models.FacebookPage;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rebel on 21/10/14.
 */
public class FbExtendedToken {

    public static final String MY_APP_ID = "175296422520925";
    public static final String MY_APP_SECRET = "c01c2bf58d8a1535d949568e8603c518";

    public static FacebookClient.AccessToken getExtendedToken(String accessToken) {
        // Tells Facebook to extend the lifetime of MY_ACCESS_TOKEN.
        // Facebook may return the same token or a new one.
        FacebookClient.AccessToken accessTokenExtended = null;
        try {
            accessTokenExtended =
                    new DefaultFacebookClient().obtainExtendedAccessToken(MY_APP_ID,
                            MY_APP_SECRET, accessToken);

            System.out.println("My access token: " + accessToken);
            System.out.println("My extended access token: " + accessTokenExtended.getAccessToken());
            System.out.println("My extended access token expires: " + accessTokenExtended.getExpires());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return accessTokenExtended;
    }

    public static List<FacebookPage> getUserPages(String token) {

        FacebookClient client = new DefaultFacebookClient(token);

        Connection<Page> connection = client.fetchConnection("/me/accounts", Page.class);
        System.out.println("------------> GET PAGES " + connection.getData().size());

        List<FacebookPage> pagenames = new ArrayList<>();
        for (Page p : connection.getData()) {
            pagenames.add(new FacebookPage(p.getName(), p.getId()));
            System.out.println("------------> " + p.getName());
            System.out.println("------------> " + p.getUsername());

        }
        return pagenames;
    }

}
