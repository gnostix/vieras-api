package gr.gnostix.api.utilities;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.auth.oauth2.*;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.model.*;
import gr.gnostix.api.models.javaModels.GoogleAnalyticsProfilesJava;
import gr.gnostix.api.models.javaModels.GoogleAnalyticsTokens;
import gr.gnostix.api.models.plainModels.GoogleAnalyticsProfiles;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rebel on 18/2/15.
 */
public class GoogleAnalyticsAuth {

    private GoogleAuthorizationCodeFlow flow = null;
    private final static String CLIENT_ID = "332673681072-n2tlr81uuslailaecolv4nbhlv13ljjl.apps.googleusercontent.com";
    private final static String CLIENT_SECRET = "83cqMzOwjmtZ-Cn7UhxZayg7";
    private static final String CALLBACK_URL = "http://app.vieras.eu:8282/api/ga";
    private static JsonFactory JSON_FACTORY = new JacksonFactory();
    private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static String APPLICATION_NAME = "Gnostix";

    public GoogleAnalyticsTokens requestAccessToken(String code) {
        GoogleAnalyticsTokens tokens = new GoogleAnalyticsTokens();

        int status = 0;
        try {
            GoogleTokenResponse response =
                    new GoogleAuthorizationCodeTokenRequest(new NetHttpTransport(), new JacksonFactory(),
                            CLIENT_ID, CLIENT_SECRET,
                            code, CALLBACK_URL)
                            .execute();

            System.out.println("Access token: " + response.getAccessToken());
            System.out.println("Access refresh token: " + response.getRefreshToken());

            status = 200;
            tokens.setToken(response.getAccessToken());
            tokens.setRefreshToken(response.getRefreshToken());
            tokens.setStatus(status);

        } catch (TokenResponseException e) {
            if (e.getDetails() != null) {
                System.err.println("Error: " + e.getDetails().getError());
                if (e.getDetails().getErrorDescription() != null) {
                    System.err.println("Description: " + e.getDetails().getErrorDescription());
                }
                if (e.getDetails().getErrorUri() != null) {
                    System.err.println(e.getDetails().getErrorUri());
                }
            } else {
                System.err.println(e.getMessage());
            }

            status = 400;
            tokens.setStatus(status);

        } catch (Exception e) {
            status = 400;
            tokens.setStatus(status);
            e.printStackTrace();
        }
        return tokens;

    }

    public List<GoogleAnalyticsProfilesJava> getUserSitesToMonitor(String token, String refreshToken){
        List<GoogleAnalyticsProfilesJava> profList = new ArrayList<>();
        try {
            Analytics analytics = initializeAnalytics(token, refreshToken);
            profList = getAccounts(analytics);
        } catch (Exception e){
            e.printStackTrace();
        }

        return profList;
    }



    public List<GoogleAnalyticsProfilesJava> getAccounts(Analytics analytics) {
        Accounts accounts = null;
        List<GoogleAnalyticsProfilesJava> profList = new ArrayList<>();
        try {
            accounts = analytics.management().accounts().list().execute();
            for(Account account : accounts.getItems()){
                GoogleAnalyticsProfilesJava gaProfile = new GoogleAnalyticsProfilesJava();
                gaProfile.setAccountId(account.getId());

                System.out.println("account.getName: " + account.getName() + " account.getId: " + account.getId());
                Webproperties webproperties = analytics.management()
                        .webproperties().list(account.getId()).execute();

                if (webproperties.getItems().isEmpty()) {
                    System.err.println("No Webproperties found for account id: " + account.getId());
                } else {
                    for (Webproperty webProp : webproperties.getItems()) {
                        String webpropertyId = webProp.getId();
                        gaProfile.setWebpropertyId(webProp.getId());

                        System.out.println("WebpropertyId: " + webpropertyId);
                        // Query profiles collection.
                        Profiles profiles = analytics.management().profiles()
                                .list(account.getId(), webpropertyId).execute();
                        if (profiles.getItems() == null
                                || profiles.getItems().isEmpty()) {
                            System.err.println("No profiles found for webpropertyId: " + webpropertyId);
                        } else {
							for(Profile profile : profiles.getItems()){
                                gaProfile.setProfileid(profile.getId());
                                gaProfile.setProfileName(profile.getName());

                                System.err.println("ProfileId: " + profile.getId() + " profilename: " + profile.getName());
                            }
                        }
                    }
                }

                profList.add(gaProfile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        return profList;
    }



    private Analytics initializeAnalytics(String token, String refreshToken) throws Exception {

		/* get from DB the file with tokens and add to variables */
        GoogleCredential googleCredentials = createCredentialWithRefreshToken(
                HTTP_TRANSPORT, JSON_FACTORY,
                new TokenResponse().setRefreshToken(refreshToken)
                        .setAccessToken(token));

        // Set up and return Google Analytics API client.
        return new Analytics.Builder(HTTP_TRANSPORT, JSON_FACTORY,
                googleCredentials).setApplicationName(APPLICATION_NAME).build();
    }



    public static GoogleCredential createCredentialWithRefreshToken(
            HttpTransport transport, JsonFactory jsonFactory,
            TokenResponse tokenResponse) {
        return new GoogleCredential.Builder().setTransport(transport)
                .setJsonFactory(jsonFactory)
                .setClientSecrets(CLIENT_ID, CLIENT_SECRET)
                .build().setFromTokenResponse(tokenResponse);
    }
}
