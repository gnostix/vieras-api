package gr.gnostix.api.utilities;

import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Created by rebel on 18/2/15.
 */
public class GoogleAnalyticsAuth {

    final static String CLIENT_SECRETS = "client_secrets.json";
    private static GoogleAuthorizationCodeFlow flow = null;
    private final static String CLIENT_ID = "332673681072-n2tlr81uuslailaecolv4nbhlv13ljjl.apps.googleusercontent.com";
    private final static String CLIENT_SECRET = "83cqMzOwjmtZ-Cn7UhxZayg7";
    private static final String CALLBACK_URL = "http://app.vieras.eu:8080/api/ga";
    private static final String BASE_URL = "https://accounts.google.com/o/oauth2/auth?";
    private static final String BASE_URL_TOKEN = "https://accounts.google.com/o/oauth2/token";
    private static final String ERROR_ACCESS = "access_denied";

    /**
     * create the url and get the token from google api
     *
     * @return
     */
    public boolean getGaAuthToken(String code) {
        boolean itWorks = false;
        String tokenUrl = new String(BASE_URL_TOKEN);

        try {
            StringBuffer params = getUrlParameters(code);

            // Send data
            URL url = new URL(tokenUrl.toString());
            HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");
            conn.setRequestProperty("Content-Length",
                    String.valueOf(params.toString().length()));

            OutputStreamWriter wr = new OutputStreamWriter(
                    conn.getOutputStream());
            wr.write(params.toString());
            wr.flush();

            InputStream is;
            if (conn.getResponseCode() == 200) {
                is = conn.getInputStream();
                itWorks = true;
				/* Read the response in success */
                readAuthUrlSuccess(wr, is);
            } else {
                is = conn.getErrorStream();
                readAuthUrlError(wr, is);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return itWorks;
    }

    /**
     * Create the url parameters for our POST request
     *
     * @return
     * @throws UnsupportedEncodingException
     */
    public StringBuffer getUrlParameters(String code) throws UnsupportedEncodingException {

        StringBuffer params = new StringBuffer("");
        try {
            params.append("code=" + URLEncoder.encode(code, "UTF-8"));
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        }
        params.append("&client_id=" + CLIENT_ID);
        params.append("&client_secret=" + CLIENT_SECRET);
        params.append("&redirect_uri=" + CALLBACK_URL);
        params.append("&grant_type=authorization_code");

        return params;

    }


    public void readAuthUrlSuccess(OutputStreamWriter wr, InputStream is) {
        // Get the response
        BufferedReader rd = new BufferedReader(new InputStreamReader(is));
        String line;
        String authTokenFileJson = new String("");
        String startOfAuthTokenFileJson = "{\"credentials\":{\"user\":";
        String endOfAuthTokenFileJson = "}}";
        try {
            //open the file properly
            authTokenFileJson = authTokenFileJson + startOfAuthTokenFileJson;
            System.out.println(authTokenFileJson);
            while ((line = rd.readLine()) != null) {
                System.out.println("-----" + line);
                authTokenFileJson = authTokenFileJson + line;
            }
            // close the file properly
            authTokenFileJson = authTokenFileJson + endOfAuthTokenFileJson;

            System.out.println(authTokenFileJson.toString());
            System.out.println("*** Auth token file ***");

            wr.close();
            rd.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void readAuthUrlError(OutputStreamWriter wr, InputStream is) {
        // Get the response
        BufferedReader rd = new BufferedReader(new InputStreamReader(is));
        String line;
        StringBuffer authTokenFileJson = new StringBuffer("");
        try {
            while ((line = rd.readLine()) != null) {
                System.out.println("-----" + line);
                authTokenFileJson.append(line);
            }

            if (authTokenFileJson.toString().contains(ERROR_ACCESS)) {
                System.out.println("---------> ACCESS_DENIED_MSG");
            }
            System.out.println("*** Auth token file ***");
            wr.close();
            rd.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


//    /**
//     * Build an authorization flow and store it as a static class attribute.
//     *
//     * @return GoogleAuthorizationCodeFlow instance.
//     * @throws IOException Unable to load client_secrets.json.
//     */
//    GoogleAuthorizationCodeFlow getFlow() throws IOException {
//        if (flow == null) {
//            ClassLoader classLoader = getClass().getClassLoader();
//            File file = new File(classLoader.getResource(WEB_CLIENT_SECRETS).getFile());
//            Reader reader = new FileReader(file);
//            HttpTransport httpTransport = new NetHttpTransport();
//            JacksonFactory jsonFactory = new JacksonFactory();
//            GoogleClientSecrets clientSecrets =
//                    GoogleClientSecrets.load(jsonFactory,reader);
//            flow =
//                    new GoogleAuthorizationCodeFlow.Builder(httpTransport, jsonFactory, clientSecrets, SCOPES)
//                            .setAccessType("offline").setApprovalPrompt("force").build();
//        }
//        return flow;
//    }

    public static int requestAccessToken(String code) throws IOException {
        int status = 0;
        try {
            GoogleTokenResponse response =
                    new GoogleAuthorizationCodeTokenRequest(new NetHttpTransport(), new JacksonFactory(),
                            CLIENT_ID, CLIENT_SECRET,
                            code, "http://app.vieras.eu:8080/api/ga")
                            .execute();

            System.out.println("Access token: " + response.getAccessToken());
            System.out.println("Access refresh token: " + response.getRefreshToken());

            status = 200;

        } catch (TokenResponseException e) {
            if (e.getDetails() != null) {
                System.err.println("Error: " + e.getDetails().getError());
                System.err.println("Error: " + e.getDetails().toPrettyString());
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
        }
        return status;

    }

    public void printCode(String code){
        System.out.println("-------------> CODE: " +code );
    }



}
