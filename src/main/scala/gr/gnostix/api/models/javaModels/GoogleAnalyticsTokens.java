package gr.gnostix.api.models.javaModels;

/**
 * Created by rebel on 20/2/15.
 */
public class GoogleAnalyticsTokens {
    private String token;
    private String refreshToken;
    private int status;


    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "GoogleAnalyticsTokens{" +
                "token='" + token + '\'' +
                ", refreshToken='" + refreshToken + '\'' +
                ", status=" + status +
                '}';
    }
}
