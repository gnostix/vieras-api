package gr.gnostix.api.models.javaModels;


import java.io.Serializable;

/**
 * Created by rebel on 20/2/15.
 */
public class GoogleAnalyticsProfilesJava implements Serializable {

    private String accountId;
    private String webpropertyId;
    private String profileid;
    private String profileName;
    private static final long serialVersionUID = 7526471155622776147L;


    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getWebpropertyId() {
        return webpropertyId;
    }

    public void setWebpropertyId(String webpropertyId) {
        this.webpropertyId = webpropertyId;
    }

    public String getProfileid() {
        return profileid;
    }

    public void setProfileid(String profileid) {
        this.profileid = profileid;
    }

    public String getProfileName() {
        return profileName;
    }

    public void setProfileName(String profileName) {
        this.profileName = profileName;
    }

    @Override
    public String toString() {
        return "GoogleAnalyticsProfiles{" +
                "accountId='" + accountId + '\'' +
                ", webpropertyId='" + webpropertyId + '\'' +
                ", profileid='" + profileid + '\'' +
                ", profileName='" + profileName + '\'' +
                '}';
    }
}
