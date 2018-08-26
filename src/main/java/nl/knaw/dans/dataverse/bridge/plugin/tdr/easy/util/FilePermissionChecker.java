package nl.knaw.dans.dataverse.bridge.plugin.tdr.easy.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/*
 * @author: Eko Indarto
 */
public class FilePermissionChecker {
    private static final Logger LOG = LoggerFactory.getLogger(FilePermissionChecker.class);

    public static PermissionStatus check(String url) {
        URL validUrl;
        try {
            validUrl = new URL(url);
            HttpURLConnection huc = (HttpURLConnection) validUrl.openConnection();
            int rc = huc.getResponseCode();
            if (rc == HttpURLConnection.HTTP_OK)
                return PermissionStatus.OK;
            else if (rc == HttpURLConnection.HTTP_FORBIDDEN)
                return PermissionStatus.RESTRICTED;
            else
                LOG.error(url + " response gives status other 200 (HTTP_OK). Response code: " + rc);
        } catch (MalformedURLException e) {
            LOG.error("MalformedURLException, message: " + e.getMessage());
        } catch (IOException e) {
            LOG.error("IOException, message: " + e.getMessage());
        }
        return PermissionStatus.OTHER;
    }

    public enum PermissionStatus {
        OTHER,
        OK,
        RESTRICTED;

        public String toString() {
            switch (this) {
                case OK:
                    return "OK";
                case RESTRICTED:
                    return "RESTRICTED";
            }
            return "OTHER";//default
        }

    }

}
