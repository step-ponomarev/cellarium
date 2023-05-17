package cellarium.http.utils;

import one.nio.http.Request;

public class ReqeustUtils {
     public static String getHeader(Request request, String key) {
        final int keyLength = key.length();

        final int headerCount = request.getHeaderCount();
        final String[] headers = request.getHeaders();
        for (int i = 1; i < headerCount; i++) {
            if (headers[i].regionMatches(true, 0, key, 0, keyLength)) {
                return headers[i].split(":")[1].trim();
            }
        }

        return null;
    }
    
    private ReqeustUtils() {}
}
