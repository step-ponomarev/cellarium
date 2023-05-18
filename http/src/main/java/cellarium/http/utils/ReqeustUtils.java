package cellarium.http.utils;

import java.util.HashMap;
import java.util.Map;
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

    public static Map<String, String> extractQueryParams(Request request) {
        final Map<String, String> reqeustParams = new HashMap<>();
        for (Map.Entry<String, String> param : request.getParameters()) {
            reqeustParams.put(param.getKey(), param.getValue());
        }

        return reqeustParams;
    }
    
    private ReqeustUtils() {}
}
