package Utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Created by autoopt/rohit.aga.
 */
public class Utils {
    public static String cleanText(String s) {
        s = s.replaceAll("\\n", " ");
        s = s.replaceAll("\\t", " ");
        s = StringEscapeUtils.unescapeHtml4(s).trim().replaceAll("\\s+", " ");
        s = StringEscapeUtils.unescapeHtml3(s).trim().replaceAll("\\s+", " ");
        s = org.apache.commons.lang.StringEscapeUtils.unescapeHtml(s).trim().replaceAll("\\s+", " ");
        s = s.replaceAll("\\<.*?>", "");
        return s;
    }

    public static String cleanAdUrl(String url) {
        String[] urlParts = url.split("/");
        String domainPartOfURL = urlParts[0];
        if (domainPartOfURL.startsWith("www."))
            domainPartOfURL = domainPartOfURL.replaceFirst("www\\.", "");
        domainPartOfURL = domainPartOfURL.replaceAll("\\.", " ").replaceAll("\\s+", " ");
        domainPartOfURL = domainPartOfURL.replaceAll("[~`!@#\\$%\\^&\\*\\(\\)_\\+\\{\\}\\|:<>\\?\\-=\\[\\]\\\\;',\\.]", " ");
        String secondPart = "";
        for (int j = 1; j < urlParts.length; j++)
            secondPart += urlParts[j].replaceAll("[~`!@#\\$%\\^&\\*\\(\\)_\\+\\{\\}\\|:<>\\?\\-=\\[\\]\\\\;',\\.]", " ") + " ";
        return domainPartOfURL.trim() + " " + secondPart.trim();
    }

    public static String getMD5Hex(String str) {
        return DigestUtils.md5Hex(str);
    }

}
