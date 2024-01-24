package cl.fala.procedures.utilities;

import java.util.Base64;

public class Parsing {
    public static String decodeBase64(String encodedString) {
        byte[] decodedBytes = Base64.getDecoder().decode(encodedString);
        return new String(decodedBytes);
    }

    public static String encodeToBase64(String str) {
        return Base64.getEncoder().encodeToString(str.getBytes());
    }
}
