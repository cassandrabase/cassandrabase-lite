package io.github.cassandrabase.lite.util;

import org.apache.commons.codec.digest.DigestUtils;

public class HashGen {
    public static String generateHash(String input, ALGType algType) {
        if (algType.equals(ALGType.MD5)) {
            return DigestUtils.md5Hex(input);
        } else if (algType.equals(ALGType.SHA256)) {
            return DigestUtils.sha256Hex(input);
        } else {
            throw new RuntimeException("Invalid alg type : " + algType);
        }
    }
    public enum ALGType {
        SHA256,
        MD5
    }
}
