package io.github.kamenriderkuuga.ojson.utils;

import java.util.concurrent.ThreadLocalRandom;

public class RandomUtils {

    private static final String alphabetic = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    public static String nextRandomRandomAlphabetic(int length) {
        var buf = new char[length];
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = alphabetic.charAt(ThreadLocalRandom.current().nextInt(alphabetic.length()));
        return new String(buf);
    }
}
