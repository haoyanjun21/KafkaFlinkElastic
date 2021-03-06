package com.x.flink.util;

import java.math.BigInteger;
import java.security.MessageDigest;

public class MD5 {

    public static String md(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(str.getBytes());
            return new BigInteger(1, md.digest()).toString(16);
        } catch (Exception e) {
            e.printStackTrace();
            return str;
        }
    }

    public static void main(String[] args) {
        System.out.println(md("1545906589000gwFfunctionId1android1.0.0"));
        System.out.println(md("1545906589000gwFfunctionId1android1.0.0"));
        System.out.println(md("1"));
        System.out.println(md("1"));
    }

}