package com.baidu.unbiz.flume.sink;

import java.lang.management.ManagementFactory;

public class SystemPropertiesUtil {

    public static String getSystemProperty(String key, String defautValue) {
        String value = System.getProperty(key);
        if (value == null || value.length() == 0) {
            value = System.getenv(key);
            if (value == null || value.length() == 0) {
                value = defautValue;
            }
        }

        return value;
    }

    public static String getSystemProperty(String dKey, String shellKey, String defautValue) {
        String value = System.getProperty(dKey);
        if (value == null || value.length() == 0) {
            value = System.getenv(shellKey);
            if (value == null || value.length() == 0) {
                value = defautValue;
            }
        }

        return value;
    }

    public static boolean isDebug() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;
    }
}

