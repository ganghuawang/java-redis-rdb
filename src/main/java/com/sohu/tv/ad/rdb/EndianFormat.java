/**
 * Copyright (c) 2013 Sohu TV
 * All rights reserved.
 */
package com.sohu.tv.ad.rdb;

/**
 * 
 * 二进制数字的高低位转换
 * @author Wang GangHua
 * @version 1.0.0 2013-11-30
 *
 */
public class EndianFormat {
    /*
     * 对16位数据进行高低位切换,低位转为高位
     */
    public static void memrev16(byte[] p, int start) {
        byte[] x = p;
        byte t;

        t = x[start];
        x[start] = x[start + 1];
        x[start + 1] = t;
    }

    /*
     * 对32位数据进行高低位切换,低位转为高位
     */
    public static void memrev32(byte[] p, int start) {
        byte[] x = p;
        byte t;

        t = x[start];
        x[start] = x[start + 3];
        x[start + 3] = t;
        t = x[start + 1];
        x[start + 1] = x[start + 2];
        x[start + 2] = t;
    }

    /*
     * 对64位数据进行高低位切换,低位转为高位
     */
    public static void memrev64(byte[] p, int start) {
        byte[] x = p;
        byte t;

        t = x[start];
        x[start] = x[start + 7];
        x[start + 7] = t;
        t = x[start + 1];
        x[start + 1] = x[start + 6];
        x[start + 6] = t;
        t = x[start + 2];
        x[start + 2] = x[start + 5];
        x[start + 5] = t;
        t = x[start + 3];
        x[start + 3] = x[start + 4];
        x[start + 4] = t;
    }
}
