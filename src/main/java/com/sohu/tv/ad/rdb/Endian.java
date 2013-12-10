package com.sohu.tv.ad.rdb;

public class Endian {
    /*
     * Toggle the 16 bit unsigned integer pointed by *p from little endian to
     * big endian
     */
    public static void memrev16(byte[] p, int start) {
        byte[] x = p;
        byte t;

        t = x[start];
        x[start] = x[start + 1];
        x[start + 1] = t;
    }

    /*
     * Toggle the 32 bit unsigned integer pointed by *p from little endian to
     * big endian
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
     * Toggle the 64 bit unsigned integer pointed by *p from little endian to
     * big endian
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
