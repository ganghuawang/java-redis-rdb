package com.sohu.tv.ad.rdb;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * 
 * decode the ZipMap type value
 * @author ganghuawang
 *
 */
public class ZipMap {
    public static final int ZIPMAP_BIGLEN=254;
    public static final int ZIPMAP_END=255;

    /* The following defines the max value for the <free> field described in the
     * comments above, that is, the max number of trailing bytes in a value. */
    public static final int ZIPMAP_VALUE_MAX_FREE=4;

    /* The following macro returns the number of bytes needed to encode the length
     * for the integer value _l, that is, 1 byte for lengths < ZIPMAP_BIGLEN and
     * 5 bytes for all the other lengths. */
    static int ZIPMAP_LEN_BYTES(int _l){
        if(_l < ZIPMAP_BIGLEN)
            return 1;
        else
            return 4+1;
    }
    /* Create a new empty zipmap. */
    static ByteBuffer zipmapNew() {
        byte[] zm = new byte[2];
        zm[0] = 0; /* Length */
        zm[1] = (byte) ZIPMAP_END;
        return ByteBuffer.wrap(zm);
    }
    
    /* Decode the encoded length pointed by 'p' */
    static int zipmapDecodeLength(byte[] p, int start) {
        int len = p[start];
        if (len < ZIPMAP_BIGLEN) return len;
        len = ((p[start+4] & 0x00ff) << 24) + ((p[start+3] & 0x00ff) << 16)
                + ((p[start+2] & 0x00ff) << 8) + ((p[start+1] & 0x00ff));
        return len;
    }
    
    /* Encode the length 'l' writing it in 'p'. If p is NULL it just returns
     * the amount of bytes required to encode such a length. */
    static int zipmapEncodeLength(byte[] p, int len) {
        if (p == null) {
            return ZIPMAP_LEN_BYTES(len);
        } else {
            if (len < ZIPMAP_BIGLEN) {
                p[0] = (byte) len;
                return 1;
            } else {
                p[0] = (byte) ZIPMAP_BIGLEN;
                p[1] = (byte) (0x00ff & len);
                p[2] = (byte) ((0x00ff & len) >> 8);
                p[3] = (byte) ((0x00ff & len) >> 16);
                p[4] = (byte) ((0x00ff & len) >> 24);
                return 1+4;
            }
        }
    }
    
    public static HashMap<String, String> zipmapExpand(byte[] zm) {
        byte[] p = zm;
        int l,llen;
        
        int pos = 1;
        HashMap<String, String> res = new  HashMap<String, String>();
        while((0x00ff & p[pos]) != ZIPMAP_END) {
            int free;
            l = zipmapDecodeLength(p, pos);
            llen = zipmapEncodeLength(null,l);
            pos += llen;
            String key = new String(zm, pos, l);
            pos += l;
            l = zipmapDecodeLength(p, pos);
            pos += zipmapEncodeLength(null,l);
            free = (0x00ff & p[pos]);
            pos += 1;
            String value = new String(zm, pos, l);
            pos += l;
            pos += free;
            res.put(key, value);
        }
        return res;
    }
}
