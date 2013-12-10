/**
 * Copyright (c) 2013 Sohu TV
 * All rights reserved.
 */
package com.sohu.tv.ad.rdb;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * 
 * 解析ZipMap类型数据
*  @author Wang GangHua
 * @version 1.0.0 2013-11-30
 *
 */
public class ZipMap {
    public static final int ZIPMAP_BIGLEN=254;	//zipMap起始符
    public static final int ZIPMAP_END=255;	//zipMap结束符
    public static final int ZIPMAP_VALUE_MAX_FREE=4;

    /* 
	 * 占1或5个字节
	 * 如果第一个字节整型值等于254,则后面的4个字节表示长度,否则第一个字节整型值就是长度
     * */
	static int decodeFlagLen(int _l) {
		if (_l < ZIPMAP_BIGLEN) {
			return 1;
		} else {
			return 5;
		}
	}
    /* 创建一个空的zipMap */
    static ByteBuffer zipmapNew() {
        byte[] zm = new byte[2];
        zm[0] = 0; /* Length */
        zm[1] = (byte) ZIPMAP_END;
        return ByteBuffer.wrap(zm);
    }
    
    /*
	  * zipMap占用的字节数
	  * */
	static int zipmapDecodeLength(byte[] p, int start) {
		int len = p[start];
		if (len < ZIPMAP_BIGLEN) {
			return len;
		}
		len = ((p[start + 4] & 0x00ff) << 24) + ((p[start + 3] & 0x00ff) << 16)
				+ ((p[start + 2] & 0x00ff) << 8) + ((p[start + 1] & 0x00ff));
		return len;
	}
    
	static int zipmapEncodeLength(byte[] p, int len) {
		if (p == null) {
			return decodeFlagLen(len);
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
				return 1 + 4;
			}
		}
	}
    
    /*
     *  byte数组转为返回的HashMap
     * */
	public static HashMap<String, String> zipmapExpand(byte[] zm) {
		byte[] p = zm;
		int l, llen;

		int pos = 1;
		HashMap<String, String> res = new HashMap<String, String>();
		while ((0x00ff & p[pos]) != ZIPMAP_END) {
			int free;
			l = zipmapDecodeLength(p, pos);
			llen = zipmapEncodeLength(null, l);
			pos += llen;
			String key = new String(zm, pos, l);
			pos += l;
			l = zipmapDecodeLength(p, pos);
			pos += zipmapEncodeLength(null, l);
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
