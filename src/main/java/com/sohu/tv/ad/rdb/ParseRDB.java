/**
 * Copyright (c) 2013 Sohu TV
 * All rights reserved.
 */
package com.sohu.tv.ad.rdb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

/**
 * 
 * 解析redis的dump.rdb文件, 可以支持最新的RDB第6版本
 * RBD文件格式可以参照 : https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
 * 
 * @author Wang GangHua
 * @version 1.0.0 2013-11-30
 */
public class ParseRDB {
	
    /* Redis数据类型 */
    public static final int REDIS_STRING = 0;

    public static final int REDIS_LIST = 1;

    public static final int REDIS_SET = 2;

    public static final int REDIS_ZSET = 3;

    public static final int REDIS_HASH = 4;

    public static final int REDIS_HASH_ZIPMAP = 9;

    public static final int REDIS_LIST_ZIPLIST = 10;

    public static final int REDIS_SET_INTSET = 11;

    public static final int REDIS_ZSET_ZIPLIST = 12;
    
    public static final int REDIS_HASH_ZIPLIST = 13;

    
    public static final int REDIS_ENCODING_RAW = 0; /* Raw representation */

    public static final int REDIS_ENCODING_INT = 1; /* Encoded as integer */

    public static final int REDIS_ENCODING_ZIPMAP = 2; /* zipmap编码方式 */

    public static final int REDIS_ENCODING_HT = 3; /* hash table编码方式 */

    /* Redis的特殊标示 */
    public static final int REDIS_EXPIRETIME_FC = 252; /* 毫秒级过期时间,占用8个字节 */
    
    public static final int REDIS_EXPIRETIME_FD = 253;	/* 秒级过期时间 ,占用8个字节 */

    public static final int REDIS_SELECTDB = 254;	/* 数据库 (后面紧接着的就是数据库编号) */

    public static final int REDIS_EOF = 255;	/* 结束符 */

    /*
     * 表示长度的方式
     */
    public static final int REDIS_RDB_6BITLEN = 0;
    public static final int REDIS_RDB_14BITLEN = 1;
    public static final int REDIS_RDB_32BITLEN = 2;
    public static final int REDIS_RDB_ENCVAL = 3;

    public static final long REDIS_RDB_LENERR = Long.MAX_VALUE;
    
    /*
     * 整型数字的编码方式
     */
    public static final int REDIS_RDB_ENC_INT8 = 0; /* 8 bit signed integer */

    public static final int REDIS_RDB_ENC_INT16 = 1; /* 16 bit signed integer */

    public static final int REDIS_RDB_ENC_INT32 = 2; /* 32 bit signed integer */

    public static final int REDIS_RDB_ENC_LZF = 3; /* string compressed with FASTLZ
                                                    */

    private static void ERROR(String msg, Object... args) {
        throw new RuntimeException(String.format(msg, args));
    }

    RandomAccessFile position = null;	//二进制方式读取RDB文件

    /*
     * 对Redis数据的封装
     */
    public static class Entry {
        public String key;
        public Object value;
        int type;	/* redis数据类型 */
        byte success;
        public int expire; /* 过期时间 , milliseconds*/
    };

    
    private static byte[] chars2bytes(String str) {
        try {
            return str.getBytes("ASCII");
        } catch (UnsupportedEncodingException e) {
            return str.getBytes();
        }
    }
    
    /* Redis文件头部，必须以"REDIS"字符串开头 **/
    private boolean processHeader() {
        byte[] buf = new byte[9];
        int dump_version;
        if (!readBytes(buf, 0, 9)) {
            ERROR("Cannot read header\n");
        }
        /* expect the first 5 bytes to equal REDIS */
        if (memcmp(buf, 0, chars2bytes("REDIS"), 0, 5) != 0) {
            ERROR("Wrong signature in header\n");
        }
        dump_version = (int) strtol(buf, 5, 10);
        if (dump_version < 1 || dump_version > 6) {
            ERROR("Unknown RDB format version: %d\n", dump_version);
        }
        return true;
    }

    private static long strtol(byte[] buf, int start, int radix) {
        int len = start;
        for (; len < buf.length; len++) {
            if (buf[len] == 0x00)
                break;
        }
        char[] c = new char[len - start];
        for (int i = start; i < buf.length; i++) {
            c[i - start] = (char) (0x000000ff & buf[i]);
        }
        String str = new String(c);
        return Long.parseLong(str, radix);
    }
    
    private boolean readBytes(byte[] buf, int start, int num) {
        RandomAccessFile p = position;
        boolean peek = (num < 0) ? true : false;
        num = (num < 0) ? -num : num;
        try {
            if (p.getFilePointer() + num > p.length()) {
                return false;
            } else {
                p.readFully(buf, start, num);
                if (peek) {
                    p.seek(p.getFilePointer() - num);
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 把两个byte数组进行对比，判断是否相等
     * @param buf1
     * @param start1
     * @param buf2
     * @param start2
     * @param len 对比的长度
     * @return
     */
    private static int memcmp(byte[] buf1, int start1, byte[] buf2, int start2,
            int len) {
        int pos1 = start1;
        int pos2 = start2;
        for (int i = 0; i < len; i++) {
            pos1 = start1 + i;
            pos2 = start2 + i;
            if (buf1.length <= pos1 && buf2.length <= pos2)
                return 0;
            if (buf1.length <= pos1)
                return -1;
            if (buf2.length <= pos2)
                return 1;
            if (buf1[pos1] != buf2[pos2])
                return buf1[pos1] > buf2[pos2] ? 1 : -1;
        }
        return 0;
    }
    
    /* 解析数据类型，占用一个字节  **/
    private boolean loadType(Entry e) {
        /* this byte needs to qualify as type */
        byte[] tt = new byte[1];
        if (readBytes(tt, 0, 1)) {
            int t = (0x00ff & tt[0]);
            if (t <= 4 || (t >= 9 && t <= 13) || t >= 252) {
                e.type = t;
                return true;
            } else {
                ERROR("Unknown type (0x%02x)", t);
            }
        } else {
            ERROR("Could not read type");
        }
        return false;
    }

    private int peekType() {
        byte[] tt = new byte[1];
        if (readBytes(tt, 0, -1)) {
            int t = (0x00ff & tt[0]);
            if (t <= 4 || (t >= 9 && t <= 13) || t >= 252){
                return t;
            }
        }
        return -1;
    }

    /* 获取过期时间, byte数组需转换为Long */
    byte[] processTime(int type) {
    	int timelen = (type == REDIS_EXPIRETIME_FC) ? 8 : 4;
        byte[] t = new byte[8];
        if (readBytes(t, 0, length)) {
            return t;
        } else {
            ERROR("Could not read time");
        }
        /* failure */
        return null;
    }

    static class Pointer<T> {
        T key;

        T get() {
            return key;
        }

        void set(T t) {
            this.key = t;
        }
    }

    private long ntohl(byte[] buf) {
        return ((buf[3] & 0x00ff) << 24) + ((buf[2] & 0x00ff) << 16)
                + ((buf[1] & 0x00ff) << 8) + ((buf[0] & 0x00ff));
    }

    /* 解析第一个字节，返回值表示此段数据占用字节的长度 **/
    private long loadLength(Pointer<Boolean> isencoded) {
        byte[] buf = new byte[2];
        int type;

        if (isencoded != null)
            isencoded.set(false);
        if (!readBytes(buf, 0, 1))
            return REDIS_RDB_LENERR;
        type = (buf[0] & 0x00C0) >> 6;
        if (type == REDIS_RDB_6BITLEN) {
            /* Read a 6 bit len */
            return buf[0] & 0x003F;
        } else if (type == REDIS_RDB_ENCVAL) {
            /* Read a 6 bit len encoding type */
            if (isencoded != null)
                isencoded.set(true);
            return buf[0] & 0x003F;
        } else if (type == REDIS_RDB_14BITLEN) {
            /* Read a 14 bit len */
            if (!readBytes(buf, 1, 1))
                return REDIS_RDB_LENERR;
            return ((buf[0] & 0x003F) << 8) | (buf[1] & 0x00ff);
        } else {
            /* Read a 32 bit len */
            buf = new byte[4];
            if (!readBytes(buf, 0, 4))
                return REDIS_RDB_LENERR;
            return ntohl(buf);
        }
    }
    
    /* 解析一个整型数据  **/
    String loadIntegerObject(int enctype) {
        byte[] enc = new byte[4];
        long val;
        if (enctype == REDIS_RDB_ENC_INT8) {
            if (!readBytes(enc, 0, 1))
                return null;
            val = (0x00ff & enc[0]);
        } else if (enctype == REDIS_RDB_ENC_INT16) {
            if (!readBytes(enc, 0, 2))
                return null;
            val = ((enc[1] & 0x00ff) << 8) | (enc[0] & 0x00ff);
        } else if (enctype == REDIS_RDB_ENC_INT32) {
            if (!readBytes(enc, 0, 4))
                return null;
            val = ((enc[3] & 0x00ff) << 24) + ((enc[2] & 0x00ff) << 16)
                    + ((enc[1] & 0x00ff) << 8) + ((enc[0] & 0x00ff));
        } else {
            ERROR("Unknown integer encoding (0x%02x)", enctype);
            return null;
        }
        return String.valueOf(val);
    }

    String loadLzfStringObject() {
        byte[] s = loadLzfStringObjectBytes();
        try {
            return new String(s, "ASCII");
        } catch (UnsupportedEncodingException e) {
            return new String(s);
        }
    }
    
    byte[] loadLzfStringObjectBytes() {
        long slen, clen;
        if ((clen = loadLength(null)) == REDIS_RDB_LENERR)
            return null;
        if ((slen = loadLength(null)) == REDIS_RDB_LENERR)
            return null;

        byte[] c = new byte[(int) clen];

        if (!readBytes(c, 0, (int) clen)) {
            return null;
        }

        byte[] s = new byte[(int) slen];
        LZFCompress.expand(c, 0, (int) clen, s, 0, (int) slen);
        return s;
    }

    /* returns NULL when not processable, char* when valid */
    String loadStringObject() {
        Pointer<Boolean> isencoded = new Pointer<Boolean>();
        long len;

        len = loadLength(isencoded);
        if (isencoded.get()) {
            switch ((int) len) {
                case REDIS_RDB_ENC_INT8:
                case REDIS_RDB_ENC_INT16:
                case REDIS_RDB_ENC_INT32:
                    return loadIntegerObject((int) len);
                case REDIS_RDB_ENC_LZF:
                    return loadLzfStringObject();
                default:
                    /* unknown encoding */
                    ERROR("Unknown string encoding (0x%02x)", len);
                    return null;
            }
        }

        if (len == REDIS_RDB_LENERR)
            return null;

        byte[] buf = new byte[(int) len];
        if (!readBytes(buf, 0, (int) len)) {
            return null;
        }
        try {
            return new String(buf, "ASCII");
        } catch (UnsupportedEncodingException e) {
            return new String(buf);
        }
    }
    
    byte[] loadStringObjectBytes() {
        Pointer<Boolean> isencoded = new Pointer<Boolean>();
        long len;

        len = loadLength(isencoded);
        if (isencoded.get()) {
            switch ((int) len) {
                case REDIS_RDB_ENC_LZF:
                    return loadLzfStringObjectBytes();
                default:
                    /* unknown encoding */
                    ERROR("Unknown string encoding (0x%02x)", len);
                    return null;
            }
        }

        if (len == REDIS_RDB_LENERR)
            return null;

        byte[] buf = new byte[(int) len];
        if (!readBytes(buf, 0, (int) len)) {
            return null;
        }
        return buf;
    }

    /* double数据 **/
    Double loadDoubleValue() {
        byte[] buf = new byte[256];
        byte[] lenArray = new byte[1];
        Double val;

        if (!readBytes(lenArray, 0, 1))
            return null;

        int len = (0x00ff & lenArray[0]);
        switch (len) {
            case 255:
                val = Double.NEGATIVE_INFINITY;
                return val;
            case 254:
                val = Double.POSITIVE_INFINITY;
                return val;
            case 253:
                val = Double.NaN;
                return val;
            default:
                if (!readBytes(buf, 0, len)) {
                    return null;
                }
                buf[len] = '\0';
                String str = "";
                try {
                    str = new String(buf, 0, len, "ASCII");
                } catch (UnsupportedEncodingException e) {
                    str = new String(buf, 0, len);
                }
                return Double.parseDouble(str);
        }
    }

    /*
     * 根据RDB中存储的Redis类型, 解析为相对应的Java数据类型
     * */
    boolean loadPair(Entry e) {
        int i;
        /* read key first */
        String key = loadStringObject();
        if (key != null) {
            e.key = key;
        } else {
            ERROR("Error reading entry key");
            return false;
        }

        long length = 0;
        if (e.type == REDIS_LIST || e.type == REDIS_SET || e.type == REDIS_ZSET
                || e.type == REDIS_HASH) {
            if ((length = loadLength(null)) == REDIS_RDB_LENERR) {
                ERROR("Error reading %d length", e.type);
                return false;
            }
        }

        switch (e.type) {
            case REDIS_STRING:
                e.value = loadStringObject();
                if (e.value == null) {
                    ERROR("Error reading entry value");
                    return false;
                }
                break;
            case REDIS_HASH_ZIPMAP:
                byte[] hashZipValue = loadStringObjectBytes();
                e.value = ZipMap.zipmapExpand(hashZipValue);
                if (e.value == null) {
                    ERROR("Error reading entry value");
                    return false;
                }
                break;
            case REDIS_LIST_ZIPLIST:
            	List<String> lists = new ArrayList<String>();
            	ZipList zipList = new ZipList(loadStringObjectBytes());
            	int entryCountList = zipList.decodeEntryCount();
            	
            	for (int j = 0; j < entryCountList; j++) {
            		if(zipList.getEndByte() == ZipList.ZIPLIST_END){	// 0xff为ziplist的结束符
            			break;
            		}
            		// value
            		lists.add(zipList.decodeEntryValue());
				}
            	e.value = lists;
            	break;
            case REDIS_SET_INTSET:
            case REDIS_ZSET_ZIPLIST:
            	//TODO
            	throw new UnsupportedOperationException("Sorry, 暂时还不支持!");
            case REDIS_HASH_ZIPLIST:
            	HashMap<String, String> hashmapValues = new HashMap<String, String>();
            	/* 将整个Hashmap in Ziplist的内容以byte数组读出来，再进行解析 */
            	ZipList zipLit = new ZipList(loadStringObjectBytes());
            	int entryCount = zipLit.decodeEntryCount();
            	
            	for (int j = 0; j < entryCount/2; j++) {
            		if(zipLit.getEndByte() == ZipList.ZIPLIST_END){	// 0xff为ziplist的结束符
            			break;
            		}
            		// key
            		String hashKey = zipLit.decodeEntryValue();
            		// value
            		String hashValue = zipLit.decodeEntryValue();
            		hashmapValues.put(hashKey, hashValue);
				}
            	e.value = hashmapValues;
            	zipLit = null;
            	break;
            case REDIS_LIST:
                List<String> listValues = new ArrayList<String>();
                for (i = 0; i < length; i++) {
                    String val = loadStringObject();
                    if (val == null) {
                        ERROR("Error reading element at index %d (length: %d)",
                                i, length);
                        return false;
                    }
                    listValues.add(val);
                }
                e.value = listValues;
                break;
            case REDIS_SET:
                HashSet<String> setValues = new HashSet<String>();
                for (i = 0; i < length; i++) {
                    String val = loadStringObject();
                    if (val == null) {
                        ERROR("Error reading element at index %d (length: %d)",
                                i, length);
                        return false;
                    }
                    setValues.add(val);
                }
                e.value = setValues;
                break;
            case REDIS_ZSET:
                TreeMap<Double, String> zsetValues = new TreeMap<Double, String>();
                for (i = 0; i < length; i++) {
                    String val = loadStringObject();
                    if (val == null) {
                        ERROR("Error reading element key at index %d (length: %d)",
                                i, length);
                        return false;
                    }
                    Double score = loadDoubleValue();
                    if (score == null) {
                        ERROR("Error reading element value at index %d (length: %d)",
                                i, length);
                        return false;
                    }
                    zsetValues.put(score, val);
                }
                e.value = zsetValues;
                break;
            case REDIS_HASH:
                HashMap<String, String> mapValues = new HashMap<String, String>();
                for (i = 0; i < length; i++) {
                    String k = loadStringObject();
                    if (k == null) {
                        ERROR("Error reading element key at index %d (length: %d)",
                                i, length);
                        return false;
                    }
                    String val = loadStringObject();
                    if (val == null) {
                        ERROR("Error reading element value at index %d (length: %d)",
                                i, length);
                        return false;
                    }
                    mapValues.put(k, val);
                }
                e.value = mapValues;
                break;
            default:
                ERROR("Type not implemented");
                return false;
        }
        /* because we're done, we assume success */
        e.success = 1;
        return true;
    }

    Entry loadEntry() {
        Entry e = new Entry();
        e.key = null;
        e.value = null;
        e.success = 0;
        e.type = -1;

        long length;
        if (!loadType(e)) {
            return e;
        }
        if (e.type == REDIS_SELECTDB) {
            if ((length = loadLength(null)) == REDIS_RDB_LENERR) {
                ERROR("Error reading database number");
                return e;
            }
            if (length > 63) {
                ERROR("Database number out of range (%d)", length);
                return e;
            }
        } else if (e.type == REDIS_EOF) {
            try {
            	// Starting with RDB version 5, an 8 byte CRC 32 checksum is added to the end of the file.
                if (position.getFilePointer()+8 < position.length()) {
                    ERROR("Unexpected EOF");
                } else {
                    e.success = 1;
                }
            } catch (IOException e1) {
                ERROR("Unexpected Exception");
            }
            return e;
        } else {
            /* optionally consume expire */
            if (e.type == REDIS_EXPIRETIME_FD || e.type == REDIS_EXPIRETIME_FC) {
            	byte[] buf = processTime(e.type);
                if ( buf == null){
                    return e;
                }else {
                	long expire = ((long)(buf[7] & 0x00ff) << 56)
    						+ ((long)(buf[6] & 0x00ff) << 48) + ((long)(buf[5] & 0x00ff) << 40)
    						+ ((long)(buf[4] & 0x00ff) << 32) + ((long)(buf[3] & 0x00ff) << 24)
    						+ ((long)(buf[2] & 0x00ff) << 16) + ((long)(buf[1] & 0x00ff) << 8)
    						+ ((long)(buf[0] & 0x00ff));
                	e.expire = (int)(expire/1000);
                }
                if (!loadType(e))
                    return e;
            } 

            if (!loadPair(e)) {
                ERROR("Error for type %d", e.type);
                return e;
            }
        }

        /*
         * all entries are followed by a valid type: e.g. a new entry, SELECTDB,
         * EXPIRE, EOF
         */
        if (peekType() == -1) {
            ERROR("Followed by invalid type");
            ERROR("Error for type %d", e.type);
            e.success = 0;
        } else {
            e.success = 1;
        }

        return e;
    }
    
    public Entry next() {
        Entry entry = loadEntry();
        if (entry.success != 1)
            ERROR("Can't get entry");
        while(entry.type == REDIS_SELECTDB ||entry.type == REDIS_EXPIRETIME_FD || entry.type == REDIS_EXPIRETIME_FC){
            entry = loadEntry();
            if (entry.success != 1)
                ERROR("Can't get entry");
        }
        if (entry.type == REDIS_EOF)
            return null;
        return entry;
    }

    public void init(File file) {
        try {
            position = new RandomAccessFile(file, "r");
            processHeader();
        } catch (Exception e) {
            try {
                position.close();
            }catch (Exception e1) {
            }
            throw new RuntimeException("Found exceptions when opening file", e);
        }
    }

    public void close(){
        try {
            position.close();
            position = null;
        } catch (Exception e) {
            
        }
    }

}
