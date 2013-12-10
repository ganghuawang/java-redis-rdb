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
 * resolve the dump.rdb file
 * the newest RDB version is 6 
 * refer : https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
 * @author ganghuawang
 *
 */
public class ResolveRDB {
	
    /* Object types */
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

    /*
     * Objects encoding. Some kind of objects like Strings and Hashes can be
     * internally represented in multiple ways. The 'encoding' field of the
     * object is set to one of this fields for this object.
     */
    public static final int REDIS_ENCODING_RAW = 0; /* Raw representation */

    public static final int REDIS_ENCODING_INT = 1; /* Encoded as integer */

    public static final int REDIS_ENCODING_ZIPMAP = 2; /* Encoded as zipmap */

    public static final int REDIS_ENCODING_HT = 3; /* Encoded as an hash table */

    /* Object types only used for dumping to disk */
    public static final int REDIS_EXPIRETIME_FC = 252; /* expiry time in milliseconds, 8 bytes */
    
    public static final int REDIS_EXPIRETIME_FD = 253;	/* expiry time in seconds, 8 bytes */

    public static final int REDIS_SELECTDB = 254;	/* indicates database selector */

    public static final int REDIS_EOF = 255;	/* represent the end */

    /*
     * Defines related to the dump file format. To store 32 bits lengths for
     * short keys requires a lot of space, so we check the most significant 2
     * bits of the first byte to interpreter the length: 00|000000 => if the two
     * MSB are 00 the len is the 6 bits of this byte 01|000000 00000000 => 01,
     * the len is 14 byes, 6 bits + 8 bits of next byte 10|000000 [32 bit
     * integer] => if it's 01, a full 32 bit len will follow 11|000000 this
     * means: specially encoded object will follow. The six bits number specify
     * the kind of object that follows. See the REDIS_RDB_ENC_* defines. Lenghts
     * up to 63 are stored using a single byte, most DB keys, and may values,
     * will fit inside.
     */
    public static final int REDIS_RDB_6BITLEN = 0;
    public static final int REDIS_RDB_14BITLEN = 1;
    public static final int REDIS_RDB_32BITLEN = 2;
    public static final int REDIS_RDB_ENCVAL = 3;

    public static final long REDIS_RDB_LENERR = Long.MAX_VALUE;
    
    /*
     * When a length of a string object stored on disk has the first two bits
     * set, the remaining two bits specify a special encoding for the object
     * accordingly to the following defines:
     */
    public static final int REDIS_RDB_ENC_INT8 = 0; /* 8 bit signed integer */

    public static final int REDIS_RDB_ENC_INT16 = 1; /* 16 bit signed integer */

    public static final int REDIS_RDB_ENC_INT32 = 2; /* 32 bit signed integer */

    public static final int REDIS_RDB_ENC_LZF = 3; /* string compressed with
                                                    * FASTLZ
                                                    */

    private static void ERROR(String msg, Object... args) {
        throw new RuntimeException(String.format(msg, args));
    }

    RandomAccessFile position = null;

    public static class Entry {
        String key;

        Object value;

        int type;

        byte success;
        
        int expire; /* time is second */
    };

    private static byte[] chars2bytes(String str) {
        try {
            return str.getBytes("ASCII");
        } catch (UnsupportedEncodingException e) {
            return str.getBytes();
        }
    }

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

    /* discard time, just consume the bytes */
    byte[] processTime(int length) {
        byte[] t = new byte[length];
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
        CompressLZF.expand(c, 0, (int) clen, s, 0, (int) slen);
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
            	throw new UnsupportedOperationException();
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
            	byte[] buf = processTime(8);
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
