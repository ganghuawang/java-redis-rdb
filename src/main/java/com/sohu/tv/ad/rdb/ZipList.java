/**
 * 
 */
package com.sohu.tv.ad.rdb;

import java.io.UnsupportedEncodingException;

/**
 * 
 * decode the Ziplist type value
 * @author ganghuawang
 *
 */
public class ZipList {
	
	public static final int ZIPLIST_PREV_ENTRY_LENGTH=254;
	public static final int ZIPLIST_END=255;
	/**
	 * //https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
	 */
    public static final int ZIPLIST_ENTRY_FLAG_6BITLEN = 0;	//6位用于计数
    public static final int ZIPLIST_ENTRY_FLAG_14BITLEN = 1;
    public static final int ZIPLIST_ENTRY_FLAG_5BYTELEN = 2; 	//5字节用于计数
    public static final int ZIPLIST_ENTRY_FLAG_N2BYTEVLAUE = 12;	//后面2字节的无符号整数就是entry值
    public static final int ZIPLIST_ENTRY_FLAG_N4BYTEVLAUE = 13;	
    public static final int ZIPLIST_ENTRY_FLAG_N8BYTEVLAUE = 14;
    public static final int ZIPLIST_ENTRY_FLAG_N3BYTEVLAUE = 0x00f0;
    public static final int ZIPLIST_ENTRY_FLAG_N1BYTEVLAUE = 0x00fe;
	
	
	private final byte[] ziplistByte;
	private int index; //byte数组下标
	
	public ZipList(byte[] ziplistByte) {
		super();
		this.ziplistByte = ziplistByte;
		/* 
		 * 从第9个字节开始，跳过前面8个字节，其中前4个字节表示ziplist的长度，
		 * 后4个字节表示最后一个entry在ziplist中的相对偏移量 
		 * */
		this.index = 8;
	}
	
	int getEndByte(){
		return ziplistByte[index];
	}
	
	/* 
	 * 占1或5个字节,表示前一个entry的字节长度，第一个entry是0
	 * 如果第一个字节整型值等于254,则后面的4个字节表示长度,否则第一个字节整型值就是长度
     * */
	private void decodePrevEntryFlag() {
		int len = prevEntryIntegerValue();
		if (len < ZIPLIST_PREV_ENTRY_LENGTH) {
			index++;
		} else {
			index = index + 5;
		}
	}
    
    /* Decode the entry encoded length pointed by 'p' */
	private int prevEntryIntegerValue() {
		int len = ziplistByte[index];
		if (len < ZIPLIST_PREV_ENTRY_LENGTH)
			return len;
		len = ((ziplistByte[index + 4] & 0x00ff) << 24)
				+ ((ziplistByte[index + 3] & 0x00ff) << 16)
				+ ((ziplistByte[index + 2] & 0x00ff) << 8)
				+ ((ziplistByte[index + 1] & 0x00ff));
		return len;
	}
    
	public int decodeEntryCount() {
		/* 
		 * 占2个字节,entry的个数,key和value都是一个entry,所以解析Map的for循环次数要除以2 
		 * */
		int entryCount = (((ziplistByte[index + 1] & 0x003F) << 8) | (ziplistByte[index] & 0x00ff));
		// 解析了2个字节，下标移动2个位置
		index = index + 2;
		return entryCount;
	}
	
	public String decodeEntryValue() {
		decodePrevEntryFlag();
		
		Integer[] object = decodeEntrySpecialFlag();
		int entryDataByteLen = object[0];	// entry数据的字节长度
		
		String value;
		if(object[1] != null){
			value = String.valueOf(object[1]);
		}else {
			value = byteToString(subbyte(ziplistByte, index, entryDataByteLen));
		}
		index = index + entryDataByteLen;
    	return value;
    }
	
    /**
     * special flag,占用字节数1到9之间, 用于表示entry数据占的字节长度或entry的整型值
     * @param value
     * @param index
     * @return Integer[] [entry data bytes length , entry integer value]
     */
    private Integer[] decodeEntrySpecialFlag() {
        byte[] buf = new byte[9];
        int flagLen=1;
        int entryLen=0;
        Integer intValue = null;
        
		for (int i = 0; i < buf.length && ((index + i) < ziplistByte.length); i++) {
			buf[i] = ziplistByte[index + i];
		}
        int type = (buf[0] & 0x00C0) >> 6;
        if (buf[0] == ZIPLIST_ENTRY_FLAG_N1BYTEVLAUE) {
        	/* Read next 1 byte integer value */
        	flagLen = 1 ;
        	entryLen = 1;
			intValue = (0x00ff & buf[1]);
		} else if (buf[0] == ZIPLIST_ENTRY_FLAG_N3BYTEVLAUE) {
			/* Read next 3 byte integer value */
			flagLen = 1;
			entryLen = 3;
			intValue = ((buf[3] & 0x00ff) << 16) + ((buf[2] & 0x00ff) << 8)
					+ ((buf[1] & 0x00ff));
		} else if (type == ZIPLIST_ENTRY_FLAG_6BITLEN) {
			/* Read a 6 bit len */
        	flagLen = 1 ;
        	entryLen = (buf[0] & 0x003F);
		} else if (type == ZIPLIST_ENTRY_FLAG_14BITLEN) {
			/* Read a 14 bit len */
			flagLen = 2 ;
        	entryLen = ((buf[0] & 0x003F) << 8) | (buf[1] & 0x00ff);
		} else if (type == ZIPLIST_ENTRY_FLAG_5BYTELEN) {
			/* Read a 5 byte len */
			flagLen = 5;
			entryLen = ((buf[4] & 0x00ff) << 32) + ((buf[3] & 0x00ff) << 24)
					+ ((buf[2] & 0x00ff) << 16) + ((buf[1] & 0x00ff) << 8)
					+ ((buf[0] & 0x003F));
		} else {
			type = (buf[0] & 0x00f0) >> 4;
			if (type == ZIPLIST_ENTRY_FLAG_N2BYTEVLAUE) {
				/* Read next 2 byte integer value */
				flagLen = 1;
				entryLen = 2;
				intValue = ((buf[2] & 0x00ff) << 8) + ((buf[1] & 0x00ff));
			} else if (type == ZIPLIST_ENTRY_FLAG_N4BYTEVLAUE) {
				/* Read next 4 byte integer value */
				flagLen = 1;
				entryLen = 4;
				intValue = ((buf[4] & 0x00ff) << 24)
						+ ((buf[3] & 0x00ff) << 16) + ((buf[2] & 0x00ff) << 8)
						+ ((buf[1] & 0x00ff));
			} else if (type == ZIPLIST_ENTRY_FLAG_N8BYTEVLAUE) {
				/* Read next 8 byte integer value */
				flagLen = 1;
				entryLen = 8;
				intValue = ((buf[8] & 0x00ff) << 56)
						+ ((buf[7] & 0x00ff) << 48) + ((buf[6] & 0x00ff) << 40)
						+ ((buf[5] & 0x00ff) << 32) + ((buf[4] & 0x00ff) << 24)
						+ ((buf[3] & 0x00ff) << 16) + ((buf[2] & 0x00ff) << 8)
						+ ((buf[1] & 0x00ff));
			} else if (type == 15) {
				/* Read next 8 byte integer value */
				flagLen = 1;
				entryLen = (buf[0] & 0x000f);
			} else {
				ERROR(" Unknown entry special flag encoding (0x%02x) ", type);
			}
		}
        if(entryLen == 0){
        	ERROR(" entryLen is not 0 (0x%02x) ", entryLen);
        }
        // 解析完special flag, index往下移
        index = index + flagLen;
        return new Integer[]{entryLen, intValue};
    }
    
    static byte[] subbyte(byte[] buf, int start, int len){
    	byte[] value = new byte[len];
    	for (int i=0; i<len; start++,i++) {
			value[i] = buf[start];
		}
    	return value;
    }
    
    
	static String byteToString(byte[] buf) {
		try {
			return new String(buf, "ASCII");
		} catch (UnsupportedEncodingException e) {
			return new String(buf);
		}
	}
    
    static void ERROR(String msg, Object... args) {
        throw new RuntimeException(String.format(msg, args));
    }
	
}
