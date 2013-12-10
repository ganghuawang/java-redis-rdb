/**
 * Copyright (c) 2013 Sohu TV
 * All rights reserved.
 */
package com.sohu.tv.ad.rdb;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.sohu.tv.ad.rdb.ParseRDB.Entry;

/**
 * 测试类
 * @author Wang GangHua
 * @version 1.0.0 2013-11-30
 *
 */
public class ParseRDBTest {
	
	public static void main(String[] args) {
    	final AtomicInteger count = new AtomicInteger();
        String filePath = "redis/dumpdir/dump.rdb";
        
        ParseRDB rdb = new ParseRDB();
        rdb.init(new File(filePath));
        Entry entry = rdb.next();
        
        while(entry!=null){
        	count.incrementAndGet();
            entry = rdb.next();
        }
        rdb.close();
    }
}
