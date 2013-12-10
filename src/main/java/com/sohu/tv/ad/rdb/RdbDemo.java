package com.sohu.tv.ad.rdb;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.sohu.tv.ad.rdb.ResolveRDB.Entry;

/**
 * 
 * @author ganghuawang
 *
 */
public class RdbDemo {
	
	public static void main(String[] args) {
    	final AtomicInteger count = new AtomicInteger();
        String filePath = System.getProperty("rdb.filePath");
        
        ResolveRDB rdb = new ResolveRDB();
        rdb.init(new File(filePath));
        Entry entry = rdb.next();
        
        while(entry!=null){
        	count.incrementAndGet();
            entry = rdb.next();
        }
        rdb.close();
    }
}
