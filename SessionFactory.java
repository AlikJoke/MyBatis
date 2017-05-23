package ru.bpc.cm.orm.common;

import java.util.concurrent.ConcurrentMap;

import jersey.repackaged.com.google.common.collect.Maps;

public class SessionFactory {


	private static ConcurrentMap<Integer, CloseableItem> sessions;

	static {
		sessions = Maps.newConcurrentMap();
	}
	
	public static ConcurrentMap<Integer, CloseableItem> getCacheableSessions() {
		return sessions;
	}
}
