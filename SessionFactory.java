package ru.bpc.cm.orm.common;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;

public class SessionFactory {

	private static ConcurrentMap<Integer, CloseableItem> sessions;
	private static ConcurrentMap<Integer, CloseableItem> batchSessions;
	private static Random rand;

	static {
		sessions = Maps.newConcurrentMap();
		batchSessions = Maps.newConcurrentMap();
		rand = new Random();
	}

	protected static ConcurrentMap<Integer, CloseableItem> getCacheableSessions(boolean isBatch) {
		return isBatch ? batchSessions : sessions;
	}

	protected static boolean containsSession(Integer hash) {
		return sessions.containsKey(hash) || batchSessions.containsKey(hash);
	}
	
	protected static CloseableItem getCachedSession(Integer hash) {
		if (sessions.containsKey(hash))
			return sessions.get(hash);
		else if (batchSessions.containsKey(hash))
			return batchSessions.get(hash);
		return null;
	}

	protected static List<CloseableItem> getSessions(boolean isBatch) {
		return isBatch ? Lists.newArrayList(batchSessions.values()) : Lists.newArrayList(sessions.values());
	}

	protected static CloseableItem getRandomSession(boolean isBatch) {
		List<CloseableItem> sessions = getSessions(isBatch);
		if (sessions.isEmpty())
			return null;
		else if (sessions.size() == 1)
			return sessions.get(0);
		else
			return sessions.get(rand.nextInt(sessions.size() - 1));
	}
}
