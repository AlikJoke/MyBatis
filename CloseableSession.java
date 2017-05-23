package ru.bpc.cm.orm.common;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.defaults.DefaultSqlSession;

/**
 * Сессия, закрываемая только в случае, если для нее больше нет "захватов".
 * 
 * @see DefaultSqlSession
 * @see CacheableSqlSession
 * 
 * @author Alimurad A. Ramazanov
 * @since 23.05.2017
 * @version 1.0.0
 *
 */
public class CloseableSession extends DefaultSqlSession {

	public CloseableSession(Configuration configuration, Executor executor) {
		super(configuration, executor);
	}

	@Override
	public void close() {
		CloseableItem sessionItem = new CloseableItem(this);
		boolean isContains = false;
		if (isContains = SessionFactory.getCacheableSessions().containsKey(sessionItem.hashCode())) {
			sessionItem = SessionFactory.getCacheableSessions().get(sessionItem.hashCode());
			sessionItem.decrement();
		}
		if (isContains && sessionItem.isUseless()) {
			SessionFactory.getCacheableSessions().remove(sessionItem.hashCode());
			super.close();
		} else if (!isContains) {
			super.close();
		}
	}

}
