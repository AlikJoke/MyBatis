package ru.bpc.cm.orm.common;

import java.sql.Connection;

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
		CloseableItem sessionItem = null;
		boolean isContains = false;
		if (isContains = SessionFactory.containsSession(this.hashCode())) {
			sessionItem = SessionFactory.getCachedSession(this.hashCode());
			sessionItem.getSession().flushStatements();
			sessionItem.decrement();
		}

		if (!isContains || sessionItem.isUseless()) {
			super.close();
		}
	}

	@Override
	public <E> List<E> selectList(String statement) {
		synchronized (this) {
			return this.selectList(statement, null);
		}
	}

	@Override
	public <T> T getMapper(Class<T> type) {
		if (!getConfiguration().getMapperRegistry().hasMapper(type))
			getConfiguration().addMapper(type);
		return getConfiguration().<T>getMapper(type, this);
	}
}
