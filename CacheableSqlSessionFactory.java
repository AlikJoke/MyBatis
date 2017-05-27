package ru.bpc.cm.orm.common;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Фабрика сессий, переиспользующая по возможности сессии.
 * 
 * @see DefaultSqlSessionFactory
 * 
 * @author Alimurad A. Ramazanov
 * @since 23.05.2017
 * @version 1.0.0
 *
 */
public class CacheableSqlSessionFactory extends DefaultSqlSessionFactory {

	public CacheableSqlSessionFactory(Configuration configuration) {
		super(configuration);
	}

	@Override
	public SqlSession openSession() {
		CloseableItem sessionItem = null;

		Iterator<Entry<Integer, CloseableItem>> iterator = SessionFactory.getCacheableSessions().entrySet().iterator();
		if (iterator.hasNext())
			sessionItem = (CloseableItem) ORMUtils.getSingleValue(Lists.newArrayList(iterator.next().getValue()));

		if (sessionItem != null && sessionItem.isUseless()) {
			SessionFactory.getCacheableSessions().remove(sessionItem.hashCode());
			sessionItem = null;
		}

		if (sessionItem != null && sessionItem.getOpened() + sessionItem.getCounter() > 30)
			sessionItem = null;

		if (sessionItem == null) {
			sessionItem = new CloseableItem(this.openCustomSession());
			SessionFactory.getCacheableSessions().put(sessionItem.hashCode(), sessionItem);
		}

		sessionItem.increment();
		return sessionItem.getSession();
	}
	
	@Override
	public SqlSession openSession(boolean createsNew) {
		if (createsNew) {
			CloseableItem sessionItem = new CloseableItem(this.openCustomSession());
			SessionFactory.getCacheableSessions().put(sessionItem.hashCode(), sessionItem);

			sessionItem.increment();
			return sessionItem.getSession();
		}
		return this.openSession();
	}

	private SqlSession openCustomSession() {
		Transaction tx = null;
		try {
			final Environment environment = super.getConfiguration().getEnvironment();
			final TransactionFactory transactionFactory = getTransactionFactoryFromEnvironment(environment);
			tx = transactionFactory.newTransaction(environment.getDataSource(), null, false);
			final Executor executor = super.getConfiguration().newExecutor(tx,
					super.getConfiguration().getDefaultExecutorType());
			return new CloseableSession(super.getConfiguration(), executor);
		} catch (Exception e) {
			closeTransaction(tx);
			throw ExceptionFactory.wrapException("Error opening session.  Cause: " + e, e);
		} finally {
			ErrorContext.instance().reset();
		}
	}

	private TransactionFactory getTransactionFactoryFromEnvironment(Environment environment) {
		if (environment == null || environment.getTransactionFactory() == null) {
			return new ManagedTransactionFactory();
		}
		return environment.getTransactionFactory();
	}

	private void closeTransaction(Transaction tx) {
		if (tx != null) {
			try {
				tx.close();
			} catch (SQLException ignore) {
				// ignore
			}
		}
	}

}
