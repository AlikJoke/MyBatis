package ru.bpc.cm.orm.common;

import java.sql.SQLException;

import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;

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

	private final int pool = 10;

	public CacheableSqlSessionFactory(Configuration configuration) {
		super(configuration);
	}

	@Override
	public SqlSession openSession() {
		return this.openAnySession(false);
	}

	@Override
	public SqlSession openSession(ExecutorType executor) {
		if (executor == ExecutorType.BATCH)
			return this.openAnySession(true);
		else
			return this.openAnySession(false);
	}

	private SqlSession openAnySession(boolean isBatch) {
		CloseableItem sessionItem = null;

		while (true) {
			try {
				if (SessionFactory.getCacheableSessions(isBatch).size() < pool) {
					CloseableItem item = new CloseableItem(this.openCustomSession(isBatch));
					SessionFactory.getCacheableSessions(isBatch).put(item.hashCode(), item);
				}

				sessionItem = SessionFactory.getRandomSession(isBatch);

				if (sessionItem.getSession().getConnection().isClosed()) {
					SessionFactory.getCacheableSessions(isBatch).remove(sessionItem.hashCode());
					continue;
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			break;
		}

		sessionItem.increment();
		return sessionItem.getSession();
	}

	@Override
	public SqlSession openSession(boolean createsNew) {
		if (createsNew) {
			CloseableItem sessionItem = new CloseableItem(this.openCustomSession(false));
			SessionFactory.getCacheableSessions(false).put(sessionItem.hashCode(), sessionItem);

			sessionItem.increment();
			return sessionItem.getSession();
		}
		return this.openSession();
	}

	private SqlSession openCustomSession(boolean isBatch) {
		Transaction tx = null;
		try {
			final Environment environment = super.getConfiguration().getEnvironment();
			final TransactionFactory transactionFactory = getTransactionFactoryFromEnvironment(environment);
			tx = transactionFactory.newTransaction(environment.getDataSource(), null, false);
			final Executor executor = super.getConfiguration().newExecutor(tx,
					isBatch ? ExecutorType.BATCH : super.getConfiguration().getDefaultExecutorType());
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
