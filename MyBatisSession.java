package ru.bpc.cm.config;

import java.util.ServiceLoader;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import ru.bpc.cm.db.DataSourceJNDIDictionary;

public class MyBatisSession {

	@Resource(name = DataSourceJNDIDictionary.SVCM_DATA_SOURCE)
	private DataSource dataSource;

	private SqlSession session;

	private static MyBatisSession INSTANCE = new MyBatisSession();

	public static MyBatisSession getInstance() {
		return INSTANCE;
	}

	public SqlSession getSession() {
		if (session == null) {
			TransactionFactory transactionFactory = new JdbcTransactionFactory();
			Environment environment = new Environment("development", transactionFactory, dataSource);
			Configuration configuration = new Configuration(environment);
			ServiceLoader.<IMapper>load(IMapper.class).iterator()
					.forEachRemaining(clazz -> configuration.addMapper(clazz.getClass()));
			session = new SqlSessionFactoryBuilder().build(configuration).openSession();
		}
		return session;
	}
}
