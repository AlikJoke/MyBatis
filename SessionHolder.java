package ejbs.cm.svcm;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.sql.DataSource;

import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.db.DataSourceJNDIDictionary;

@Stateless
public class SessionHolder implements ISessionHolder {

	@Resource(name = DataSourceJNDIDictionary.SVCM_DATA_SOURCE)
	private DataSource dataSource;

	private Configuration configuration;

	@Override
	public SqlSession getSession(Class<?> clazz, ExecutorType... type) {
		if (configuration == null) {
			TransactionFactory transactionFactory = new JdbcTransactionFactory();
			Environment environment = new Environment("development", transactionFactory, dataSource);
			configuration = new Configuration(environment);
			configuration.setLazyLoadingEnabled(true);
		}

		if (IMapper.class.isAssignableFrom(clazz) && !configuration.hasMapper(clazz))
			configuration.addMapper(clazz);
		return type.length == 0 ? new SqlSessionFactoryBuilder().build(configuration).openSession()
				: new SqlSessionFactoryBuilder().build(configuration).openSession(type[0]);
	}

	@Override
	public void close(SqlSession session) {
		if (session == null)
			throw new IllegalStateException("Session can't be null");
		session.close();
	}
}
