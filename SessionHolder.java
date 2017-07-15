package ejbs.cm.svcm;

import java.util.Set;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.sql.DataSource;

import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.reflections.Reflections;

import jersey.repackaged.com.google.common.collect.Sets;
import ru.bpc.cm.db.DataSourceJNDIDictionary;
import ru.bpc.cm.orm.common.CacheableSqlSessionFactoryBuilder;
import ru.bpc.cm.orm.common.IMapper;

@Stateless
public class SessionHolder implements ISessionHolder {

	@Resource(name = DataSourceJNDIDictionary.SVCM_DATA_SOURCE)
	private DataSource dataSource;

	private Configuration configuration;
	
	private Set<Class<? extends IMapper>> mappers;
	
	private void loadMappers() {
		Reflections reflections = new Reflections("ru.bpc.cm");    
		mappers = Sets.newHashSet(reflections.getSubTypesOf(IMapper.class));
		for (Class<? extends IMapper> mapper : mappers)
			configuration.addMapper(mapper);
	}
	
	private void initConfigurationIfNotInit() {
		if (configuration == null) {
			TransactionFactory transactionFactory = new JdbcTransactionFactory();
			Environment environment = new Environment("development", transactionFactory, dataSource);
			configuration = new Configuration(environment);
			this.loadMappers();
			configuration.setLazyLoadingEnabled(true);
		}
	}

	private void addMapperIfAbsent(Class<? extends IMapper> clazz) {
		if (!configuration.hasMapper(clazz))
			configuration.addMapper(clazz);
	}

	@Override
	public SqlSession getSession(Class<? extends IMapper> clazz, ExecutorType... type) {
		initConfigurationIfNotInit();
		addMapperIfAbsent(clazz);
		return type.length == 0 ? new CacheableSqlSessionFactoryBuilder().build(configuration).openSession()
				: new CacheableSqlSessionFactoryBuilder().build(configuration).openSession(type[0]);
	}
	
	@Override
	public SqlSession getSession(ExecutorType... type) {
		initConfigurationIfNotInit();
		return type.length == 0 ? new CacheableSqlSessionFactoryBuilder().build(configuration).openSession()
				: new CacheableSqlSessionFactoryBuilder().build(configuration).openSession(type[0]);
	}

	@Override
	public SqlSession getSession(Class<? extends IMapper> clazz, boolean createsNew, ExecutorType... type) {
		initConfigurationIfNotInit();
		addMapperIfAbsent(clazz);
		return type.length == 0 ? new CacheableSqlSessionFactoryBuilder().build(configuration).openSession(createsNew)
				: new CacheableSqlSessionFactoryBuilder().build(configuration).openSession(type[0]);
	}

	@Override
	public SqlSession getBatchSession(Class<? extends IMapper> clazz) {
		initConfigurationIfNotInit();
		addMapperIfAbsent(clazz);
		return new CacheableSqlSessionFactoryBuilder().build(configuration).openSession(ExecutorType.BATCH);
	}

	@Override
	public void close(SqlSession session) {
		if (session == null)
			throw new IllegalStateException("Session can't be null");
		session.close();
	}
}
