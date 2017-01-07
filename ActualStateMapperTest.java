package ru.bpc.sv.ejb.svfe;

import javax.sql.DataSource;

import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import ru.bpc.cm.monitoring.orm.ActualStateMapper;

@RunWith(MockitoJUnitRunner.class)
public class ActualStateMapperTest {

	SqlSession session;
	@Mock
	DataSource dataSource;

	@Before
	public void initializeSession() {
		TransactionFactory transactionFactory = new JdbcTransactionFactory();
		Environment environment = new Environment("development", transactionFactory, dataSource);
		Configuration configuration = new Configuration(environment);
		configuration.addMapper(ActualStateMapper.class);
		session = new SqlSessionFactoryBuilder().build(configuration).openSession();
	}

	@Test
	public void checkSession() {
		Assert.assertNotNull(session);
	}

	@Test
	public void checkMapper() {
		ActualStateMapper mapper = session.getMapper(ActualStateMapper.class);
		Assert.assertNotNull(mapper);
	}
	
	@After
	public void close() {
		session.close();
	}
}
