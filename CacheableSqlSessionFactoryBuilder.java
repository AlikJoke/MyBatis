package ru.bpc.cm.orm.common;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class CacheableSqlSessionFactoryBuilder extends SqlSessionFactoryBuilder {

	@Override
	public SqlSessionFactory build(Configuration config) {
	    return new CacheableSqlSessionFactory(config);
	  }
}
