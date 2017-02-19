package ejbs.cm.svcm;

import javax.ejb.Local;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

@Local
public interface ISessionHolder {

	SqlSession getSession(Class<?> clazz, ExecutorType... type);
	
	void close(SqlSession session);
}
