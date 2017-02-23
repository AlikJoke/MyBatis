package ejbs.cm.svcm;

import javax.ejb.Local;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

/**
 * Интерфейс, хранящий основные методы для работы с сессией MyBatis ORM.
 * 
 * @author Alimurad A. Ramazanov
 * @since 19.02.2017
 * @version 1.0.0
 *
 */
@Local
public interface ISessionHolder {

	/**
	 * Получает сессию по классу-мапперу, добавляя ее в
	 * {@linkplain Configuration}. После вызова <b>обязательно</b> должен быть
	 * вызван метод {@linkplain ISessionHolder#close(SqlSession)}.
	 * <p>
	 * 
	 * @param clazz
	 *            - класс-маппера, не может быть {@code null}.
	 * @param type
	 *            - тип сессии.
	 * @return сессию, не может быть {@code null}.
	 */
	SqlSession getSession(Class<?> clazz, ExecutorType... type);

	/**
	 * Закрывает сессию.
	 * <p>
	 * 
	 * @see SqlSession
	 * @param session
	 *            - сессия, не может быть {@code null}.
	 * @throws IllegalStateExcetion
	 */
	void close(SqlSession session);
}
