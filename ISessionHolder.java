package ejbs.cm.svcm;

import javax.ejb.Local;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.orm.common.IMapper;

/**
 * Интерфейс, хранящий основные методы для работы с сессией MyBatis ORM.
 * 
 * @author Alimurad A. Ramazanov
 * @since 19.02.2017
 * @version 1.1.0
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
	SqlSession getSession(Class<? extends IMapper> clazz, ExecutorType... type);
	
	/**
	 * Получает сессию, добавляя ее в
	 * {@linkplain Configuration}. После вызова <b>обязательно</b> должен быть
	 * вызван метод {@linkplain ISessionHolder#close(SqlSession)}.
	 * <p>
	 * 
	 * @param type
	 *            - тип сессии.
	 * @return сессию, не может быть {@code null}.
	 */
	SqlSession getSession(ExecutorType... type);
	
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
	 * @param createsNew
	 *            - признак необходимости создания <b> новой</b> сессии, а не
	 *            переиспользования имеющейся.
	 * @return сессию, не может быть {@code null}.
	 */
	SqlSession getSession(Class<? extends IMapper> clazz, boolean createsNew, ExecutorType... type);

	/**
	 * Получает сессию для batch-запросов по классу-мапперу, добавляя ее в
	 * {@linkplain Configuration}. После вызова <b>обязательно</b> должен быть
	 * вызван метод {@linkplain ISessionHolder#close(SqlSession)}.
	 * <p>
	 * 
	 * @see {ExecutorType#BATCH}
	 * @param clazz
	 *            - класс-маппера, не может быть {@code null}.
	 * @return сессию, не может быть {@code null}.
	 */
	SqlSession getBatchSession(Class<? extends IMapper> clazz);

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
