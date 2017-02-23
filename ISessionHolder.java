package ejbs.cm.svcm;

import javax.ejb.Local;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.config.IMapper;

/**
 * ���������, �������� �������� ������ ��� ������ � ������� MyBatis ORM.
 * 
 * @author Alimurad A. Ramazanov
 * @since 19.02.2017
 * @version 1.0.0
 *
 */
@Local
public interface ISessionHolder {

	/**
	 * ���������� ������ ��� ������.
	 * <p>
	 * 
	 * @see IMapper
	 * @param clazz
	 *            - ����� ����������-�������.
	 * @return ������; {@code null}, ���� ������� �� ����������.
	 */
	<T extends IMapper> T getMapper(SqlSession session, Class<T> clazz);

	/**
	 * �������� ������ �� ������-�������, �������� �� �
	 * {@linkplain Configuration}. ����� ������ <b>�����������</b> ������ ����
	 * ������ ����� {@linkplain ISessionHolder#close(SqlSession)}.
	 * <p>
	 * 
	 * @param clazz
	 *            - �����-�������, �� ����� ���� {@code null}.
	 * @param type
	 *            - ��� ������.
	 * @return ������, �� ����� ���� {@code null}.
	 */
	SqlSession getSession(Class<?> clazz, ExecutorType... type);

	/**
	 * ��������� ������.
	 * <p>
	 * 
	 * @see SqlSession
	 * @param session
	 *            - ������, �� ����� ���� {@code null}.
	 * @throws IllegalStateExcetion
	 */
	void close(SqlSession session);
}
