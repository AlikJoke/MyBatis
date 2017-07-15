package ejbs.cm.svcm;

import javax.ejb.Local;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.orm.common.IMapper;

/**
 * ���������, �������� �������� ������ ��� ������ � ������� MyBatis ORM.
 * 
 * @author Alimurad A. Ramazanov
 * @since 19.02.2017
 * @version 1.1.0
 *
 */
@Local
public interface ISessionHolder {

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
	SqlSession getSession(Class<? extends IMapper> clazz, ExecutorType... type);
	
	/**
	 * �������� ������, �������� �� �
	 * {@linkplain Configuration}. ����� ������ <b>�����������</b> ������ ����
	 * ������ ����� {@linkplain ISessionHolder#close(SqlSession)}.
	 * <p>
	 * 
	 * @param type
	 *            - ��� ������.
	 * @return ������, �� ����� ���� {@code null}.
	 */
	SqlSession getSession(ExecutorType... type);
	
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
	 * @param createsNew
	 *            - ������� ������������� �������� <b> �����</b> ������, � ��
	 *            ����������������� ���������.
	 * @return ������, �� ����� ���� {@code null}.
	 */
	SqlSession getSession(Class<? extends IMapper> clazz, boolean createsNew, ExecutorType... type);

	/**
	 * �������� ������ ��� batch-�������� �� ������-�������, �������� �� �
	 * {@linkplain Configuration}. ����� ������ <b>�����������</b> ������ ����
	 * ������ ����� {@linkplain ISessionHolder#close(SqlSession)}.
	 * <p>
	 * 
	 * @see {ExecutorType#BATCH}
	 * @param clazz
	 *            - �����-�������, �� ����� ���� {@code null}.
	 * @return ������, �� ����� ���� {@code null}.
	 */
	SqlSession getBatchSession(Class<? extends IMapper> clazz);

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
