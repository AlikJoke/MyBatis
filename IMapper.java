package ru.bpc.cm.config;

import java.util.List;

import org.apache.ibatis.annotations.CacheNamespace;
import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.executor.BatchResult;

import ejbs.cm.svcm.SessionHolder;

/**
 * ���������-������, ����������� ������� ������ ������ ���������, ����������
 * ������� ����������� MyBatis. ������������� ����������� ���, ��� � ���������
 * ������ � ������ �� ����� ������� ��� ���� ��������.
 * 
 * @author Alimurad A. Ramazanov
 * @since 07.01.2017
 * @version 1.0.0
 *
 */
@CacheNamespace(implementation = org.mybatis.caches.ehcache.EhcacheCache.class)
public interface IMapper {

	/**
	 * ����� ��� ������������� �������� �������� � ����� ������.
	 * <p>
	 * 
	 * @see BatchResult
	 * @see {@link SessionHolder}
	 * @return ���������� batch-�������.
	 */
	@Flush
	List<BatchResult> flush();
}
