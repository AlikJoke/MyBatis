package ru.bpc.cm.config;

import java.util.List;

import org.apache.ibatis.annotations.CacheNamespace;
import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.executor.BatchResult;

import ejbs.cm.svcm.SessionHolder;

/**
 * Интерфейс-маркер, наследовать который должен каждый интерфейс, содержащий
 * маппинг аннотациями MyBatis. Необходимость обусловлена тем, что в противном
 * случае в сессии не будет маппера для этой сущности.
 * 
 * @author Alimurad A. Ramazanov
 * @since 07.01.2017
 * @version 1.0.0
 *
 */
@CacheNamespace(implementation = org.mybatis.caches.ehcache.EhcacheCache.class)
public interface IMapper {

	/**
	 * Метод для синхронизации пакетных запросов с базой данных.
	 * <p>
	 * 
	 * @see BatchResult
	 * @see {@link SessionHolder}
	 * @return результаты batch-запроса.
	 */
	@Flush
	List<BatchResult> flush();
}
