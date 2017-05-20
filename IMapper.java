package ru.bpc.cm.orm.common;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.CacheNamespace;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.executor.BatchResult;

import ejbs.cm.svcm.SessionHolder;
import ru.bpc.cm.integration.orm.builders.AggregationBuilder;

/**
 * Интерфейс-маркер, наследовать который должен каждый интерфейс, содержащий
 * маппинг аннотациями MyBatis. Необходимость обусловлена тем, что в противном
 * случае результаты работы маппера не будут кэшироваться.
 * 
 * @author Alimurad A. Ramazanov
 * @since 07.01.2017
 * @version 1.1.1
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

	@DeleteProvider(type = AggregationBuilder.class, method = "simpleQueryBuilder")
	void truncate(@Param("query") String query);

	@DeleteProvider(type = AggregationBuilder.class, method = "simpleQueryBuilder")
	void update(@Param("query") String query);

	@Result(column = "result", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@SelectProvider(type = AggregationBuilder.class, method = "simpleQueryBuilder")
	Timestamp getTimestampValue(@Param("query") String query);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = AggregationBuilder.class, method = "simpleQueryBuilder")
	Integer getIntegerValue(@Param("query") String query);

	@Result(column = "result", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@SelectProvider(type = AggregationBuilder.class, method = "simpleQueryBuilder")
	List<Timestamp> getTimestampValues(@Param("query") String query);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = AggregationBuilder.class, method = "simpleQueryBuilder")
	List<Integer> getIntegerValues(@Param("query") String query);

	@Result(column = "result", javaType = Long.class)
	@ResultType(Long.class)
	@SelectProvider(type = AggregationBuilder.class, method = "simpleQueryBuilder")
	List<Long> getLongValues(@Param("query") String query);
}
