package ru.bpc.cm.config;

import java.util.List;

import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.executor.BatchResult;

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
public interface IMapper {

	@Flush
	List<BatchResult> flush();
}
