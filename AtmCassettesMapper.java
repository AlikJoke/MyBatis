package ru.bpc.cm.cashmanagement.orm;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.mapping.StatementType;

import ru.bpc.cm.cashmanagement.AtmCassettesController;
import ru.bpc.cm.cashmanagement.orm.handlers.EnumHandler;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;

/**
 * Интерфейс-маппер для класса {@link AtmCassettesController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 24.02.2017
 * @version 1.0.1
 *
 */
public interface AtmCassettesMapper extends IMapper {

	@Select("SELECT count(1) as CNT FROM T_CM_ATM_CASSETTES WHERE ATM_ID = #{atmId} "
			+ "AND CASS_TYPE = #{cassTypeId} ")
	@Result(column = "CNT", javaType = Integer.class)
	@Options(useCache = true, statementType = StatementType.PREPARED)
	Integer getAtmCassCount(@Param("atmId") int atmId, @Param("cassTypeId") int cassTypeId);

	@Select("SELECT ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_CURR, CASS_VALUE FROM T_CM_ATM_CASSETTES WHERE "
			+ "ATM_ID = #{atmId} ORDER BY CASS_TYPE, CASS_NUMBER")
	@Results({ 
		@Result(property = "number", column = "CASS_NUMBER"),
		@Result(property = "denom", column = "CASS_VALUE"), 
		@Result(property = "curr", column = "CASS_CURR"),
		@Result(property = "type", column = "CASS_TYPE", typeHandler = EnumHandler.class, javaType = AtmCassetteType.class) 
	})
	@Options(useCache = true, statementType = StatementType.PREPARED, fetchSize = 1000)
	List<AtmCassetteItem> getAtmCassettes(@Param("atmId") int atmId);

	@Insert("insert into T_CM_ATM_CASSETTES (ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_CURR, CASS_VALUE) VALUES ( #{atmId}, #{typeId}, #{cassNumber}, "
			+ "#{curr}, #{denom})")
	void saveAtmCassettes(@Param("atmId") int atmId, @Param("typeId") Integer typeId, @Param("curr") Integer curr,
			@Param("cassNumber") Integer cassNumber, @Param("denom") Integer denom);

	@Delete("DELETE FROM T_CM_ATM_CASSETTES WHERE ATM_ID = #{atmId}")
	void deleteAtmCassettes(@Param("atmId") int atmId);
}
