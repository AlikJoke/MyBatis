package ru.bpc.cm.cashmanagement.orm;

import java.util.List;

import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.mapping.StatementType;

import ru.bpc.cm.cashmanagement.orm.handlers.EnumHandler;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;

public interface AtmCassettesMapper extends IMapper {

	@Select("SELECT count(1) as CNT FROM T_CM_ATM_CASSETTES WHERE ATM_ID = #{atmId} "
			+ "AND CASS_TYPE = #{cassTypeId} ")
	@Result(column = "CNT")
	@Options(useCache = true, statementType = StatementType.PREPARED)
	int getAtmCassCount(@Param("atmId") int atmId, @Param("cassTypeId") int cassTypeId);

	@Select("SELECT ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_CURR, CASS_VALUE FROM T_CM_ATM_CASSETTES WHERE "
			+ "ATM_ID = #{atmId} ORDER BY CASS_TYPE, CASS_NUMBER")
	@Results(value = { 
			@Result(property = "number", column = "CASS_NUMBER", id = true),
			@Result(property = "denom", column = "CASS_VALUE"), @Result(property = "curr", column = "CASS_CURR"),
			@Result(property = "type", column = "CASS_TYPE", typeHandler = EnumHandler.class, javaType = AtmCassetteType.class),
			@Result(property = "balanceAlert", column = "CASS_CURR") })
	@Options(useCache = true, statementType = StatementType.PREPARED)
	List<AtmCassetteItem> getAtmCassettes(@Param("atmId") int atmId);
	
	void saveAtmCassettes(@Param("atmId") int atmId,List<AtmCassetteItem> atmCassList);
}
