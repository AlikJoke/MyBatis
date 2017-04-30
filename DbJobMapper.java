package ru.bpc.cm.integration.orm;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.UpdateProvider;

import ru.bpc.cm.integration.orm.builders.DbJobBuilder;
import ru.bpc.cm.orm.common.IMapper;

/**
 * Интерфейс-маппер для класса DbJobController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 30.04.2017
 * @version 1.0.0
 *
 */
public interface DbJobMapper extends IMapper {

	@Result(column = "RES_SHIFT", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select (trunc(sysdate) - trunc(last_trans_datetime)) AS RES_SHIFT from t_cm_intgr_params")
	Integer actualizeStats_getShift();

	@UpdateProvider(type = DbJobBuilder.class, method = "updateQueryBuilder")
	void actualizeStats_update(@Param("shift") Integer shift, @Param("tableName") String tableName,
			@Param("paramName") String paramName);

	@DeleteProvider(type = DbJobBuilder.class, method = "deleteQueryBuilder")
	void actualizeStats_delete(@Param("tableName") String tableName, @Param("paramName") String paramName);

	@Delete("DELETE FROM T_CM_ENC_CASHOUT_STAT_DETAILS WHERE ENCASHMENT_ID IN ( "
			+ "SELECT ENCASHMENT_ID FROM T_CM_ENC_CASHOUT_STAT WHERE ENC_DATE >= sysdate )")
	void actualizeStats_deleteFromDetails();

	@Update("UPDATE t_cm_intgr_params SET LAST_TRANS_DATETIME = LAST_TRANS_DATETIME +#{shift}, "
			+ "CASS_CHECK_DATETIME = CASS_CHECK_DATETIME +#{shift}, LAST_DOWNTIME_DATETIME = LAST_DOWNTIME_DATETIME +#{shift}")
	void actualizeStats_updateParams(@Param("shift") Integer shift);
}
