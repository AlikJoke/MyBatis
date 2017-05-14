package ru.bpc.cm.integration.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

import ru.bpc.cm.integration.orm.builders.DataLoadBuilder;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса DataLoadController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 30.04.2017
 * @version 1.0.1
 *
 */
public interface DataLoadMapper extends IMapper {

	@Result(column = "VCHECK", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COUNT(1) as vCheck FROM t_cm_atm WHERE atm_id = #{atmId}")
	@Options(useCache = true)
	Integer insertAtms_selectCount(@Param("atmId") Integer atmId);

	@Insert("INSERT INTO T_CM_ATM(atm_id,street,city,state,inst_id,external_atm_id,name) "
			+ "VALUES(#{atmId}, #{street}, #{city}, #{state}, #{instId}, #{extId}, #{name})")
	void insertAtms_insert(@Param("atmId") Integer atmId, @Param("street") String street, @Param("city") String city,
			@Param("state") String state, @Param("instId") Integer instId, @Param("extId") String extId,
			@Param("name") String name);

	@Update("UPDATE T_CM_ATM SET STATE= #{state} , CITY = #{city} ,STREET = #{street} ,"
			+ "NAME = #{name} ,INST_ID = #{instId}, external_atm_id = #{extId}  WHERE atm_id = #{atmId}")
	void insertAtms_update(@Param("atmId") String atmId, @Param("street") String street, @Param("city") String city,
			@Param("state") String state, @Param("instId") Integer instId, @Param("extId") String extId,
			@Param("name") String name);

	@Result(column = "VCHECK", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COUNT(1) as vCheck FROM t_cm_inst WHERE id = #{id}")
	@Options(useCache = true)
	Integer insertInsts_selectCount(@Param("id") String id);

	@Insert("INSERT INTO t_cm_inst (id, description) VALUES (#{id}, #{descx})")
	void insertInsts_insert(@Param("id") String id, @Param("descx") String descx);

	@Insert("INSERT INTO T_CM_CURR_CONVERT_RATE"
			+ "(CNVT_RATE, SRC_CURR_CODE, DEST_CURR_CODE, CNVT_DATE, MULTIPLE_FLAG,SRC_INST_ID,DEST_INST_ID) "
			+ "VALUES(#{rate},#{srcCurr},#{dstCurr},#{effDate},#{flag},#{instId},#{instId})")
	void insertCurrencyCnvtRates(@Param("rate") Double rate, @Param("srcCurr") Integer srcCurr,
			@Param("dstCurr") Integer dstCurr, @Param("effDate") Timestamp effDate, @Param("flag") String flag,
			@Param("instId") String instId);

	@Insert("INSERT INTO T_CM_INTGR_DOWNTIME_PERIOD (PID, START_DATE, END_DATE, DOWNTIME_TYPE_IND) "
			+ "VALUES(#{pid},#{startDate},#{endDate},#{type})")
	void insertDowntimes(@Param("pid") Integer pid, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate, @Param("type") Integer type);

	@Insert("INSERT INTO t_cm_intgr_trans_md (TERMINAL_ID,OPER_ID,DATETIME,OPER_TYPE, AMOUNT,"
			+ "NOTE_RETRACTED,NOTE_REJECTED) VALUES (#{pid},#{operId},#{dt}, #{type},#{amount}, #{noteRetracted},#{noteRejected})")
	void insertTransactionsMultiDisp_heads(@Param("pid") Integer pid, @Param("operId") String operId,
			@Param("dt") Timestamp dt, @Param("type") Integer type, @Param("amount") Long amount,
			@Param("noteRetracted") Integer noteRetracted, @Param("noteRejected") Integer noteRejected);

	@Insert("INSERT INTO t_cm_intgr_trans_md_disp (OPER_ID,CASS_NUMBER,CASS_TYPE,FACE,CURRENCY, "
			+ "NOTE_DISPENSED, NOTE_REMAINED) VALUES (#{operId}, #{cassNumber}, #{cassType}, #{face}, #{curr},#{dispensed}, #{remained})")
	void insertTransactionsMultiDisp_disps(@Param("operId") Integer operId, @Param("cassNumber") Integer cassNumber,
			@Param("cassType") Integer cassType, @Param("face") Integer face, @Param("curr") Integer curr,
			@Param("dispensed") Integer dispensed, @Param("remained") Integer remained);

	@Result(column = "VCHECK", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COUNT(1) as vCheck FROM T_CM_ATM_CASSETTES WHERE ATM_ID = #{atmId} AND CASS_TYPE = #{cassType} "
			+ "AND CASS_NUMBER = #{cassNumber} ")
	@Options(useCache = true)
	Integer insertAtmCassStatuses_selectCount(@Param("atmId") Integer atmId, @Param("cassType") Integer cassType,
			@Param("cassNumber") Integer cassNumber);

	@Insert("INSERT INTO "
			+ "T_CM_ATM_CASSETTES(ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_CURR, CASS_VALUE,CASS_STATE, CASS_IS_PRESENT) "
			+ "VALUES(#{atmId}, #{cassType}, #{cassNumber}, 0 , 0 , #{cassState} , 1)")
	void insertAtmCassStatuses_insert(@Param("atmId") Integer atmId, @Param("cassType") Integer cassType,
			@Param("cassNumber") Integer cassNumber, @Param("cassState") Integer cassState);

	@Update("UPDATE T_CM_ATM_CASSETTES SET CASS_STATE = {cassState}, CASS_IS_PRESENT = 1 WHERE "
			+ "ATM_ID = #{atmId} AND CASS_TYPE = #{cassType} AND CASS_NUMBER = #{cassNumber} ")
	void insertAtmCassStatuses_update(@Param("atmId") Integer atmId, @Param("cassType") Integer cassType,
			@Param("cassNumber") Integer cassNumber, @Param("cassState") Integer cassState);

	@Result(column = "VCHECK", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COUNT(1) as vCheck FROM t_cm_intgr_params ")
	@Options(useCache = true)
	Integer insertAtmCassBalances_selectCount();

	@Insert("INSERT INTO t_cm_intgr_params (CASS_CHECK_DATETIME) VALUES (#{dt}) ")
	void insertAtmCassBalances_insertParams(@Param("dt") Timestamp dt);

	@Update("UPDATE t_cm_intgr_params SET CASS_CHECK_DATETIME = #{dt} ")
	void insertAtmCassBalances_update(@Param("dt") Timestamp dt);

	@Insert("INSERT INTO "
			+ "T_CM_INTGR_CASS_BALANCE(ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_REMAINING_LOAD, BALANCE_STATUS) "
			+ "VALUES(#{atmId}, #{cassType}, #{cassNumber}, #{cassLoad}, {status})")
	void insertAtmCassStatuses_insertBalance(@Param("atmId") Integer atmId, @Param("cassType") Integer cassType,
			@Param("cassNumber") Integer cassNumber, @Param("cassLoad") Integer cassLoad,
			@Param("status") Integer status);

	@DeleteProvider(type = DataLoadBuilder.class, method = "truncateTimestampBuilder")
	void truncateTrans(@Param("tableName") String tableName, @Param("dateField") String dateField,
			@Param("dateFrom") Timestamp dateFrom, @Param("dateTo") Timestamp dateTo);

	@DeleteProvider(type = DataLoadBuilder.class, method = "deleteQueryBuilder")
	void deleteOldStats_delete(@Param("tableName") String tableName, @Param("atmId") Integer atmId,
			@Param("encId") Integer encId);

	@Delete("DELETE FROM t_cm_enc_cashout_stat_details WHERE "
			+ "encashment_id in (select encashment_id from t_cm_enc_cashout_stat where atm_id=#{atmId}) AND "
			+ "cash_in_encashment_id < #{encId}")
	void deleteOldStats_deleteDetails(@Param("atmId") Integer atmId, @Param("encId") Integer encId);
	
	@Delete("DELETE FROM t_cm_enc_cashin_stat_details WHERE "
			+ "cash_in_encashment_id in (select cash_in_encashment_id from t_cm_enc_cashin_stat where atm_id=#{atmId}) AND "
			+ "encashment_id < #{encId}")
	void deleteOldStats_deleteDetailsCashIn(@Param("atmId") Integer atmId, @Param("encId") Integer encId);

	@Results({
			@Result(column = "atm_id", property = "key", javaType = Integer.class),
			@Result(column = "encashment_id", property = "value", javaType = Integer.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT MAX(encashment_id) as encashment_id,atm_id  FROM t_cm_enc_cashout_stat "
			+ " WHERE enc_date < #{dt}  GROUP BY atm_id")
	List<ObjectPair<Integer, Integer>> deleteOldStats_selectMaxEncId(@Param("dt") Timestamp dt);

	@Results({
			@Result(column = "atm_id", property = "key", javaType = Integer.class),
			@Result(column = "encashment_id", property = "value", javaType = Integer.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT MAX(cash_in_encashment_id) as encashment_id,atm_id  FROM t_cm_enc_cashin_stat "
			+ " WHERE cash_in_enc_date < #{dt}  GROUP BY atm_id")
	List<ObjectPair<Integer, Integer>> deleteOldStats_selectMaxCashEncId(@Param("dt") Timestamp dt);

	@Insert("INSERT INTO t_cm_intgr_params  (LAST_UTRNNO,LAST_DOWNTIME_DATETIME,LAST_TRANS_DATETIME)  VALUES "
			+ " (#{lastUtrnno},#{lastDowntimeDatetime},#{lastTransDatetime})")
	void saveParams_insert(@Param("lastUtrnno") Long lastUtrnno,
			@Param("lastDowntimeDatetime") Timestamp lastDowntimeDatetime,
			@Param("lastTransDatetime") Timestamp lastTransDatetime);

	@Update("UPDATE t_cm_intgr_params SET LAST_UTRNNO = #{lastUtrnno}, "
			+ "LAST_DOWNTIME_DATETIME = #{lastDowntimeDatetime}, LAST_TRANS_DATETIME = #{lastTransDatetime}")
	void saveParams_update(@Param("lastUtrnno") Long lastUtrnno,
			@Param("lastDowntimeDatetime") Timestamp lastDowntimeDatetime,
			@Param("lastTransDatetime") Timestamp lastTransDatetime);

	@Results({
			@Result(column = "lastUtrnno", property = "key", javaType = Long.class),
			@Result(column = "lastTransDatetime", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT COALESCE(MAX(utrnno),0) as lastUtrnno ,COALESCE(MAX(datetime),CURRENT_TIMESTAMP) as lastTransDatetime "
			+ " FROM t_cm_intgr_trans")
	List<ObjectPair<Long, Timestamp>> saveParams_selectLastTransInfo();
	
	@Results({
			@Result(column = "LAST_UTRNNO", property = "key", javaType = Long.class),
			@Result(column = "LAST_TRANS_DATETIME", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("select LAST_UTRNNO, LAST_TRANS_DATETIME from t_cm_intgr_params")
	List<ObjectPair<Long, Timestamp>> saveParams_selectPairs();

	@Results({
			@Result(column = "lastUtrnno", property = "key", javaType = Long.class),
			@Result(column = "lastTransDatetime", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@SelectProvider(type = DataLoadBuilder.class, method = "saveParamsForTaskBuilder_lastTransInfo")
	List<ObjectPair<Long, Timestamp>> saveParamsForTask_lastTransInfo();
	
	@Results({
			@Result(column = "lastUtrnno", property = "key", javaType = Long.class),
			@Result(column = "lastTransDatetime", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT COALESCE(MAX(oper_id),0) as lastUtrnno ,COALESCE(MAX(datetime),CURRENT_TIMESTAMP) as lastTransDatetime "
			+ " FROM t_cm_intgr_trans_md")
	List<ObjectPair<Long, Timestamp>> saveParamsMultiDisp_lastTransInfo();

}
