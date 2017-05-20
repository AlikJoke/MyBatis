package ru.bpc.cm.integration.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

import ru.bpc.cm.integration.orm.builders.IntegrationBuilder;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса IntegrationController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 06.05.2017
 * @version 1.0.2
 *
 */
public interface IntegrationMapper extends IMapper {

	@Result(column = "last_trans_datetime", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT last_trans_datetime FROM t_cm_intgr_params")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> getLastStatDate();

	@Result(column = "last_utrnno", javaType = Long.class)
	@ResultType(Long.class)
	@Select("SELECT last_utrnno FROM t_cm_intgr_params")
	@Options(useCache = true, fetchSize = 1000)
	List<Long> getLastUtrnno();

	@Results({
			@Result(column = "last_utrnno", property = "first", javaType = Long.class),
			@Result(column = "last_trans_datetime", property = "second", javaType = Timestamp.class),
			@Result(column = "cass_check_datetime", property = "third", javaType = Timestamp.class)
	})
	@ResultType(TripleObject.class)
	@Select("SELECT last_utrnno, last_trans_datetime, cass_check_datetime FROM t_cm_intgr_params")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Long, Timestamp, Timestamp>> loadAtmTrans_getIntgrParams();
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = IntegrationBuilder.class, method = "simpleQueryBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getIntegerValueByQuery(@Param("query") String query);
	
	@Insert("INSERT INTO t_cm_intgr_trans (ATM_ID,UTRNNO,DATETIME,TRANS_TYPE_IND,BILL_RETRACT,BILL_REJECT, "
			+ "BILL_CASS1,DENOM_CASS1,CURRENCY_CASS1,TYPE_CASS1, "
			+ "BILL_CASS2,DENOM_CASS2,CURRENCY_CASS2,TYPE_CASS2, "
			+ "BILL_CASS3,DENOM_CASS3,CURRENCY_CASS3,TYPE_CASS3, "
			+ "BILL_CASS4,DENOM_CASS4,CURRENCY_CASS4,TYPE_CASS4) VALUES (#{atmId},#{operId},#{dt},#{operType},"
			+ "#{noteRetracted},"
			+ "#{noteRejected}, #{billCass1},#{denomCass1},#{currCass1},#{typeCass1}, "
			+ "#{billCass2},#{denomCass2},#{currCass2},#{typeCass2}, "
			+ "#{billCass3},#{denomCass3},#{currCass3},#{typeCass3}, "
			+ "#{billCass4},#{denomCass4},#{currCass4},#{typeCass4})")
	void loadAtmTrans_insertIntgrParams(@Param("atmId") Integer atmId, @Param("operId") Long operId,
			@Param("dt") Timestamp dt, @Param("operType") Integer operType,
			@Param("noteRetracted") Integer noteRetracted, @Param("noteRejected") Integer noteRejected,
			@Param("billCass1") Integer billCass1, @Param("denomCass1") Integer denomCass1,
			@Param("currCass1") Integer currCass1, @Param("typeCass1") Integer typeCass1,
			@Param("billCass2") Integer billCass2, @Param("denomCass2") Integer denomCass2,
			@Param("currCass2") Integer currCass2, @Param("typeCass2") Integer typeCass2,
			@Param("billCass3") Integer billCass3, @Param("denomCass3") Integer denomCass3,
			@Param("currCass3") Integer currCass3, @Param("typeCass3") Integer typeCass3,
			@Param("billCass4") Integer billCass4, @Param("denomCass4") Integer denomCass4,
			@Param("currCass4") Integer currCass4, @Param("typeCass4") Integer typeCass4);
	
	@Results({
			@Result(column = "res1", property = "key", javaType = Long.class),
			@Result(column = "res2", property = "value", javaType = Long.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT MIN(utrnno)-1 AS res1, MAX(utrnno) AS res2 FROM t_cm_intgr_trans")
	@Options(useCache = true)
	ObjectPair<Long, Long> loadAtmTrans_getIntgrLastUtrnno();
	
	@Results({
			@Result(column = "atm_id", property = "key", javaType = Integer.class),
			@Result(column = "datetime", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT atm_id, datetime FROM t_cm_intgr_trans t where t.utrnno = #{operId}")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> loadAtmTrans_getIntgrTransAtmIdDt(@Param("operId") Long operId);

	@Insert("INSERT INTO t_cm_intgr_trans_cash_in (ATM_ID,UTRNNO,DATETIME,TRANS_TYPE_IND, "
			+ "BILL_DENOM,BILL_CURR,BILL_NUM) VALUES (#{atmId},#{operId},#{dt},{operType}, #{denom},"
			+ "#{curr},#{num})")
	void loadAtmTrans_insertIntgrCashInTrans(@Param("atmId") Integer atmId, @Param("operId") Long operId,
			@Param("dt") Timestamp dt, @Param("operType") Integer operType, @Param("denom") Integer denom,
			@Param("curr") Integer curr, @Param("num") Integer num);
	
	@Result(column = "result", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@SelectProvider(type = IntegrationBuilder.class, method = "simpleQueryBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> getTimestampValueByQuery(@Param("query") String query);
	
	@Results({
			@Result(column = "LAST_UTRNNO", property = "key", javaType = Long.class),
			@Result(column = "LAST_TRANS_DATETIME", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("select COALESCE(min(LAST_UTRNNO),0) as LAST_UTRNNO, "
			+ "min(LAST_TRANS_DATETIME) as LAST_TRANS_DATETIME from T_CM_ATM_INTGR_LAST last, "
			+ "T_CM_ATM_INTGR_ERROR err where last.ATM_ID=err.ATM_ID")
	@Options(useCache = true, fetchSize = 1000)
	ObjectPair<Long, Timestamp> loadAtmErrorTrans_getMinStartTrans();
	
	@Delete("delete from T_CM_INTGR_TRANS where exists (select last.ATM_ID as ATM_ID, last.LAST_UTRNNO as LAST_UTRNNO, "
			+ "last.LAST_TRANS_DATETIME as LAST_TRANS_DATETIME from T_CM_ATM_INTGR_LAST last, T_CM_ATM_INTGR_ERROR err "
			+ "where last.ATM_ID=err.ATM_ID and T_CM_INTGR_TRANS.ATM_ID=last.ATM_ID and T_CM_INTGR_TRANS.utrnno<=last.LAST_UTRNNO "
			+ "and T_CM_INTGR_TRANS.datetime<=last.LAST_TRANS_DATETIME)")
	void clearExcessIntgrTrans();
	
	@Results({
			@Result(column = "res1", property = "key", javaType = Long.class),
			@Result(column = "res2", property = "value", javaType = Long.class)
	})
	@ResultType(ObjectPair.class)
	@SelectProvider(type = IntegrationBuilder.class, method = "loadAtmTrans_getIntgrLastUtrnnoComplexBuilder")
	@Options(useCache = true)
	List<ObjectPair<Long, Long>> loadAtmTrans_getIntgrLastUtrnnoComplex(@Param("vAtmList") List<Integer> vAtmList);

	@Result(column = "result", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT COALESCE(last_downtime_datetime,#{vMinStatDate}) as result FROM t_cm_intgr_params")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> loadAtmDowntime_getLastDownTime(@Param("vMinStatDate") Timestamp vMinStatDate);

	@Insert("INSERT INTO t_cm_intgr_downtime_period (PID,START_DATE,END_DATE,DOWNTIME_TYPE_IND) "
			+ "VALUES (#{pid},#{startDate},#{endDate},#{downType})")
	void loadAtmDowntime_insertIntgrDowntimePeriod(@Param("pid") Integer pid, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate, @Param("downType") Integer downType);

	@Results({
			@Result(column = "res1", property = "key", javaType = Integer.class),
			@Result(column = "res2", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT COALESCE(MAX(utrnno),0) as res1, COALESCE(MAX(datetime),CURRENT_TIMESTAMP) as res2 "
			+ "FROM t_cm_intgr_trans")
	@Options(useCache = true)
	ObjectPair<Integer, Timestamp> saveParams_getLastUtrnnoAndDatetime();

	@Insert("INSERT INTO t_cm_intgr_params (LAST_UTRNNO,LAST_DOWNTIME_DATETIME,LAST_TRANS_DATETIME) VALUES "
			+ "(#{vLastUtrnno},#{vLastDowntimeDatetime},#{vLastTransDatetime})")
	void saveParams_insert(@Param("vLastUtrnno") Integer vLastUtrnno,
			@Param("vLastDowntimeDatetime") Timestamp vLastDowntimeDatetime,
			@Param("vLastTransDatetime") Timestamp vLastTransDatetime);

	@Update("UPDATE t_cm_intgr_params SET LAST_UTRNNO = #{vLastUtrnno}, LAST_DOWNTIME_DATETIME = #{vLastDowntimeDatetime}, "
			+ "LAST_TRANS_DATETIME = #{vLastTransDatetime}")
	void saveParams_update(@Param("vLastUtrnno") Integer vLastUtrnno,
			@Param("vLastDowntimeDatetime") Timestamp vLastDowntimeDatetime,
			@Param("vLastTransDatetime") Timestamp vLastTransDatetime);

	@Results({
			@Result(column = "res1", property = "key", javaType = Integer.class),
			@Result(column = "res2", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT COALESCE(MAX(oper_id),0) as res1, COALESCE(MAX(datetime),CURRENT_TIMESTAMP) as res2 FROM t_cm_intgr_trans_md")
	@Options(useCache = true)
	ObjectPair<Integer, Timestamp> saveParamsMultiDisp_getLastUtrnnoAndDatetime();

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COUNT(1) as result FROM t_cm_inst WHERE id = #{id}")
	Integer loadInstList_selectCount(@Param("id") String id);

	@Insert("INSERT INTO t_cm_inst (id,description) VALUES (#{id},#{descx})")
	void loadInstList_insert(@Param("id") String id, @Param("descx") String descx);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COUNT(1) as result FROM t_cm_atm WHERE atm_id = #{atmId}")
	Integer loadAtmList_selectCount(@Param("atmId") Integer atmId);

	@Insert("INSERT INTO t_cm_atm (atm_id,state,city,street,inst_id,external_atm_id,name) VALUES "
			+ "(#{atmId},#{state},#{city},#{street},COALESCE(#{instId},'9999'),#{extId},#{name})")
	void loadAtmList_insert(@Param("atmId") Integer atmId, @Param("state") String state, @Param("city") String city,
			@Param("street") String street, @Param("instId") String instId, @Param("extId") String extId,
			@Param("name") String name);

	@Update("UPDATE t_cm_atm set state = #{state}, city = #{city}, street = #{street}, "
			+ "name = #{name}, inst_Id = COALESCE(#{instId},'9999'), external_atm_id = #{extId} "
			+ "WHERE atm_id = #{atmId}")
	void loadAtmList_update(@Param("atmId") Integer atmId, @Param("state") String state, @Param("city") String city,
			@Param("street") String street, @Param("instId") String instId, @Param("extId") String extId,
			@Param("name") String name);

	@Insert("INSERT INTO t_cm_curr_convert_rate "
			+ "(CNVT_DATE,SRC_CURR_CODE,DEST_CURR_CODE,CNVT_RATE,MULTIPLE_FLAG,SRC_INST_ID,DEST_INST_ID) VALUES "
			+ "(#{cnvtDate},#{srcCurrCode},#{destCurrCode},#{rate},#{multipleFlag},#{srcInstId},#{destInstId})")
	void loadCurrencyConvertRates_insert(@Param("cnvtDate") Timestamp cnvtDate,
			@Param("srcCurrCode") Integer srcCurrCode, @Param("destCurrCode") Integer destCurrCode,
			@Param("rate") Float rate, @Param("multipleFlag") String multipleFlag, @Param("srcInstId") String srcInstId,
			@Param("destInstId") String destInstId);

	@Delete("DELETE FROM T_CM_CURR_CONVERT_RATE rates WHERE rates.CNVT_DATE <  "
			+ "(SELECT  MAX (CNVT_DATE) FROM   T_CM_CURR_CONVERT_RATE cr "
			+ "WHERE rates.SRC_CURR_CODE = cr.SRC_CURR_CODE AND rates.DEST_CURR_CODE = cr.DEST_CURR_CODE "
			+ "AND rates.SRC_INST_ID = cr.SRC_INST_ID AND rates.DEST_INST_ID = cr.DEST_INST_ID)")
	void loadCurrencyConvertRates_delete();

	@Insert("INSERT INTO T_CM_INTGR_CASS_BALANCE "
			+ "(ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_REMAINING_LOAD, BALANCE_STATUS) VALUES "
			+ "(#{atmId},#{cassType},#{cassNumber},#{load},#{status})")
	void loadAtmCassettesBalances_insertCassBalance(@Param("atmId") Integer atmId, @Param("cassType") Integer cassType,
			@Param("cassNumber") Integer cassNumber, @Param("load") Integer load, @Param("status") Integer status);

	@Insert("INSERT INTO t_cm_intgr_params (CASS_CHECK_DATETIME) VALUES (#{dt})")
	void loadAtmCassettesBalances_insertIntgrParams(@Param("dt") Timestamp dt);

	@Update("UPDATE t_cm_intgr_params SET CASS_CHECK_DATETIME = #{dt}")
	void loadAtmCassettesBalances_updateIntgrParams(@Param("dt") Timestamp dt);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(1) as result "
			+ "FROM T_CM_ATM_CASSETTES where ATM_ID=#{atmId} and CASS_NUMBER=#{cassNumber} ")
	Integer loadAtmCassettesStatuses_selectCount(@Param("atmId") Integer atmId,
			@Param("cassNumber") Integer cassNumber);

	@Update("UPDATE T_CM_ATM_CASSETTES set CASS_STATE = #{cassState}, CASS_IS_PRESENT = 1 where "
			+ "ATM_ID = #{atmId} AND CASS_TYPE = #{cassType} AND CASS_NUMBER = #{cassNumber}")
	void loadAtmCassettesStatuses_updateCassetes(@Param("atmId") Integer atmId, @Param("cassNumber") Integer cassNumber,
			@Param("cassState") Integer cassState, @Param("cassType") Integer cassType);

	@Insert("INSERT INTO T_CM_ATM_CASSETTES "
			+ "(ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_CURR, CASS_VALUE,CASS_STATE, CASS_IS_PRESENT) " + "VALUES "
			+ "(#{atmId},#{cassType},#{cassNumber},0,0,#{cassState}, 1)")
	void loadAtmCassettesStatuses_insertCassetes(@Param("atmId") Integer atmId, @Param("cassType") Integer cassType,
			@Param("cassNumber") Integer cassNumber, @Param("cassState") Integer cassState);

	@Delete("DELETE FROM T_CM_ATM_CASSETTES WHERE CASS_IS_PRESENT = 0")
	void loadAtmCassettesStatuses_delete();
}
