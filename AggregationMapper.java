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

import ru.bpc.cm.integration.AggregationController;
import ru.bpc.cm.integration.orm.builders.AggregationBuilder;
import ru.bpc.cm.integration.orm.builders.IntegrationBuilder;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.orm.items.MultiObject;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса AggregationController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 06.05.2017
 * @version 1.0.1
 *
 */
public interface AggregationMapper extends IMapper {

	@Result(column = "result", javaType = String.class)
	@ResultType(String.class)
	@SelectProvider(type = IntegrationBuilder.class, method = "simpleQueryBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<String> getStringValueByQuery(@Param("query") String query);
	
	@Results({
			@Result(column = "ENCASHMENT_ID", property = "key", javaType = Integer.class),
			@Result(column = "ENC_DATE", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT ENC_DATE,ENCASHMENT_ID  FROM T_CM_ENC_CASHOUT_STAT  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID >= #{encId}  ORDER BY ENC_DATE")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> aggregateCashOut_getCoEncStat(@Param("atmId") String atmId,
			@Param("encId") Integer encId);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(MAX(ENCASHMENT_ID),0) as result  FROM T_CM_ENC_CASHOUT_STAT  WHERE ATM_ID = #{atmId}")
	@Options(useCache = true, fetchSize = 1000)
	Integer aggregateCashOut_getLastEncId(@Param("atmId") String atmId);
	
	@Result(column = "ENC_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT COALESCE(MIN(DISTINCT ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE  FROM T_CM_ENC_CASHOUT_STAT "
			+ " WHERE ATM_ID = #{atmId}  AND ENC_DATE > #{encDate}")
	@Options(useCache = true, fetchSize = 1000)
	Timestamp aggregateCashOut_getNextIncass(@Param("atmId") String atmId, @Param("encDate") Timestamp encDate);
	
	@DeleteProvider(type = AggregationBuilder.class, method = "deleteCashOutQueryBuilder")
	void simpleDeleteCashOutQuery(@Param("tableName") String tableName, @Param("atmId") String atmId,
			@Param("encId") Integer encId, @Param("statDate") Timestamp statDate);

	@Results({
			@Result(column = "ENCASHMENT_ID", property = "key", javaType = Integer.class),
			@Result(column = "ENC_DATE", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT CASH_IN_ENC_DATE as ENC_DATE ,CASH_IN_ENCASHMENT_ID as ENCASHMENT_ID "
			+ "FROM T_CM_ENC_CASHIN_STAT  WHERE ATM_ID = #{atmId}  AND CASH_IN_ENCASHMENT_ID >= #{encId} "
			+ " ORDER BY CASH_IN_ENC_DATE")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> aggregateCashIn_getCiEncStat(@Param("atmId") String atmId,
			@Param("encId") Integer encId);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(MAX(CASH_IN_ENCASHMENT_ID),0) as result  FROM T_CM_ENC_CASHIN_STAT "
			+ " WHERE ATM_ID = #{atmId}")
	@Options(useCache = true, fetchSize = 1000)
	Integer aggregateCashIn_getLastEncId(@Param("atmId") String atmId);
	
	@Result(column = "ENC_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT COALESCE(MIN(DISTINCT CASH_IN_ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE "
			+ " FROM T_CM_ENC_CASHIN_STAT  WHERE ATM_ID = #{atmId}  AND CASH_IN_ENC_DATE > #{encDate}")
	@Options(useCache = true, fetchSize = 1000)
	Timestamp aggregateCashIn_getNextIncass(@Param("atmId") String atmId, @Param("encDate") Timestamp encDate);
	
	@DeleteProvider(type = AggregationBuilder.class, method = "deleteCashInQueryBuilder")
	void simpleDeleteCashInQuery(@Param("tableName") String tableName, @Param("atmId") String atmId,
			@Param("encId") Integer encId, @Param("statDate") Timestamp statDate);
	
	@Results({
			@Result(column = "PID", property = "first", javaType = String.class),
			@Result(column = "START_DATE", property = "second", javaType = Timestamp.class),
			@Result(column = "ENC_DATE", property = "third", javaType = Timestamp.class),
	})
	@ResultType(TripleObject.class)
	@Select("select PID,START_DATE,COALESCE(END_DATE, CURRENT_TIMESTAMP) as END_DATE "
			+ "FROM t_cm_intgr_downtime_period WHERE DOWNTIME_TYPE_IND in (#{offlineType}, #{cashType})")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<String, Timestamp, Timestamp>> prepareDowntimes(@Param("offlineType") Integer offlineType,
			@Param("cashType") Integer cashType);
	
	@Results({
			@Result(column = "PID", property = "first", javaType = String.class),
			@Result(column = "START_DATE", property = "second", javaType = Timestamp.class),
			@Result(column = "ENC_DATE", property = "third", javaType = Timestamp.class),
	})
	@ResultType(TripleObject.class)
	@SelectProvider(type = AggregationBuilder.class, method = "prepareDowntimesComplexBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<String, Timestamp, Timestamp>> prepareDowntimesComplex(@Param("atmList") List<Integer> atmList,
			@Param("offlineType") Integer offlineType, @Param("cashType") Integer cashType);
	
	@Result(column = "CASH_ADD_ENCASHMENT", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT CASH_ADD_ENCASHMENT FROM T_CM_ENC_CASHOUT_STAT WHERE ENCASHMENT_ID = #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> checkEncashmentCashAdd(@Param("encId") Integer encId);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(1) as resultFROM T_CM_ENC_CASHIN_STAT_DETAILS "
			+ "WHERE CASH_IN_ENCASHMENT_ID = #{encId} and ACTION_TYPE = #{actionType} and CASS_COUNT > 0")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> checkEncashmentCashInPreload(@Param("encId") Integer encId, @Param("actionType") Integer actionType);
	
	@Results({
			@Result(column = "cash_add_encashment", property = "first", javaType = Integer.class),
			@Result(column = "bills", property = "second", javaType = Integer.class),
			@Result(column = "curr", property = "third", javaType = Integer.class),
			@Result(column = "denom", property = "fourth", javaType = Integer.class),
			@Result(column = "cass_num", property = "fifth", javaType = Integer.class),
			@Result(column = "enc_date", property = "sixth", javaType = Timestamp.class)
	})
	@ResultType(MultiObject.class)
	@Select("select bills as bills, curr,denom, d as enc_date,cass_num,  CASE  WHEN trans_type_ind = "
			+ AggregationController.CO_719_ENC_TRANSACTION_TYPE + " THEN 0  WHEN trans_type_ind = "
			+ AggregationController.CO_743_ENC_TRANSACTION_TYPE + " THEN 0  WHEN trans_type_ind = "
			+ AggregationController.CO_CA_ENC_TRANSACTION_TYPE + " THEN 1  ELSE 0 END as cash_add_encashment "
			+ " FROM( "
			+ " select BILL_CASS1 as BILLS,DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR,1 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_719_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS1 > 0 "
			+ " and CURRENCY_CASS1 > 0  union all "
			+ " select BILL_CASS2 as BILLS,DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR,2 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_719_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS2 > 0 "
			+ " and CURRENCY_CASS2 > 0  union all "
			+ " select BILL_CASS3 as BILLS,DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR,3 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_743_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS3 > 0 "
			+ " and CURRENCY_CASS3 > 0  union all "
			+ " select BILL_CASS4 as BILLS,DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR,4 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_743_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS4 > 0 "
			+ " and CURRENCY_CASS4 > 0  union all "
			+ " select BILL_CASS5 as BILLS,DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR,5 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_743_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS5 > 0 "
			+ " and CURRENCY_CASS5 > 0  union all "
			+ " select BILL_CASS6 as BILLS,DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR,6 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_743_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS6 > 0 "
			+ " and CURRENCY_CASS6 > 0  union all "
			+ " select BILL_CASS7 as BILLS,DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR,7 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_743_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS7 > 0 "
			+ " and CURRENCY_CASS7 > 0  union all "
			+ " select BILL_CASS8 as BILLS,DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR,8 as CASS_NUM,trans_type_ind "
			+ " from t_cm_intgr_trans  where trans_type_ind in( " + AggregationController.CO_743_ENC_TRANSACTION_TYPE
			+ ", " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and DENOM_CASS8 > 0 "
			+ " and CURRENCY_CASS8 > 0  order by d  ) ORDER BY ENC_DATE,CASS_NUM,trans_type_ind")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Integer, Integer, Integer, Integer, Integer, Timestamp, ?, ?, ?, ?>> insertEncashments_check(
			@Param("pPid") String pPid);
	
	@Results({
			@Result(column = "ENCASHMENT_ID", property = "key", javaType = Integer.class),
			@Result(column = "ENC_DATE", property = "value", javaType = Timestamp.class)
	})
	@Select("select st.ENCASHMENT_ID,st.ENC_DATE from t_cm_enc_cashout_stat st where "
			+ "st.atm_id = #{pPid} and abs(dateDiffMin(st.ENC_DATE, #{encDate})) < 15")
	@ResultType(ObjectPair.class)
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> insertEncashments_select(@Param("pPid") String pPid,
			@Param("encDate") Timestamp encDate);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = IntegrationBuilder.class, method = "simpleQueryBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getIntegerValueByQuery(@Param("query") String query);

	@Insert("INSERT INTO T_CM_ENC_CASHOUT_STAT(ATM_ID,ENCASHMENT_ID,ENC_DATE) VALUES(#{atmId}, #{encId}, #{encDate})")
	void insertEncashments_insertStat(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("encDate") Timestamp encDate);

	@Insert("INSERT INTO T_CM_ENC_CASHOUT_STAT(ATM_ID,ENCASHMENT_ID,ENC_DATE,CASH_ADD_ENCASHMENT) "
			+ "VALUES(#{atmId}, #{encId}, #{encDate}, #{cashAdd})")
	void insertEncashments_insertStatFull(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("encDate") Timestamp encDate, @Param("cashAdd") Integer cashAdd);
	
	@Insert("INSERT INTO "
			+ "t_cm_enc_cashout_stat_details(ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) "
			+ "VALUES(#{encId}, #{cassValue}, #{cassCurr}, #{cassCount}, #{actionType}, #{cassNumber})")
	void insertEncashments_insertDetails(@Param("encId") Integer encId, @Param("cassValue") Integer cassValue,
			@Param("cassCurr") Integer cassCurr, @Param("cassCount") Integer cassCount,
			@Param("actionType") Integer actionType, @Param("cassNumber") Integer cassNumber);

	@Update("UPDATE t_cm_enc_cashout_stat_details  SET CASS_CURR = #{cassCurr},  CASS_VALUE = #{cassValue}, "
			+ " CASS_COUNT = #{cassCount}  WHERE  CASS_NUMBER = #{cassNumber}  AND "
			+ " ENCASHMENT_ID = #{encId}  AND  ACTION_TYPE = #{actionType} ")
	void insertEncashments_updateDetails(@Param("encId") Integer encId, @Param("cassValue") Integer cassValue,
			@Param("cassCurr") Integer cassCurr, @Param("cassCount") Integer cassCount,
			@Param("actionType") Integer actionType, @Param("cassNumber") Integer cassNumber);
	
	@Results({
			@Result(column = "cash_add_encashment", property = "first", javaType = Integer.class),
			@Result(column = "bills", property = "second", javaType = Integer.class),
			@Result(column = "curr", property = "third", javaType = Integer.class),
			@Result(column = "denom", property = "fourth", javaType = Integer.class),
			@Result(column = "cass_num", property = "fifth", javaType = Integer.class),
			@Result(column = "enc_date", property = "sixth", javaType = Timestamp.class)
	})
	@ResultType(MultiObject.class)
	@Select("select sum(bills) as bills,truncToMinute(d) as d , curr,denom, min(d) as enc_date,cass_num,  CASE "
			+ " WHEN trans_type_ind = " + AggregationController.CO_719_ENC_TRANSACTION_TYPE + " THEN 0 "
			+ " WHEN trans_type_ind = " + AggregationController.CO_743_ENC_TRANSACTION_TYPE + " THEN 0 "
			+ " WHEN trans_type_ind = " + AggregationController.CO_CA_ENC_TRANSACTION_TYPE + " THEN 1 "
			+ " ELSE 0 END as cash_add_encashment  FROM( "
			+ " select itmd.note_dispensed as BILLS,DATETIME as d,itmd.face as denom, itmd.currency as CURR,itmd.disp_number as CASS_NUM,oper_type as trans_type_ind "
			+ " from t_cm_intgr_trans_md itm  join t_cm_intgr_trans_md_disp itmd on (itm.oper_id = itmd.oper_id) "
			+ " where oper_type in( " + AggregationController.CO_719_ENC_TRANSACTION_TYPE + ", "
			+ AggregationController.CO_743_ENC_TRANSACTION_TYPE + ", "
			+ AggregationController.CO_CA_ENC_TRANSACTION_TYPE + ")  and itm.terminal_id = #{pPid} "
			+ " and itmd.note_dispensed > 0  order by d  ) GROUP BY truncToMinute(d),curr,denom,cass_num "
			+ " ORDER BY ENC_DATE,CASS_NUM ")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Integer, Integer, Integer, Integer, Integer, Timestamp, ?, ?, ?, ?>> insertEncashmentsMd_check(
			@Param("pPid") String pPid);

	@Insert("INSERT INTO T_CM_ENC_CASHOUT_STAT(ATM_ID,ENCASHMENT_ID,ENC_DATE,CASH_ADD_ENCASHMENT) "
			+ "VALUES(#{atmId}, #{encId}, #{encDate}, #{cashAdd, jdbcType=INTEGER})")
	void insertEncashmentsMd_insertStatFull(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("encDate") Timestamp encDate, @Param("cashAdd") Integer cashAdd);
	
	@Insert("INSERT INTO "
			+ "t_cm_enc_cashout_stat_details(ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) "
			+ "VALUES(#{encId}, #{cassValue}, #{cassCurr}, #{cassCount}, #{actionType}, #{cassNumber})")
	void insertEncashmentsMd_insertDetails(@Param("encId") Integer encId, @Param("cassValue") Integer cassValue,
			@Param("cassCurr") Integer cassCurr, @Param("cassCount") Integer cassCount,
			@Param("actionType") Integer actionType, @Param("cassNumber") Integer cassNumber);
	
	@Results({
			@Result(column = "trans_date", property = "first", javaType = Timestamp.class),
			@Result(column = "denom", property = "second", javaType = Integer.class),
			@Result(column = "bills", property = "third", javaType = Integer.class),
			@Result(column = "TRANS_COUNT", property = "fourth", javaType = Integer.class),
			@Result(column = "curr", property = "fifth", javaType = Integer.class),
			@Result(column = "cass_num", property = "sixth", javaType = Integer.class),
			@Result(column = "AVAIL_COEFF", property = "seventh", javaType = Integer.class)
	})
	@ResultType(MultiObject.class)
	@Select("select  cs.PID as PID, cs.BILLS,cs.trans_count,cs.trans_date , cs.denom, cs.CURR,cs.CASS_NUM, "
			+ " COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF  FROM ( "
			+ " select #{pPid} as PID,sum(BILLS) as BILLS,count(1) as trans_count,truncToHour(d) as trans_date , denom, CURR, CASS_NUM "
			+ " FROM( "
			+ " select BILL_CASS1 as BILLS,DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR,1 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS1 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate} and TYPE_CASS1 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " union all "
			+ " select BILL_CASS2 as BILLS,DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR,2 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS2 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate} and TYPE_CASS2 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " union all "
			+ " select BILL_CASS3 as BILLS,DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR,3 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS3 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate} and TYPE_CASS3 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " union all "
			+ " select BILL_CASS4 as BILLS,DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR,4 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS4 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate} and TYPE_CASS4 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " union all "
			+ " select BILL_CASS5 as BILLS,DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR,5 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS5 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate} and TYPE_CASS5 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " union all "
			+ " select BILL_CASS6 as BILLS,DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR,6 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS6 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS6 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " union all "
			+ " select BILL_CASS7 as BILLS,DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR,7 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS7 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate} and TYPE_CASS7 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " union all "
			+ " select BILL_CASS8 as BILLS,DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR,8 as CASS_NUM "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ " ," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS8 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate} and TYPE_CASS8 = " + AggregationController.CASS_TYPE_CASH_OUT
			+ " ) GROUP BY truncToHour(d),denom, CURR,CASS_NUM  ORDER BY trans_date,CASS_NUM) cs "
			+ " left outer join t_cm_intgr_downtime_cashout ds on (cs.PID = ds.PID and cs.trans_date = ds.stat_date)")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Timestamp, Integer, Integer, Integer, Integer, Integer, Integer, ?, ?, ?>> insertCassStat_check(
			@Param("pPid") String pPid, @Param("pEncDate") Timestamp pEncDate, @Param("pNextEncDate") Timestamp pNextEncDate);
	
	@Insert("INSERT INTO "
			+ "T_CM_CASHOUT_CASS_STAT(ATM_ID,STAT_DATE,ENCASHMENT_ID,CASS_VALUE,CASS_COUNT,CASS_TRANS_COUNT,CASS_CURR,CASS_NUMBER,AVAIL_COEFF) "
			+ "VALUES(#{atmId}, #{statDate}, #{encId}, #{cassValue}, #{cassCount}, #{transCount},#{curr}, #{cassNumber}, #{coeff})")
	void insertCassStat_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("cassValue") Integer denom, @Param("cassCount") Integer bills,
			@Param("transCount") Integer transCount, @Param("curr") Integer curr,
			@Param("cassNumber") Integer cassNumber, @Param("coeff") Integer coeff);
	
	@Update("UPDATE T_CM_CASHOUT_CASS_STAT  SET CASS_COUNT = CASS_COUNT + #{cassCount}, "
			+ " CASS_TRANS_COUNT = CASS_TRANS_COUNT + #{transCount},  AVAIL_COEFF = #{coeff} WHERE "
			+ "ATM_ID = #{atmId} AND STAT_DATE = #{statDate} AND ENCASHMENT_ID = #{encId} AND "
			+ "CASS_NUMBER = #{cassNumber} AND CASS_VALUE =  #{cassValue} AND CASS_CURR = #{curr}")
	void insertCassStat_update(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("cassValue") Integer denom, @Param("cassCount") Integer bills,
			@Param("transCount") Integer transCount, @Param("curr") Integer curr,
			@Param("cassNumber") Integer cassNumber, @Param("coeff") Integer coeff);
	
	@Results({
			@Result(column = "trans_date", property = "first", javaType = Timestamp.class),
			@Result(column = "denom", property = "second", javaType = Integer.class),
			@Result(column = "bills", property = "third", javaType = Integer.class),
			@Result(column = "TRANS_COUNT", property = "fourth", javaType = Integer.class),
			@Result(column = "curr", property = "fifth", javaType = Integer.class),
			@Result(column = "cass_num", property = "sixth", javaType = Integer.class),
			@Result(column = "AVAIL_COEFF", property = "seventh", javaType = Integer.class),
			@Result(column = "BILLS_REMAINING", property = "eighth", javaType = Integer.class)
	})
	@ResultType(MultiObject.class)
	@Select("select cs.PID as PID, cs.BILLS,cs.trans_count,cs.trans_date , cs.denom, cs.CURR,cs.CASS_NUM, "
			+ " COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF, cs.BILLS_REMAINING  FROM ( "
			+ " select #{pPid} as PID,sum(itmd.note_dispensed) as BILLS, min(itmd.note_remained) as BILLS_REMAINING,count(1) as trans_count,truncToHour(itm.datetime) as trans_date , "
			+ " itmd.face as denom, itmd.currency as CURR,itmd.disp_number as CASS_NUM "
			+ " FROM t_cm_intgr_trans_md itm  join t_cm_intgr_trans_md_disp itmd on (itm.oper_id = itmd.oper_id)"
			+ " where oper_type in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE + "," + AggregationController.DEBIT_TRANSACTION_TYPE + ") "
			+ "and itm.terminal_id = #{pPid} and itmd.note_dispensed > 0 and itm.datetime between #{pEncDate} and #{pNextEncDate} "
			+ "GROUP BY truncToHour(itm.datetime),itmd.face, itmd.currency, itmd.disp_number "
			+ "ORDER BY trans_date,CASS_NUM) cs "
			+ "left outer join t_cm_intgr_downtime_cashout ds on (cs.PID = ds.PID and cs.trans_date = ds.stat_date) ")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Timestamp, Integer, Integer, Integer, Integer, Integer, Integer, Integer, ?, ?>> insertCassStatMd_check(
			@Param("pPid") String pPid, @Param("pEncDate") Timestamp pEncDate,
			@Param("pNextEncDate") Timestamp pNextEncDate);

	@Insert("INSERT INTO "
			+ "T_CM_CASHOUT_CASS_STAT(ATM_ID,STAT_DATE,ENCASHMENT_ID,CASS_VALUE,CASS_COUNT,CASS_TRANS_COUNT,CASS_CURR,CASS_NUMBER,AVAIL_COEFF,CASS_REMAINING) "
			+ "VALUES(#{atmId}, #{statDate}, #{encId}, #{cassValue}, #{cassCount}, #{transCount},#{curr}, #{cassNumber}, #{coeff}, #{cassRemaining})")
	void insertCassStatMd_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("cassValue") Integer denom, @Param("cassCount") Integer bills,
			@Param("transCount") Integer transCount, @Param("curr") Integer curr,
			@Param("cassNumber") Integer cassNumber, @Param("coeff") Integer coeff,
			@Param("cassRemaining") Integer cassRemaining);

	@Update("UPDATE T_CM_CASHOUT_CASS_STAT  SET CASS_COUNT = CASS_COUNT + #{cassCount}, "
			+ " CASS_TRANS_COUNT = CASS_TRANS_COUNT + #{transCount},  AVAIL_COEFF = #{coeff}, CASS_REMAINING = #{cassRemaining} WHERE "
			+ "ATM_ID = #{atmId} AND STAT_DATE = #{statDate} AND ENCASHMENT_ID = #{encId} AND "
			+ "CASS_NUMBER = #{cassNumber} AND CASS_VALUE =  #{cassValue} AND CASS_CURR = #{curr}")
	void insertCassStatMd_update(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("cassValue") Integer denom, @Param("cassCount") Integer bills,
			@Param("transCount") Integer transCount, @Param("curr") Integer curr,
			@Param("cassNumber") Integer cassNumber, @Param("coeff") Integer coeff,
			@Param("cassRemaining") Integer cassRemaining);
	
	@Results({
			@Result(column = "trans_date", property = "first", javaType = Timestamp.class),
			@Result(column = "curr", property = "second", javaType = Integer.class),
			@Result(column = "summ", property = "third", javaType = Integer.class),
			@Result(column = "CURR_TRANS_COUNT", property = "fourth", javaType = Integer.class)
	})
	@ResultType(MultiObject.class)
	@Select("SELECT sum(bills*denom) as summ,truncToHour(d) as trans_date, CURR, count(distinct utrnno) as curr_trans_count "
			+ " FROM( "
			+ " select utrnno,BILL_CASS1 as BILLS,DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS1 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS1 = "
			+ AggregationController.CASS_TYPE_CASH_OUT + " union all "
			+ " select utrnno,BILL_CASS2 as BILLS,DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS2 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS2 = "
			+ AggregationController.CASS_TYPE_CASH_OUT + " union all "
			+ " select utrnno,BILL_CASS3 as BILLS,DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS3 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS3 = "
			+ AggregationController.CASS_TYPE_CASH_OUT + " union all "
			+ " select utrnno,BILL_CASS4 as BILLS,DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS4 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS4 = "
			+ AggregationController.CASS_TYPE_CASH_OUT + " union all "
			+ " select utrnno,BILL_CASS5 as BILLS,DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS5 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS5 = "
			+ AggregationController.CASS_TYPE_CASH_OUT + " union all "
			+ " select utrnno,BILL_CASS6 as BILLS,DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS6 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS6 = "
			+ AggregationController.CASS_TYPE_CASH_OUT + " union all "
			+ " select utrnno,BILL_CASS7 as BILLS,DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS7 > 0 "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS7 = "
			+ AggregationController.CASS_TYPE_CASH_OUT + " union all "
			+ " select utrnno,BILL_CASS8 as BILLS,DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR "
			+ " from t_cm_intgr_trans  where trans_type_ind in (" + AggregationController.EXCHANGE_TRANSACTION_TYPE
			+ "," + AggregationController.DEBIT_TRANSACTION_TYPE + ")  and atm_id = #{pPid}  and BILL_CASS8 > 0 "
			+ " ) GROUP BY truncToHour(d), CURR  ORDER BY trans_date")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Timestamp, Integer, Integer, Integer, ?, ?, ?, ?, ?, ?>> insertCurrStat_check(
			@Param("pPid") String pPid, @Param("pEncDate") Timestamp pEncDate,
			@Param("pNextEncDate") Timestamp pNextEncDate);

	@Insert("INSERT INTO "
			+ "T_CM_CASHOUT_CURR_STAT(ATM_ID,STAT_DATE,ENCASHMENT_ID,CURR_CODE,CURR_SUMM,CURR_TRANS_COUNT) "
			+ "VALUES(#{atmId}, #{statDate}, #{encId}, #{currCode}, #{currSumm}, #{transCount})")
	void insertCurrStat_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("currCode") Integer currCode, @Param("currSumm") Integer currSumm,
			@Param("transCount") Integer transCount);

	@Update("UPDATE T_CM_CASHOUT_CURR_STAT  SET CURR_SUMM = CURR_SUMM + #{currSumm}, "
			+ " CURR_TRANS_COUNT = CURR_TRANS_COUNT + #{transCount} WHERE ATM_ID = #{atmId} AND "
			+ "STAT_DATE = #{statDate} AND ENCASHMENT_ID = #{encId} AND CURR_CODE = #{currCode}")
	void insertCurrStat_update(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("currCode") Integer currCode, @Param("currSumm") Integer currSumm,
			@Param("transCount") Integer transCount);
	
	@Results({
			@Result(column = "trans_date", property = "first", javaType = Timestamp.class),
			@Result(column = "curr", property = "second", javaType = Integer.class),
			@Result(column = "summ", property = "third", javaType = Integer.class),
			@Result(column = "CURR_TRANS_COUNT", property = "fourth", javaType = Integer.class)
	})
	@ResultType(MultiObject.class)
	@Select("SELECT sum(itmd.note_dispensed*face) as summ, truncToHour(itm.datetime) as trans_date, itmd.currency as CURR, "
			+ " count(distinct itm.oper_id) as curr_trans_count  from t_cm_intgr_trans_md itm "
			+ " join t_cm_intgr_trans_md_disp itmd on (itm.oper_id = itmd.oper_id)  where oper_type in ("
			+ AggregationController.EXCHANGE_TRANSACTION_TYPE + ", " + AggregationController.DEBIT_TRANSACTION_TYPE
			+ ") and itm.terminal_id = #{pPid}  and itmd.note_dispensed > 0 "
			+ " and itm.datetime between #{pEncDate} and #{pNextEncDate}  GROUP BY truncToHour(itm.datetime), itmd.currency "
			+ " ORDER BY trans_date")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Timestamp, Integer, Integer, Integer, ?, ?, ?, ?, ?, ?>> insertCurrStatMd_check(
			@Param("pPid") String pPid, @Param("pEncDate") Timestamp pEncDate,
			@Param("pNextEncDate") Timestamp pNextEncDate);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(MAX(ENCASHMENT_ID),0) as result FROM T_CM_ENC_CASHOUT_STAT WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID < #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	Integer insertEncashmentsPartAndOut_getPrevEnc(@Param("atmId") String atmId, @Param("encId") Integer encId);
	
	@Result(column = "result", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT COALESCE(MAX(STAT_DATE),#{pEncDate}) as result FROM T_CM_CASHOUT_CASS_STAT WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID < #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	Timestamp insertEncashmentsPartAndOut_getPrevEncLastStat(@Param("pEncDate") Timestamp pEncDate,
			@Param("atmId") String atmId, @Param("encId") Integer encId);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT  CASE  WHEN CASH_ADD_ENCASHMENT > 0 THEN " + AggregationController.CO_ENC_DET_NOT_UNLOADED_CA
			+ "  ELSE " + AggregationController.CO_ENC_DET_UNLOADED + "  END as result FROM T_CM_ENC_CASHOUT_STAT "
			+ " WHERE ENCASHMENT_ID = #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertEncashmentsPartAndOut_getUnloadType(@Param("encId") Integer encId);
	
	@Result(column = "CASS_NUMBER", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select CASS_NUMBER from t_cm_enc_cashout_stat_Details  where encashment_id = #{prevEncId} "
			+ " and not exists (select CASS_NUMBER from t_cm_enc_cashout_stat_Details "
			+ " where encashment_id = #{encId})")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertEncashmentsPartAndOut_getDetailsLoop(@Param("prevEncId") Integer prevEncId,
			@Param("encId") Integer encId);
	
	@Result(column = "CASS_NUMBER", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select CASS_NUMBER from t_cm_enc_cashout_stat_Details where encashment_id = #{prevEncId} ")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertEncashmentsPartAndOut_getPrevEncDetailsLoop(@Param("prevEncId") Integer prevEncId);
	
	@Results({
			@Result(column = "CASS_REMAINING", property = "first", javaType = Integer.class),
			@Result(column = "CASS_VALUE", property = "second", javaType = Integer.class),
			@Result(column = "CASS_CURR", property = "third", javaType = Integer.class)
	})
	@ResultType(TripleObject.class)
	@Select("SELECT CASS_REMAINING,CASS_VALUE,CASS_CURR  FROM T_CM_CASHOUT_CASS_STAT  WHERE ATM_ID = #{pPid} "
			+ " AND ENCASHMENT_ID = #{prevEncId}  AND STAT_DATE = #{prevEncLastStat}  AND CASS_NUMBER = #{cassNumber} ")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Integer, Integer, Integer>> insertEncashmentsPartAndOut_getCassStat(@Param("pPid") String pPid,
			@Param("prevEncId") Integer prevEncId, @Param("prevEncLastStat") Timestamp prevEncLastStat,
			@Param("cassNumber") Integer cassNumber);
	
	@Insert(" INSERT INTO T_CM_ENC_CASHOUT_STAT_details "
			+ " (ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER)  VALUES "
			+ " (#{encId}, #{cassValue}, #{cassCurr}, #{cassCount}, #{actionType}, #{cassNumber}) ")
	void insertEncashmentsPartAndOut_insert(@Param("encId") Integer encId, @Param("cassValue") Integer cassValue,
			@Param("cassCurr") Integer cassCurr, @Param("cassCount") Integer cassCount,
			@Param("actionType") Integer actionType, @Param("cassNumber") Integer cassNumber);
	
	@Result(column = "ENCASHMENT_ID", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select ENCASHMENT_ID from T_CM_ENC_CASHOUT_STAT_details "
			+ " where ENCASHMENT_ID=#{pEncId} and CASS_NUMBER=#{cassNumber} and ACTION_TYPE=#{actionType}")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertEncashmentsPartAndOut_checkInsert(@Param("pEncId") Integer pEncId,
			@Param("cassNumber") Integer cassNumber, @Param("actionType") Integer actionType);
	
	@Delete("delete from t_cm_enc_cashout_stat_details ecs  where ecs.encashment_id = #{pEncId} and ecs.ACTION_TYPE = "
			+ AggregationController.CO_ENC_DET_NOT_UNLOADED + " and exists  "
			+ " (select null from t_cm_enc_cashout_stat_details ecsd  "
			+ " where ecsd.ENCASHMENT_ID = ecs.ENCASHMENT_ID and ecsd.CASS_NUMBER = ecs.CASS_NUMBER  "
			+ " and ecsd.ACTION_TYPE = " + AggregationController.CO_ENC_DET_LOADED + ")")
	void insertEncashmentsPartAndOut_delete(@Param("pEncId") Integer pEncId);
	
	@Result(column = "CASS_NUMBER", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT DISTINCT CASS_NUMBER  FROM T_CM_CASHOUT_CASS_STAT  WHERE ENCASHMENT_ID = #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertRemainingsForCass_getCassStatLoop(@Param("encId") Integer encId);
	
	@Results({
			@Result(column = "STAT_DATE", property = "key", javaType = Timestamp.class),
			@Result(column = "CASS_COUNT", property = "value", javaType = Integer.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT STAT_DATE, CASS_COUNT  FROM T_CM_CASHOUT_CASS_STAT  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID = #{encId}  AND CASS_NUMBER = #{cassNumber}  ORDER BY STAT_DATE")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Timestamp, Integer>> insertRemainingsForCass_getCassStatLoop2(@Param("atmId") String atmId,
			@Param("encId") Integer encId, @Param("cassNumber") Integer cassNumber);
	
	@Result(column = "CASS_COUNT", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT CASS_COUNT  FROM T_CM_ENC_CASHOUT_STAT_details  WHERE ENCASHMENT_ID = #{encId} "
			+ " AND CASS_NUMBER = #{cassNumber}  AND ACTION_TYPE in (" + AggregationController.CO_ENC_DET_LOADED + ") ")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertRemainingsForCass_getRemainingCashAdd(@Param("encId") Integer encId,
			@Param("cassNumber") Integer cassNumber);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT sum(CASS_COUNT) as result FROM T_CM_ENC_CASHOUT_STAT_details  WHERE ENCASHMENT_ID = #{encId} "
			+ " AND CASS_NUMBER = #{cassNumber}  AND ACTION_TYPE in (" + AggregationController.CO_ENC_DET_LOADED + ", "
			+ AggregationController.CO_ENC_DET_NOT_UNLOADED_CA + ")")
	@Options(useCache = true)
	Integer insertRemainingsForCass_getRemainingCashAddNotUnloaded(@Param("encId") Integer encId,
			@Param("cassNumber") Integer cassNumber);

	@Result(column = "result", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT MAX(STAT_DATE) as result  FROM T_CM_CASHOUT_CASS_STAT  WHERE  ATM_ID = #{atmId} "
			+ " AND CASS_NUMBER = #{cassNumber}  AND ENCASHMENT_ID < #{encId}")
	@Options(useCache = true)
	Timestamp insertRemainingsForCass_getMaxStatDate(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("cassNumber") Integer cassNumber);

	@Result(column = "CASS_REMAINING", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT CASS_REMAINING  FROM T_CM_CASHOUT_CASS_STAT  WHERE STAT_DATE = #{statDate} "
			+ " AND ATM_ID = #{atmId}  AND CASS_NUMBER = #{cassNumber} ")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertRemainingsForCass_getRemainingMaxStatDate(@Param("statDate") Timestamp statDate,
			@Param("atmId") String atmId, @Param("cassNumber") Integer cassNumber);

	@Update("UPDATE T_CM_CASHOUT_CASS_STAT SET CASS_REMAINING = #{cassRemaining} WHERE ATM_ID = #{atmId} "
			+ "AND ENCASHMENT_ID = #{encId} AND CASS_NUMBER = #{cassNumber} AND STAT_DATE = #{statDate}")
	void insertRemainingsForCass_update(@Param("cassRemaining") Integer cassRemaining, @Param("atmId") String atmId,
			@Param("encId") Integer encId, @Param("cassNumber") Integer cassNumber,
			@Param("statDate") Timestamp statDate);
	
	@Results({
			@Result(column = "stat_date", property = "first", javaType = Timestamp.class),
			@Result(column = "CURR_REMAINING", property = "second", javaType = Integer.class),
			@Result(column = "CASS_CURR", property = "third", javaType = Integer.class)
	})
	@ResultType(TripleObject.class)
	@Select("select stat_date,sum(cass_remaining*cass_value) as CURR_REMAINING,CASS_CURR "
			+ " from T_CM_CASHOUT_CASS_STAT ds  where  ATM_ID = #{pPid}  AND ENCASHMENT_ID = #{encId} "
			+ " group by stat_date,cass_curr")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Timestamp, Integer, Integer>> insertRemainingsForCurr_getCassStat(@Param("pPid") String pPid,
			@Param("encId") Integer encId);

	@Update("UPDATE T_CM_CASHOUT_CURR_STAT SET CURR_REMAINING = #{currRemaining} WHERE ATM_ID = #{atmId} "
			+ "AND ENCASHMENT_ID = #{encId} AND CURR_CODE = #{currCode} AND STAT_DATE = #{statDate}")
	void insertRemainingsForCurr_update(@Param("currRemaining") Integer currRemaining, @Param("atmId") String atmId,
			@Param("encId") Integer encId, @Param("currCode") Integer currCode, @Param("statDate") Timestamp statDate);
	
	@Results({
			@Result(column = "CASS_NUMBER", property = "first", javaType = Integer.class),
			@Result(column = "CASS_CURR", property = "second", javaType = Integer.class),
			@Result(column = "CASS_VALUE", property = "third", javaType = Integer.class)
	})
	@ResultType(TripleObject.class)
	@Select("SELECT distinct CASS_NUMBER,CASS_CURR,CASS_VALUE  FROM T_CM_ENC_CASHOUT_STAT_details "
			+ " WHERE ENCASHMENT_ID = #{encId}  AND ACTION_TYPE = " + AggregationController.CO_ENC_DET_LOADED)
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Integer, Integer, Integer>> insertZeroTakeOffsForCass_getCoStatDetails(
			@Param("encId") Integer encId);
	
	@Result(column = "STAT_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT STAT_DATE  FROM T_CM_CASHOUT_CASS_STAT  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID = #{encId}  AND CASS_NUMBER = #{cassNumber} AND STAT_DATE = #{statDate}")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> insertZeroTakeOffsForCass_getFillStatDate(@Param("atmId") String atmId,
			@Param("encId") Integer encId, @Param("cassNumber") Integer cassNumber,
			@Param("statDate") Timestamp statDate);
	
	@Result(column = "CASS_COUNT", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT CASS_COUNT FROM T_CM_ENC_CASHOUT_STAT_details  WHERE ENCASHMENT_ID = #{encId} "
			+ " AND CASS_NUMBER = #{cassNumber} AND ACTION_TYPE in (" + AggregationController.CO_ENC_DET_LOADED + ")")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertZeroTakeOffsForCass_getStatDetailsLoad(@Param("encId") Integer encId,
			@Param("cassNumber") Integer cassNumber);
	
	@Result(column = "CASS_COUNT", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT sum(CASS_COUNT) as CASS_COUNT  FROM T_CM_ENC_CASHOUT_STAT_details "
			+ " WHERE ENCASHMENT_ID = #{encId}  AND CASS_NUMBER = #{cassNumber}  AND ACTION_TYPE in ("
			+ AggregationController.CO_ENC_DET_LOADED + ", " + AggregationController.CO_ENC_DET_NOT_UNLOADED_CA + ")")
	@Options(useCache = true, fetchSize = 1000)
	Integer insertZeroTakeOffsForCass_getStatDetailsLoadUnload(@Param("encId") Integer encId,
			@Param("cassNumber") Integer cassNumber);
	
	@Result(column = "result", javaType = Double.class)
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(ds.AVAIL_COEFF,1) as result FROM t_cm_intgr_downtime_cashout ds "
			+ "WHERE ds.PID = #{pid} AND ds.STAT_DATE = #{statDate}")
	@Options(useCache = true)
	Double insertZeroTakeOffsForCass_getDowntimeCo(@Param("pid") String pid, @Param("statDate") Timestamp statDate);
	
	@Results({
			@Result(column = "CASS_REMAINING", property = "key", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "value", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT CASS_REMAINING,cs.STAT_DATE  FROM T_CM_CASHOUT_CASS_STAT cs  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID = #{encId}  AND CASS_NUMBER = #{cassNumber} AND cs.STAT_DATE = #{statDate} ")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> insertZeroTakeOffsForCass_getCassStat(@Param("atmId") String atmId,
			@Param("encId") Integer encId, @Param("cassNumber") Integer cassNumber,
			@Param("statDate") Timestamp statDate);
	
	@Insert("INSERT INTO T_CM_CASHOUT_CASS_STAT "
			+ " (ATM_ID,STAT_DATE,ENCASHMENT_ID,CASS_VALUE,CASS_COUNT,CASS_TRANS_COUNT,CASS_CURR,CASS_REMAINING,CASS_NUMBER,AVAIL_COEFF) "
			+ " VALUES "
			+ " (#{atmId} ,#{statDate}, #{encId}, #{cassValue}, #{cassCount}, #{cassTrans}, #{cassCurr}, #{cassRemaining}, #{cassNumber}, #{coeff})")
	void insertZeroTakeOffsForCass_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("cassValue") Integer cassValue, @Param("cassCount") Integer cassCount,
			@Param("cassTrans") Integer cassTrans, @Param("cassCurr") Integer cassCurr,
			@Param("cassRemaining") Integer cassRemaining, @Param("cassNumber") Integer cassNumber,
			@Param("coeff") Double coeff);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COUNT(*) as result FROM T_CM_CASHOUT_CASS_STAT  where  ATM_ID = #{atmId} AND STAT_DATE = #{statDate "
			+ " AND ENCASHMENT_ID = #{encId}  AND CASS_VALUE = #{cassValue}  AND CASS_CURR = #{cassCurr} "
			+ " AND CASS_NUMBER = #{cassNumber}")
	@Options(useCache = true, fetchSize = 1000)
	Integer insertZeroTakeOffsForCass_checkInsert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("cassValue") Integer cassValue, @Param("cassCurr") Integer cassCurr,
			@Param("cassNumber") Integer cassNumber);
	
	@Update("UPDATE T_CM_CASHOUT_CASS_STAT  SET CASS_COUNT = #{cassCount},  CASS_TRANS_COUNT = #{cassTrans}, "
			+ " CASS_REMAINING = #{cassRemaining},  AVAIL_COEFF = #{coeff}  where  ATM_ID = #{atmId} "
			+ " AND STAT_DATE = #{statDate}  AND ENCASHMENT_ID = #{encId}  AND CASS_VALUE = #{cassValue} "
			+ " AND CASS_CURR = #{cassCurr}  AND CASS_NUMBER = #{cassNumber}")
	void insertZeroTakeOffsForCass_update(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("cassValue") Integer cassValue, @Param("cassCount") Integer cassCount,
			@Param("cassTrans") Integer cassTrans, @Param("cassCurr") Integer cassCurr,
			@Param("cassRemaining") Integer cassRemaining, @Param("cassNumber") Integer cassNumber,
			@Param("coeff") Double coeff);
	
	@Result(column = "CURR_CODE", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT distinct CASS_CURR as CURR_CODE  FROM T_CM_ENC_CASHOUT_STAT_details "
			+ " WHERE ENCASHMENT_ID = #{encId}  AND ACTION_TYPE = " + AggregationController.CO_ENC_DET_LOADED)
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertZeroTakeOffsForCurr_getCoStatDetails(@Param("encId") Integer encId);
	
	@Result(column = "STAT_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT STAT_DATE  FROM T_CM_CASHOUT_CURR_STAT  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID = #{encId}  AND CURR_CODE = #{currCode}  AND STAT_DATE = #{statDate}")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> insertZeroTakeOffsForCurr_getStatDate(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("currCode") Integer currCode, @Param("statDate") Timestamp statDate);

	@Insert("INSERT INTO T_CM_CASHOUT_CURR_STAT "
			+ " (ATM_ID,STAT_DATE,ENCASHMENT_ID,CURR_CODE,CURR_SUMM,CURR_TRANS_COUNT) VALUES "
			+ " (#{atmId} ,#{statDate}, #{encId}, #{currCode}, #{currSumm}, #{cassTrans})")
	void insertZeroTakeOffsForCurr_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("currCode") Integer currCode, @Param("currSumm") Integer currSumm,
			@Param("cassTrans") Integer cassTrans);
	
	@Results({ 
			@Result(column = "BILLS_COUNT", property = "key", javaType = Integer.class),
			@Result(column = "stat_Date", property = "value", javaType = Timestamp.class) 
	})
	@ResultType(ObjectPair.class)
	@Select("select sum(COALESCE(bill_reject,0))+sum(COALESCE(bill_retract,0)) as BILLS_COUNT,truncToHour(datetime) as stat_Date "
			+ " from t_cm_intgr_trans  WHERE atm_id = #{atmId} "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  group by truncToHour(datetime) "
			+ " order by stat_Date")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> insertRejects_getIntgrParams(@Param("atmId") String atmId,
			@Param("pEncDate") Timestamp pEncDate, @Param("pNextEncDate") Timestamp pNextEncDate);

	@Insert("INSERT INTO T_CM_REJECT_STAT  (ATM_ID,STAT_DATE,ENCASHMENT_ID,BILLS_COUNT)  VALUES "
			+ " (#{atmId} ,#{statDate}, #{encId}, #{count})")
	void insertRejects_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("count") Integer count);
	
	@Result(column = "ATM_ID", javaType = String.class) 
	@ResultType(String.class)
	@Select("select ATM_ID from T_CM_REJECT_STAT "
			+ " where ATM_ID=#{atmId} and STAT_DATE=#{statDate} and ENCASHMENT_ID=#{encId}")
	@Options(useCache = true, fetchSize = 1000)
	List<String> insertRejects_checkInsert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId);

	@Update("UPDATE T_CM_REJECT_STAT  SET BILLS_COUNT = BILLS_COUNT + #{count}  WHERE "
			+ " ATM_ID = #{atmId}  AND  STAT_DATE = #{statDate}  AND  ENCASHMENT_ID = #{encId}")
	void insertRejects_update(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("count") Integer count);
	
	@Results({ 
			@Result(column = "BILLS_COUNT", property = "key", javaType = Integer.class),
			@Result(column = "stat_Date", property = "value", javaType = Timestamp.class) 
	})
	@ResultType(ObjectPair.class)
	@Select("select sum(note_rejected)+sum(note_retracted) as BILLS_COUNT,truncToHour(datetime) as stat_date "
			+ " from t_cm_intgr_trans_md  WHERE terminal_id = #{atmId} "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  group by trunc(datetime,'HH24') "
			+ " order by stat_Date")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> insertRejectsMd_getIntgrParams(@Param("atmId") String atmId,
			@Param("pEncDate") Timestamp pEncDate, @Param("pNextEncDate") Timestamp pNextEncDate);
	
	@Result(column = "STAT_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT DISTINCT STAT_DATE  FROM T_CM_REJECT_STAT  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID = #{encId}  AND STAT_DATE = #{statDate}")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> insertZeroDaysForRejects_getStatDate(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("statDate") Timestamp statDate);
	
	@Results({ 
			@Result(column = "BILLS_COUNT", property = "key", javaType = Integer.class),
			@Result(column = "stat_Date", property = "value", javaType = Timestamp.class) 
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT STAT_DATE, BILLS_COUNT  FROM T_CM_REJECT_STAT  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID = #{encId} ORDER BY STAT_DATE")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> insertRemainingsForRejects_select(@Param("atmId") String atmId,
			@Param("encId") Integer encId);
	
	@Update("UPDATE T_CM_REJECT_STAT  SET BILLS_REMAINING = #{remaining}  WHERE ATM_ID = #{atmId} "
			+ " AND ENCASHMENT_ID = #{encId} AND STAT_DATE = #{statDate}")
	void insertRemainingsForRejects_update(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("statDate") Timestamp statDate, @Param("remaining") Integer remaining);
	
	@Results({
			@Result(column = "bills", property = "first", javaType = Integer.class),
			@Result(column = "denom", property = "second", javaType = Integer.class),
			@Result(column = "enc_date", property = "third", javaType = Timestamp.class),
			@Result(column = "cass_num", property = "fourth", javaType = Integer.class)
	})
	@ResultType(MultiObject.class)
	@Select("select bills as bills, curr,denom, d as enc_date,cass_num FROM( "
			+ "select 0 as BILLS, datetime as d, 0 as denom, -999 as CURR, -1 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind = " + AggregationController.CI_ENC_TRANSACTION_TYPE
			+ " and atm_id = #{pPid} union all "
			+ "select BILL_CASS1 as BILLS,DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR,1 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_909_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS1 > 0 and CURRENCY_CASS1 > 0 union all "
			+ "select BILL_CASS2 as BILLS,DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR,2 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_909_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS2 > 0 and CURRENCY_CASS2 > 0 union all "
			+ "select BILL_CASS3 as BILLS,DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR,3 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_910_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS3 > 0 and CURRENCY_CASS3 > 0 union all "
			+ "select BILL_CASS4 as BILLS,DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR,4 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_910_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS4 > 0 and CURRENCY_CASS4 > 0 union all "
			+ "select BILL_CASS5 as BILLS,DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR,5 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_909_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS5 > 0 and CURRENCY_CASS5 > 0 union all "
			+ "select BILL_CASS6 as BILLS,DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR,6 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_909_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS6 > 0 and CURRENCY_CASS6 > 0 union all "
			+ "select BILL_CASS7 as BILLS,DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR,7 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_910_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS7 > 0 and CURRENCY_CASS7 > 0 union all "
			+ "select BILL_CASS8 as BILLS,DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR,8 as CASS_NUM,trans_type_ind "
			+ "from t_cm_intgr_trans where trans_type_ind in(" + AggregationController.CR_910_ENC_TRANSACTION_TYPE
			+ ") and atm_id = #{pPid} and DENOM_CASS8 > 0 and CURRENCY_CASS8 > 0 order by d "
			+ ") ORDER BY ENC_DATE,CASS_NUM,trans_type_ind")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Integer, Integer, Timestamp, Integer, ?, ?, ?, ?, ?, ?>> insertEcnashmentsCashIn_select(
			@Param("pPid") String pPid);
	
	@Results({ 
			@Result(column = "CASH_IN_ENCASHMENT_ID", property = "key", javaType = Integer.class),
			@Result(column = "CASH_IN_ENC_DATE", property = "value", javaType = Timestamp.class) 
	})
	@ResultType(ObjectPair.class)
	@Select("select st.CASH_IN_ENCASHMENT_ID,st.CASH_IN_ENC_DATE from t_cm_enc_cashin_stat st where "
			+ "st.atm_id = #{atmId} and abs(dateDiffMin(st.CASH_IN_ENC_DATE, #{statDate})) < 15")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Timestamp>> insertEcnashmentsCashIn_getEncIdAndDate(@Param("atmId") String atmId,
			@Param("statDate") Timestamp statDate);
	
	@Insert("INSERT INTO T_CM_ENC_CASHIN_STAT (ATM_ID,CASH_IN_ENCASHMENT_ID,CASH_IN_ENC_DATE) VALUES "
			+ " (#{atmId}, #{encId}, #{encDate})")
	void insertEcnashmentsCashIn_insertWithoutSeq(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("encDate") Timestamp encDate);

	@Insert("INSERT INTO t_cm_enc_cashin_stat_details "
			+ "(CASH_IN_ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) VALUES "
			+ "(#{encId}, #{cassValue} , #{cassCurr}, #{cassCount}, " + AggregationController.CO_ENC_DET_LOADED
			+ ", #{cassNumber})")
	void insertEcnashmentsCashIn_insertDetails(@Param("encId") Integer encId, @Param("cassValue") Integer cassValue,
			@Param("cassCurr") Integer cassCurr, @Param("cassCount") Integer cassCount,
			@Param("cassNumber") Integer cassNumber);
	
	@Update("UPDATE t_cm_enc_cashin_stat_details SET CASS_CURR = #{cassCurr}, CASS_VALUE = #{cassValue} , "
			+ "CASS_COUNT = #{cassCount} WHERE CASS_NUMBER = #{cassNumber} AND "
			+ "CASH_IN_ENCASHMENT_ID = #{encId} AND ACTION_TYPE = " + AggregationController.CO_ENC_DET_LOADED)
	void insertEcnashmentsCashIn_updateDetails(@Param("encId") Integer encId, @Param("cassValue") Integer cassValue,
			@Param("cassCurr") Integer cassCurr, @Param("cassCount") Integer cassCount,
			@Param("cassNumber") Integer cassNumber);

	@Result(column = "ENC_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("select datetime as ENC_DATE  from t_cm_intgr_trans_md  where  oper_type = "
			+ AggregationController.CI_ENC_TRANSACTION_TYPE + " and terminal_id = #{atmId} order by datetime")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> insertEcnashmentsCashInMd_getEncDate(@Param("atmId") String atmId);
	
	@Insert("INSERT INTO T_CM_ENC_CASHIN_STAT  (ATM_ID,CASH_IN_ENCASHMENT_ID,CASH_IN_ENC_DATE)  VALUES "
			+ " (#{atmId}, #{encId}, #{encDate})")
	void insertEcnashmentsCashInMd_insert(@Param("atmId") String atmId, @Param("encId") Integer encId,
			@Param("Integer") Integer encDate);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(MAX(st.CASH_IN_ENCASHMENT_ID),0) as result "
			+ "FROM T_CM_ENC_CASHIN_STAT st, t_cm_enc_cashin_stat_details dt "
			+ "WHERE st.CASH_IN_ENCASHMENT_ID = dt.CASH_IN_ENCASHMENT_ID and ATM_ID = #{atmId} "
			+ "AND st.CASH_IN_ENCASHMENT_ID < #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	Integer insertCiEncashmentsPartAndOut_getPrevEncId(@Param("atmId") String atmId, @Param("encId") Integer encId);

	@Result(column = "result", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT COALESCE(MAX(STAT_DATE),pEncDate) as result FROM T_CM_CASHIN_R_CASS_STAT "
			+ "WHERE ATM_ID = #{atmId} AND CASH_IN_ENCASHMENT_ID < #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	Timestamp insertCiEncashmentsPartAndOut_getPrevEncLastStat(@Param("atmId") String atmId,
			@Param("encId") Integer encId);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT " + AggregationController.CO_ENC_DET_UNLOADED + " as result FROM T_CM_ENC_CASHIN_STAT "
			+ "WHERE CASH_IN_ENCASHMENT_ID = #{encId}")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertCiEncashmentsPartAndOut_getUnloadType(@Param("encId") Integer encId);
	
	@Result(column = "CASS_NUMBER", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select CASS_NUMBER from t_cm_enc_cashin_stat_Details where cash_in_encashment_id = #{prevEncId} "
			+ "and CASS_NUMBER not in (select CASS_NUMBER from t_cm_enc_cashin_stat_Details "
			+ "where cash_in_encashment_id = #{encId})")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertCiEncashmentsPartAndOut_getCiEncStatDetails(@Param("prevEncId") Integer prevEncId,
			@Param("encId") Integer encId);
	
	@Results({
			@Result(column = "CASS_REMAINING", property = "first", javaType = Integer.class),
			@Result(column = "CASS_VALUE", property = "second", javaType = Integer.class),
			@Result(column = "CASS_CURR", property = "third", javaType = Integer.class)
	})
	@ResultType(TripleObject.class)
	@Select("SELECT CASS_REMAINING,CASS_VALUE,CASS_CURR FROM T_CM_CASHIN_R_CASS_STAT WHERE ATM_ID = #{atmId} "
			+ "AND CASH_IN_ENCASHMENT_ID = #{encId} AND STAT_DATE = #{statDate} AND CASS_NUMBER = #{cassNumber}")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Integer, Integer, Integer>> insertCiEncashmentsPartAndOut_getCiEncStatDetails(
			@Param("atmId") String pPid, @Param("encId") Integer encId, @Param("statDate") Timestamp statDate,
			@Param("cassNumber") Integer cassNumber);
	
	@Insert("INSERT INTO T_CM_ENC_CASHIN_STAT_DETAILS "
			+ "(CASH_IN_ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) VALUES "
			+ "(#{encId}, #{cassValue} , #{cassCurr}, #{cassCount}, #{actionType}, #{cassNumber})")
	void insertCiEncashmentsPartAndOut_insert(@Param("encId") Integer encId, @Param("cassValue") Integer cassValue,
			@Param("cassCurr") Integer cassCurr, @Param("cassCount") Integer cassCount,
			@Param("actionType") Integer actionType, @Param("cassNumber") Integer cassNumber);
	
	@Result(column = "CASS_NUMBER", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select CASS_NUMBER from t_cm_enc_cashin_stat_details where cash_in_encashment_id = #{encId})")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> insertCiEncashmentsPartAndOut_getCiEncStatDetailsLoop2(@Param("encId") Integer encId);
	
	@Delete("delete from t_cm_enc_cashin_stat_details ecs where ecs.cash_in_encashment_id = #{encId} and "
			+ "ecs.ACTION_TYPE = " + AggregationController.CO_ENC_DET_NOT_UNLOADED + " and exists "
			+ "(select null from t_cm_enc_cashin_stat_details ecsd "
			+ "where ecsd.CASH_IN_ENCASHMENT_ID = ecs.CASH_IN_ENCASHMENT_ID "
			+ "and ecsd.CASS_NUMBER = ecs.CASS_NUMBER and ecsd.ACTION_TYPE = " + AggregationController.CO_ENC_DET_LOADED
			+ ")")
	void insertCiEncashmentsPartAndOut_delete(@Param("encId") Integer encId);
	
	@Results({
		@Result(column = "BILLS_COUNT", property = "first", javaType = Integer.class),
		@Result(column = "STAT_DATE", property = "second", javaType = Timestamp.class),
		@Result(column = "AVAIL_COEFF", property = "third", javaType = Double.class)
	})
	@ResultType(TripleObject.class)
	@Select(" SELECT cs.BILLS_COUNT,cs.STAT_DATE,COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF  FROM "
			+ " (select atmId as PID,sum(BILLS_COUNT) as BILLS_COUNT,stat_date from "
			+ " (select atmId as PID,sum(bill_num) as BILLS_COUNT,truncToHour(datetime) as stat_Date "
			+ " from t_cm_intgr_trans_cash_in itci  WHERE trans_type_ind in ("
			+ AggregationController.EXCHANGE_TRANSACTION_TYPE + " ," + AggregationController.CREDIT_TRANSACTION_TYPE
			+ ")  and atm_id = atmId  and datetime between #{pEncDate} and #{pNextEncDate} "
			+ " group by truncToHour(datetime)  union all  select atmId as PID, "
			+ " -sum(COALESCE(BILL_CASS1,0)+COALESCE(BILL_CASS2,0)+COALESCE(BILL_CASS3,0)+COALESCE(BILL_CASS4,0)) as BILLS_COUNT, "
			+ " truncToHour(datetime) as stat_Date  from t_cm_intgr_trans  where trans_type_ind in ("
			+ AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = atmId "
			+ " and datetime between #{pEncDate} and #{pNextEncDate}  and (TYPE_CASS1 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " or TYPE_CASS2 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " or TYPE_CASS3 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " or TYPE_CASS4 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " or TYPE_CASS5 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " or TYPE_CASS6 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " or TYPE_CASS7 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " or TYPE_CASS8 = "
			+ AggregationController.CASS_TYPE_RECYCLING + ")  group by truncToHour(datetime)  ) "
			+ " group by stat_date order by stat_date) cs "
			+ " left outer join t_cm_intgr_downtime_cashin ds on (cs.PID = ds.PID and cs.stat_Date = ds.stat_date)")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Integer, Timestamp, Double>> insertCashInStat_getTrans(@Param("atmId") String atmId,
			@Param("pEncDate") Timestamp pEncDate, @Param("pNextEncDate") Timestamp pNextEncDate);
	
	@Insert("INSERT INTO T_CM_CASHIN_STAT  (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,BILLS_COUNT,AVAIL_COEFF) "
			+ " VALUES  (#{atmId} , #{statDate}, #{encId}, #{bills}, #{coeff})")
	void insertCashInStat_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("bills") Integer bills, @Param("coeff") Double coeff);
	
	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select COUNT(ATM_ID) as result from T_CM_CASHIN_STAT "
			+ " where ATM_ID=#{atmId} and STAT_DATE=#{statDate} and CASH_IN_ENCASHMENT_ID=#{encId}")
	Integer insertCashInStat_checkInsert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId);
	
	@Update("UPDATE T_CM_CASHIN_STAT  SET BILLS_COUNT = BILLS_COUNT + #{bills}, AVAIL_COEFF = #{coeff} "
			+ " WHERE  ATM_ID = #{atmId}  AND  STAT_DATE = #{statDate} AND CASH_IN_ENCASHMENT_ID = #{encId}")
	void insertCashInStat_update(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("bills") Integer bills, @Param("coeff") Double coeff);
	
	@Results({
			@Result(column = "DENOM_COUNT", property = "first", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "second", javaType = Timestamp.class),
			@Result(column = "DENOM_CURR", property = "third", javaType = Integer.class),
			@Result(column = "DENOM_VALUE", property = "fourth", javaType = Integer.class)
	})
	@ResultType(MultiObject.class)
	@Select(" SELECT cs.STAT_DATE,DENOM_COUNT, DENOM_CURR,DENOM_VALUE  FROM "
			+ " (select #{atmId} as PID,sum(DENOM_COUNT) as DENOM_COUNT,DENOM_CURR,DENOM_VALUE, stat_date  FROM  ( "
			+ " select #{atmId} as PID,sum(bill_num) as DENOM_COUNT,BILL_CURR as DENOM_CURR,BILL_DENOM as DENOM_VALUE,truncToHour(datetime) as stat_Date "
			+ " from t_cm_intgr_trans_cash_in itci  WHERE trans_type_ind in ("
			+ AggregationController.EXCHANGE_TRANSACTION_TYPE + ", " + AggregationController.CREDIT_TRANSACTION_TYPE
			+ ")  and atm_id = #{atmId}  and datetime between #{pEncDate} and #{pNextEncDate} "
			+ " group by truncToHour(datetime), BILL_CURR, BILL_DENOM  union all "
			+ " select #{atmId} as PID,-sum(bill_num) as DENOM_COUNT,CURR as DENOM_CURR,DENOM as DENOM_VALUE,truncToHour(d) as stat_Date "
			+ " from (  select  BILL_CASS1 as bill_num, "
			+ " DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS1 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS1 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " union all  select  BILL_CASS2 as bill_num, "
			+ " DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS2 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS2 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " union all  select  BILL_CASS3 as bill_num, "
			+ " DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS3 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS3 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " union all  select  BILL_CASS4 as bill_num, "
			+ " DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS4 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS4 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " union all  select  BILL_CASS5 as bill_num, "
			+ " DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS5 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS5 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " union all  select  BILL_CASS6 as bill_num, "
			+ " DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS6 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS6 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " union all  select  BILL_CASS7 as bill_num, "
			+ " DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS7 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS7 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " union all  select  BILL_CASS8 as bill_num, "
			+ " DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR  from t_cm_intgr_trans "
			+ " where trans_type_ind in (" + AggregationController.CREDIT_TRANSACTION_TYPE + ")  and atm_id = #{atmId} "
			+ " and BILL_CASS8 > 0  and datetime between #{pEncDate} and #{pNextEncDate}  and TYPE_CASS8 = "
			+ AggregationController.CASS_TYPE_RECYCLING + " )  group by truncToHour(d), CURR, DENOM  ) "
			+ " group by DENOM_CURR,DENOM_VALUE, stat_date  order by stat_date) cs "
			+ " left outer join t_cm_intgr_downtime_cashin ds on (cs.PID = ds.PID and cs.stat_Date = ds.stat_date)")
	@Options(useCache = true, fetchSize = 1000)
	List<MultiObject<Integer, Timestamp, Integer, Integer, ?, ?, ?, ?, ?, ?>> insertCashInDenomStat_getTrans(
			@Param("atmId") String atmId, @Param("pEncDate") Timestamp pEncDate,
			@Param("pNextEncDate") Timestamp pNextEncDate);

	@Insert("INSERT INTO T_CM_CASHIN_DENOM_STAT "
			+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,DENOM_CURR,DENOM_COUNT,DENOM_VALUE)  VALUES "
			+ " ( #{atmId}, #{statDate}, #{encId}, #{denomCurr}, #{denomCount}, #{denomValue})")
	void insertCashInDenomStat_insert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("denomCurr") Integer denomCurr,
			@Param("denomCount") Integer denomCount, @Param("denomValue") Integer denomValue);

	@Result(column = "result", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("select COUNT(ATM_ID) as result from T_CM_CASHIN_DENOM_STAT "
			+ " where ATM_ID=#{atmId} and STAT_DATE=#{statDate} and CASH_IN_ENCASHMENT_ID=#{encId} AND DENOM_CURR=#{denomCurr}")
	Integer insertCashInDenomStat_checkInsert(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("denomCurr") Integer denomCurr);

	@Update("UPDATE T_CM_CASHIN_DENOM_STAT  SET DENOM_COUNT = DENOM_COUNT + #{denomCount}  WHERE "
			+ " ATM_ID = #{atmId}  AND  STAT_DATE = #{statDate}  AND "
			+ " CASH_IN_ENCASHMENT_ID = #{encId}  AND  DENOM_VALUE = #{denomValue}  AND "
			+ " DENOM_CURR = #{denomCurr}")
	void insertCashInDenomStat_update(@Param("atmId") String atmId, @Param("statDate") Timestamp statDate,
			@Param("encId") Integer encId, @Param("denomCurr") Integer denomCurr,
			@Param("denomCount") Integer denomCount, @Param("denomValue") Integer denomValue);
}