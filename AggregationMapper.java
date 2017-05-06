package ru.bpc.cm.integration.orm;

import java.sql.Timestamp;
import java.util.List;

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
 * @version 1.0.0
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
	List<Integer> aggregateCashOut_getLastEncId(@Param("atmId") String atmId);
	
	@Result(column = "ENC_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT COALESCE(MIN(DISTINCT ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE  FROM T_CM_ENC_CASHOUT_STAT "
			+ " WHERE ATM_ID = #{atmId}  AND ENC_DATE > #{encDate}")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> aggregateCashOut_getNextIncass(@Param("atmId") String atmId, @Param("encDate") Timestamp encDate);
	
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
	List<Integer> aggregateCashIn_getLastEncId(@Param("atmId") String atmId);
	
	@Result(column = "ENC_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT COALESCE(MIN(DISTINCT CASH_IN_ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE "
			+ " FROM T_CM_ENC_CASHIN_STAT  WHERE ATM_ID = #{atmId}  AND CASH_IN_ENC_DATE > #{encDate}")
	@Options(useCache = true, fetchSize = 1000)
	List<Timestamp> aggregateCashIn_getNextIncass(@Param("atmId") String atmId, @Param("encDate") Timestamp encDate);
	
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
			+ " )GROUP BY truncToHour(d),denom, CURR,CASS_NUM  ORDER BY trans_date,CASS_NUM) cs "
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
}
