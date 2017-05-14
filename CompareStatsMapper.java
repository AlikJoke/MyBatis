package ru.bpc.cm.optimization.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.items.encashments.AtmPeriodEncashmentItem;
import ru.bpc.cm.items.encashments.EncashmentCassItem;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.utils.Pair;

/**
 * Интерфейс-маппер для класса CompareStatsController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 29.04.2017
 * @version 1.0.1
 *
 */
public interface CompareStatsMapper extends IMapper {

	@Result(column = "cnt", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(ecs.encashment_id) AS cnt FROM t_cm_enc_cashout_stat ecs WHERE "
			+ " ecs.atm_id = #{atmId} AND ecs.enc_date > #{startDate} AND ecs.enc_date < #{endDate} AND not exists (SELECT null "
			+ "FROM t_cm_enc_cashin_stat ecsi "
			+ "WHERE ecs.atm_id = ecsi.atm_id and abs(ecsi.cash_in_enc_date - ecs.enc_date) < 1/24)")
	Integer getEncCount_1(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate);

	@Result(column = "cnt", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(ecs.cash_in_encashment_id) AS cnt FROM t_cm_enc_cashin_stat ecs WHERE "
			+ " ecs.atm_id = #{atmId} AND ecs.cash_in_enc_date > #{startDate} AND ecs.cash_in_enc_date < #{endDate} "
			+ "AND not exists (SELECT null FROM t_cm_enc_cashout_stat ecsi "
			+ "WHERE ecs.atm_id = ecsi.atm_id and abs(ecsi.enc_date - ecs.cash_in_enc_date) < 1/24)")
	Integer getEncCount_2(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate);

	@Result(column = "cnt", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(ecs.encashment_id) AS cnt FROM t_cm_enc_cashout_stat ecs WHERE "
			+ " ecs.atm_id = #{atmId} AND ecs.enc_date > #{startDate} AND ecs.enc_date < #{endDate} "
			+ "AND exists (SELECT null FROM t_cm_enc_cashin_stat ecsi WHERE ecs.atm_id = ecsi.atm_id "
			+ "and abs(ecsi.cash_in_enc_date - ecs.enc_date) < 1/24)")
	Integer getEncCount_3(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate);

	@Result(column = "losts", javaType = Double.class)
	@ResultType(Double.class)
	@Select("WITH curr_takeoffs AS ( SELECT atm_id, stat_date, encashment_id, curr_code, "
			+ "curr_remaining AS curr_remaining FROM T_CM_CASHOUT_CURR_STAT ), curr_encashment_out AS ( "
			+ "SELECT atm_id, ecs.encashment_id, ecs.enc_date, ecsd.cass_curr AS curr_code, "
			+ "SUM(ecsd.cass_count*ecsd.cass_value) AS denom_count FROM t_cm_enc_cashout_stat ecs "
			+ "JOIN t_cm_enc_cashout_stat_details ecsd ON (ecsd.encashment_id = ecs.encashment_id) "
			+ "WHERE ecsd.action_type = 1 GROUP BY atm_id,enc_date,ecs.encashment_id,cass_curr ) "
			+ "SELECT  SUM((ct.curr_remaining+ co.denom_count)*(co.enc_date - ct.stat_date)*24) as losts "
			+ "FROM curr_takeoffs ct "
			+ "left OUTER JOIN v_cm_cashout_stat_enc2enc e2e on (ct.encashment_id = e2e.encashment_id) "
			+ "left OUTER JOIN curr_encashment_out co on (co.atm_id = ct.atm_id and co.encashment_id = e2e.next_encashment_id "
			+ "AND co.curr_code = ct.curr_code) WHERE ct.atm_id = #{atmId} AND ct.curr_code = #{currCode} AND ct.stat_date = #{startDate} ")
	Double getPeriodStartLostsForCurr(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("currCode") Integer currCode);

	@Result(column = "losts", javaType = Double.class)
	@ResultType(Double.class)
	@Select("WITH curr_takeoffs AS ( SELECT atm_id, stat_date, encashment_id, curr_code, "
			+ "curr_remaining AS curr_remaining FROM T_CM_CASHOUT_CURR_STAT ), curr_encashment_in AS ( "
			+ "SELECT atm_id, ecs.encashment_id, ecs.enc_date, ecsd.cass_curr AS curr_code, "
			+ "SUM(ecsd.cass_count*ecsd.cass_value) AS denom_count FROM t_cm_enc_cashout_stat ecs "
			+ "JOIN t_cm_enc_cashout_stat_details ecsd ON (ecsd.encashment_id = ecs.encashment_id) "
			+ "WHERE ecsd.action_type = 2 GROUP BY atm_id,enc_date,ecs.encashment_id,cass_curr ) "
			+ "SELECT  SUM((ct.curr_remaining+ ci.denom_count)*(ct.stat_date - ci.enc_date)*24) as losts "
			+ "FROM curr_takeoffs ct "
			+ "left OUTER JOIN curr_encashment_in ci on (ci.atm_id = ct.atm_id and ci.encashment_id = ct.encashment_id "
			+ "AND ci.curr_code = ct.curr_code) WHERE ct.atm_id = #{atmId} AND ct.curr_code = #{currCode} AND ct.stat_date = #{startDate} ")
	Double getPeriodEndLostsForCurr(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("currCode") Integer currCode);

	@Result(column = "losts", javaType = Double.class)
	@ResultType(Double.class)
	@Select("WITH curr_encashment_in AS ( SELECT atm_id, ecs.encashment_id, ecs.enc_date, "
			+ "ecsd.cass_curr AS curr_code, SUM(ecsd.cass_count*ecsd.cass_value) AS denom_count "
			+ "FROM t_cm_enc_cashout_stat ecs "
			+ "JOIN t_cm_enc_cashout_stat_details ecsd ON (ecsd.encashment_id = ecs.encashment_id) "
			+ "WHERE ecsd.action_type = 2 GROUP BY atm_id,enc_date,ecs.encashment_id,cass_curr ), "
			+ "curr_encashment_out AS ( SELECT atm_id, ecs.encashment_id, ecs.enc_date, "
			+ "ecsd.cass_curr AS curr_code, SUM(ecsd.cass_count*ecsd.cass_value) AS denom_count "
			+ "FROM t_cm_enc_cashout_stat ecs "
			+ "JOIN t_cm_enc_cashout_stat_details ecsd ON (ecsd.encashment_id = ecs.encashment_id) "
			+ "WHERE ecsd.action_type = 1 GROUP BY atm_id,enc_date,ecs.encashment_id,cass_curr ) "
			+ "SELECT SUM((ci.denom_count+ co.denom_count)*(co.enc_date - ci.enc_date)*24) as losts "
			+ "FROM curr_encashment_in ci  "
			+ "left OUTER JOIN v_cm_cashout_stat_enc2enc e2e on (ci.encashment_id = e2e.encashment_id) "
			+ "left OUTER JOIN curr_encashment_out co on (co.atm_id = ci.atm_id and co.encashment_id = e2e.next_encashment_id "
			+ "AND co.curr_code = ci.curr_code) WHERE ci.atm_id = #{atmId} AND ci.enc_date > #{startDate} "
			+ "AND ci.enc_date < #{encDate} AND ci.curr_code = #{currCode} ")
	@Options(useCache = true)
	Double getEncLostsForCurr(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate, @Param("currCode") Integer currCode);
	
	@Results({
			@Result(column = "encashment_id", property = "fisrt", javaType = Double.class),
			@Result(column = "CURR_CODE", property = "second", javaType = Integer.class),
			@Result(column = "CURR_SUMM", property = "third", javaType = Long.class) 
	})
	@ResultType(TripleObject.class)
	@Select("SELECT atm_id, ecs.encashment_id, ecsd.cass_curr AS curr_code, "
			+ "SUM(ecsd.cass_count*ecsd.cass_value) AS curr_summ FROM t_cm_enc_cashout_stat ecs "
			+ "JOIN t_cm_enc_cashout_stat_details ecsd ON (ecsd.encashment_id = ecs.encashment_id) "
			+ "WHERE ecsd.action_type = 2 AND ecs.atm_id = #{atmId} AND ecs.enc_date > #{startDate} AND ecs.enc_date < #{endDate} "
			+ "AND not exists (SELECT null FROM t_cm_enc_cashin_stat ecsi "
			+ "WHERE ecs.atm_id = ecsi.atm_id and abs(ecsi.cash_in_enc_date - ecs.enc_date) < 1/24) "
			+ "GROUP BY atm_id,enc_date,ecs.encashment_id,cass_curr ")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Integer, Integer, Long>> getSplitEncCurrList(@Param("atmId") Integer atmId,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate);

	@Results({
			@Result(column = "encashment_id", property = "fisrt", javaType = Double.class),
			@Result(column = "CURR_CODE", property = "second", javaType = Integer.class),
			@Result(column = "CURR_SUMM", property = "third", javaType = Long.class) 
	})
	@ResultType(TripleObject.class)
	@Select("SELECT atm_id, ecs.encashment_id, ecsd.cass_curr AS curr_code, "
			+ "SUM(ecsd.cass_count*ecsd.cass_value) AS curr_summ FROM t_cm_enc_cashout_stat ecs "
			+ "JOIN t_cm_enc_cashout_stat_details ecsd ON (ecsd.encashment_id = ecs.encashment_id) "
			+ "WHERE ecsd.action_type = 2 AND ecs.atm_id = #{atmId} AND ecs.enc_date > #{startDate} AND ecs.enc_date < #{endDate} "
			+ "AND exists (SELECT null FROM t_cm_enc_cashin_stat ecsi "
			+ "WHERE ecs.atm_id = ecsi.atm_id and abs(ecsi.cash_in_enc_date - ecs.enc_date) < 1/24) "
			+ "GROUP BY atm_id,enc_date,ecs.encashment_id,cass_curr ")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Integer, Integer, Long>> getJointEncCurrList(@Param("atmId") Integer atmId,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate);
	
	@Result(column = "cnt", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(ecs.cash_in_encashment_id) AS cnt FROM t_cm_enc_cashin_stat ecs WHERE "
			+ " ecs.atm_id = #{atmId} AND ecs.cash_in_enc_date > #{startDate} AND ecs.cash_in_enc_date < #{endDate} "
			+ "AND not exists (SELECT null FROM t_cm_enc_cashout_stat ecsi "
			+ "WHERE ecs.atm_id = ecsi.atm_id and abs(ecsi.enc_date - ecs.cash_in_enc_date) < 1/24)")
	Integer getSplitCiEncCount(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate);
	
	@Results({
			@Result(column = "ENCASHMENT_ID", javaType = Integer.class),
			@Result(column = "ENC_DATE", javaType = Timestamp.class)
	})
	@ResultType(AtmPeriodEncashmentItem.class)
	@Select("SELECT ENCASHMENT_ID,ENC_DATE FROM ( SELECT ENCASHMENT_ID,ENC_DATE FROM T_CM_ENC_CASHOUT_STAT "
			+ "WHERE ATM_ID = #{atmId} AND ENC_DATE <= #{endDate} AND ENC_DATE >= #{startDate} UNION ALL "
			+ "SELECT CASH_IN_ENCASHMENT_ID as ENCASHMNET_ID,CASH_IN_ENC_DATE as ENC_DATE "
			+ "FROM T_CM_ENC_CASHIN_STAT ecs WHERE ATM_ID = #{atmId} AND ecs.CASH_IN_ENC_DATE <= #{endDate} "
			+ "AND ecs.CASH_IN_ENC_DATE >= #{startDate} AND not exists (SELECT null FROM t_cm_enc_cashout_stat ecsi "
			+ "WHERE ecs.atm_id = ecsi.atm_id and abs(ecsi.enc_date - ecs.cash_in_enc_date) < 1/24)"
			+ ") ORDER BY ENC_DATE")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmPeriodEncashmentItem> getEncListStats(@Param("atmId") Integer atmId,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate);
	
	@ConstructorArgs({
			@Arg(column = "CURR_SUMM", javaType = String.class),
			@Arg(column = "CURR_CODE_A3", javaType = String.class)
	})
	@ResultType(Pair.class)
	@Select("SELECT CODE_A3 as CURR_CODE_A3, SUM(CASS_COUNT*CASS_VALUE) as CURR_SUMM "
			+ "FROM T_CM_ENC_CASHOUT_STAT_DETAILS join T_CM_CURR ci on (CASS_CURR = ci.code_n3) "
			+ "WHERE ENCASHMENT_ID = #{encPeriodId} AND ACTION_TYPE = 2 GROUP BY CODE_A3 ORDER BY CURR_SUMM DESC")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getEncCurrs(@Param("encPeriodId") Integer encPeriodId);
	
	@Results({
			@Result(column = "CASS_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "CASS_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "CASS_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class)
	})
	@ResultType(EncashmentCassItem.class)
	@Select("SELECT CASS_VALUE,CASS_COUNT, CASS_CURR , CODE_A3 FROM T_CM_ENC_CASHOUT_STAT_DETAILS "
			+ "join T_CM_CURR ci on (CASS_CURR = ci.code_n3) WHERE ENCASHMENT_ID = #{encPeriodId} "
			+ "AND ACTION_TYPE = 2 ORDER BY CASS_CURR,CASS_VALUE DESC")
	@Options(useCache = true, fetchSize = 1000)
	List<EncashmentCassItem> getEncDenoms(@Param("encPeriodId") Integer encPeriodId);
}
