package ru.bpc.cm.monitoring;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.CurrencyConverter;
import ru.bpc.cm.cashmanagement.orm.items.CodesItem;
import ru.bpc.cm.filters.MonitoringFilter;
import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.Currency;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.forecast.ForecastStatDay;
import ru.bpc.cm.items.monitoring.AtmActualStateItem;
import ru.bpc.cm.items.monitoring.AtmCashOutCassetteItem;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;
import ru.bpc.cm.items.monitoring.AtmRecyclingCassetteItem;
import ru.bpc.cm.items.monitoring.MonitoringException;
import ru.bpc.cm.monitoring.orm.AtmActualStateMapper;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class ActualStateController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");
	private static final String CASS_NA = "na";
	private static final String CASS_CI = "ci";
	private static final double ALERT_DELTA_LOW = 30000;
	private static final double ALERT_DELTA_HIGH = 300000;
	private static final int ALERT_DELTA_CURR = 810;
	private static final int BALANCES_CHECK_THRESHHOLD = 200;
	private static final String BALANCES_CHECK_ERROR_FORMAT = "Wrong balance for ATM: ID={}; CASS_TYPE = {}; CASS_NUMBER = {}; LOADED_REMAINING = {}; CALCULATED_REMAINING = {}";

	public static AtmActualStateMapper getMapper(SqlSession session) {
		AtmActualStateMapper mapper = session.getMapper(AtmActualStateMapper.class);
		if (mapper == null)
			throw new IllegalArgumentException("Mapper can't be null");
		return mapper;
	}
	
	public static List<AtmActualStateItem> getAtmActualStateList(ISessionHolder sessionHolder, MonitoringFilter addFilter) {
		SqlSession session = sessionHolder.getSession(AtmActualStateMapper.class);
		try {
			AtmActualStateMapper mapper = getMapper(session);
			List<AtmActualStateItem> atmActualStateList = Optional.ofNullable(mapper.getAtmActualStateList(addFilter))
					.orElse(new ArrayList<AtmActualStateItem>());
	
			for (AtmActualStateItem item : atmActualStateList) {
				List<AtmCashOutCassetteItem> atmCashOutCassettes = Optional
						.ofNullable(
								mapper.getAtmCashOutCassettesList(item.getAtmID(), AtmCassetteType.CASH_OUT_CASS.getId(),
										item.getEncID(), new Timestamp(item.getStatDate().getTime())))
						.orElse(new ArrayList<AtmCashOutCassetteItem>());
				item.getCashOutCassettes().addAll(atmCashOutCassettes);
				List<AtmRecyclingCassetteItem> atmRecyclingCassette = Optional
						.ofNullable(mapper.getAtmRecyclingCassettesList(item.getAtmID(), item.getCashInEncId(),
								new Timestamp(item.getStatDate().getTime()), AtmCassetteType.CASH_IN_R_CASS.getId()))
						.orElse(new ArrayList<AtmRecyclingCassetteItem>());
				item.getCashInRCassettes().addAll(atmRecyclingCassette);
			}
			return atmActualStateList;
		} finally {
			session.close();
		}
	}

	private static int checkDeltaCoeff(int lastHourDemand,int lastThreeHourDemand, double deltaLow, double deltaHigh){
		if(lastThreeHourDemand == 0){
			if(lastHourDemand > deltaLow){
				return lastHourDemand;
			}
		} else {
			double coeff = (double)lastHourDemand/(double)lastThreeHourDemand;
			
			if(coeff >= 0.35 && coeff <= 3.0){
				if(Math.abs(lastThreeHourDemand-lastHourDemand) > deltaHigh){
					return Math.abs(lastThreeHourDemand-lastHourDemand);
				}
			} else {
				if(Math.abs(lastThreeHourDemand-lastHourDemand) > deltaLow){
					return Math.abs(lastThreeHourDemand-lastHourDemand);
				}
			}
		}
		return 0;
	}

	protected static List<Integer> getCurrenciesList(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(AtmActualStateMapper.class);
		List<Integer> res = new ArrayList<Integer>();
		try {
			AtmActualStateMapper mapper = getMapper(session);
			for (CodesItem item : mapper.getCurrenciesList(atmId)) {
				res.add(item.getMainCurrCode());
				res.add(item.getSecCurrCode());
				res.add(item.getSec2CurrCode());
				res.add(item.getSec3CurrCode());
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId, e);
		} finally {
			session.close();
		}
		return res;
	}
	// TODO
	public static ObjectPair<Date, Integer> getCashOutLastStat(Connection con, int atmId) {
		AtmActualStateMapper mapper = getMapper(null);
		ObjectPair<Date, Integer> res = null;
		try {
			res = mapper.getCashOutLastStat(atmId);
		} catch (Exception e) {
			logger.error("atmID = " + atmId, e);
		}
		return res;
	}

	protected static int getCashOutHoursFromLastWithdrawal(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int res=0;
		try {
			String query =
					"select t1.stat_date, t2.cass_count "+
					"from (select atm_id, max(stat_date) as stat_date "+
					"from T_CM_CASHOUT_CASS_STAT where cass_count<>0 group by atm_id) t1, T_CM_CASHOUT_CASS_STAT t2 "+
					"where t1.atm_id=t2.atm_id and t1.stat_date=t2.stat_date and t1.atm_id=?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res = CmUtils.getHoursBetweenTwoDates(rs.getTimestamp("stat_date"), new Date());
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}

	protected static int getCashInHoursFromLastAddition(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int res=0;
		try {
			String query =
					"select t1.stat_date, t2.bills_count "+
					"from (select atm_id, max(stat_date) as stat_date "+
					"from T_CM_CASHIN_STAT where bills_count<>0 group by atm_id) t1, T_CM_CASHIN_STAT t2 "+
					"where t1.atm_id=t2.atm_id and t1.stat_date=t2.stat_date and t1.atm_id=?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res = CmUtils.getHoursBetweenTwoDates(rs.getTimestamp("stat_date"), new Date());
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}

	public static ObjectPair<Date,Integer> getCashInLastStat(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ObjectPair<Date,Integer> res = null;
		try {
			String query =
				"select max(stat_date) as stat_date,max(cash_in_encashment_id) as enc_id "+
				"from T_CM_CASHIN_STAT "+
				"where atm_id = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res = new ObjectPair<Date,Integer>(rs.getTimestamp("stat_date"),rs.getInt("enc_id"));
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}

	protected static double getAverageDemandStat(
			Map<Date, ForecastStatDay> days, Date startDate, Date endDate, int atmNotAvailableDays) {
		Date currentDate;
		double averageDemand = 0;
		double takeOffSumm = 0;
		int daysSize=7;
		//daysSize = days.size() == 0 ? 1 : days.size();
		for (Entry<Date, ForecastStatDay> entry : days.entrySet()) {
			currentDate=CmUtils.truncateDate(entry.getKey());
			if ((currentDate.compareTo(startDate)==0 || currentDate.after(startDate)) && (currentDate.compareTo(endDate)==0 || currentDate.before(endDate)))
					takeOffSumm += entry.getValue().getTakeOffs();
		}

		averageDemand = takeOffSumm / (daysSize - atmNotAvailableDays);
		
		return (averageDemand / (daysSize - atmNotAvailableDays));
	}

	// UPDATE LAST_UPDATE_DATE ONLY ON TASK CALL
	protected static void actualizeAtmStateForAtm(Connection con,AnyAtmForecast forecast, 
				ObjectPair<Date,Integer> cashOutStat,
				ObjectPair<Date,Integer> cashInStat, boolean updateLastUpdateDate){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		int check = 0;
		int paramIndex = 0;
		
		List<Currency> currencies = forecast.getAtmCurrencies();
		int currCount = currencies == null ? 0 : currencies.size();
		
		int mainCurrInDifference = 0;
		int mainCurrOutDifference = 0;
		int secCurrInDifference = 0;
		int secCurrOutDifference = 0;
		int sec2CurrInDifference = 0;
		int sec2CurrOutDifference = 0;
		int sec3CurrInDifference = 0;
		int sec3CurrOutDifference = 0;
		
		Currency mainCurrItem = currCount > 0 ? currencies.get(0) : null;
		Currency secCurrItem = currCount > 1 ? currencies.get(1) : null;
		Currency sec2CurrItem = currCount > 2 ? currencies.get(2) : null;
		Currency sec3CurrItem = currCount > 3 ? currencies.get(3) : null;
		CurrencyConverter converter = forecast.getCurrencyConverter();
		
		
		if(mainCurrItem != null){
			mainCurrInDifference = checkDeltaCoeff(
					mainCurrItem.getAvgStatRecInCurrLastHourDemand(), 
					mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			mainCurrOutDifference = checkDeltaCoeff(
					mainCurrItem.getAvgStatOutLastHourDemand()+mainCurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					mainCurrItem.getAvgStatOutLastThreeHoursDemand()+mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		if(secCurrItem != null){
			secCurrInDifference = checkDeltaCoeff(
					secCurrItem.getAvgStatRecInCurrLastHourDemand(), 
					secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			secCurrOutDifference = checkDeltaCoeff(
					secCurrItem.getAvgStatOutLastHourDemand()+secCurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					secCurrItem.getAvgStatOutLastThreeHoursDemand()+secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		if(sec2CurrItem != null){
			sec2CurrInDifference = checkDeltaCoeff(
					sec2CurrItem.getAvgStatRecInCurrLastHourDemand(), 
					sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			sec2CurrOutDifference = checkDeltaCoeff(
					sec2CurrItem.getAvgStatOutLastHourDemand()+sec2CurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					sec2CurrItem.getAvgStatOutLastThreeHoursDemand()+sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		
		if(sec3CurrItem != null){
			sec3CurrInDifference = checkDeltaCoeff(
					sec3CurrItem.getAvgStatRecInCurrLastHourDemand(), 
					sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			sec3CurrOutDifference = checkDeltaCoeff(
					sec3CurrItem.getAvgStatOutLastHourDemand()+sec3CurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					sec3CurrItem.getAvgStatOutLastThreeHoursDemand()+sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		
		
		try {
			
			String query =
				"SELECT count(1) as vcheck "+
			    "FROM T_CM_ATM_ACTUAL_STATE "+
			    "where atm_id = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, forecast.getAtmId());
			rs = pstmt.executeQuery();
			rs.next();
			check = rs.getInt("vcheck");

			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);

			
			if(check == 0){
				query =
					" INSERT INTO T_CM_ATM_ACTUAL_STATE "+
			        "(ATM_ID," +
			        "CASH_OUT_STAT_DATE,CASH_OUT_ENCASHMENT_ID, " +
			        "CASH_IN_STAT_DATE,CASH_IN_ENCASHMENT_ID, " +
			        "LAST_UPDATE," +
			        "OUT_OF_CASH_OUT_DATE,OUT_OF_CASH_OUT_CURR,OUT_OF_CASH_OUT_RESP," +
			        "OUT_OF_CASH_IN_DATE,OUT_OF_CASH_IN_RESP," +
			        "LAST_WITHDRAWAL_HOURS,LAST_ADDITION_HOURS," +
			        "CURR_REMAINING_ALERT) "+
			        "VALUES "+
			        "(?, " +
			        "?, ?, " +
			        "?, ?, " +
			        "?, " +
			        "?, ?, ?, " +
			        "?, ?, " +
			        "?, ?," +
			        "?)";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(++paramIndex, forecast.getAtmId());
				pstmt.setTimestamp(++paramIndex, new Timestamp(cashOutStat.getKey().getTime()));
				pstmt.setInt(++paramIndex, cashOutStat.getValue());
				if(cashInStat != null && cashInStat.getKey() != null){
					pstmt.setTimestamp(++paramIndex, new Timestamp(cashInStat.getKey().getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				if(cashInStat != null && cashInStat.getValue() != null){
					pstmt.setInt(++paramIndex, cashInStat.getValue());
				} else {
					pstmt.setInt(++paramIndex, 0);
				}
				pstmt.setTimestamp(++paramIndex, new Timestamp(new Date().getTime()));
				
				if(forecast.getOutOfCashOutDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashOutDate()).getTime()));
					pstmt.setInt(++paramIndex, forecast.getOutOfCashOutCurr());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
					pstmt.setNull(++paramIndex, java.sql.Types.INTEGER);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashOutResp());
				
				if(forecast.getOutOfCashInDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashInDate()).getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashInResp());
				
								
				pstmt.setInt(++paramIndex, getCashOutHoursFromLastWithdrawal(con, forecast.getAtmId()));
				pstmt.setInt(++paramIndex, getCashInHoursFromLastAddition(con, forecast.getAtmId()));
				pstmt.setBoolean(++paramIndex, forecast.isNeedCurrRemainingAlert());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
				query = 
				"INSERT INTO T_CM_ATM_AVG_DEMAND "+
					"(ATM_ID, "+
					"MAIN_CURR_CI, "+
					"MAIN_CURR_CO, "+
					"MAIN_CURR_CI_LAST_HOUR_DIFF, "+
					"MAIN_CURR_CO_LAST_HOUR_DIFF, "+
					"MAIN_CURR_CI_LAST_THREE_HOURS, "+
					"MAIN_CURR_CO_LAST_THREE_HOURS, "+
					"SEC_CURR_CI, "+
					"SEC_CURR_CO, "+
					"SEC_CURR_CI_LAST_HOUR_DIFF, "+
					"SEC_CURR_CO_LAST_HOUR_DIFF, "+
					"SEC_CURR_CI_LAST_THREE_HOURS, "+
					"SEC_CURR_CO_LAST_THREE_HOURS, "+
					"SEC2_CURR_CI, "+
					"SEC2_CURR_CO, "+
					"SEC2_CURR_CI_LAST_HOUR_DIFF, "+
					"SEC2_CURR_CO_LAST_HOUR_DIFF, "+
					"SEC2_CURR_CI_LAST_THREE_HOURS, "+
					"SEC2_CURR_CO_LAST_THREE_HOURS) " +
					"SEC3_CURR_CI, "+
					"SEC3_CURR_CO, "+
					"SEC3_CURR_CI_LAST_HOUR_DIFF, "+
					"SEC3_CURR_CO_LAST_HOUR_DIFF, "+
					"SEC3_CURR_CI_LAST_THREE_HOURS, "+
					"SEC3_CURR_CO_LAST_THREE_HOURS) " +
				"VALUES " +
					"(?," +
					"?, ?, ?, ?, ?, ?, " +
					"?, ?, ?, ?, ?, ?, " +
					"?, ?, ?, ?, ?, ?, " +
					"?, ?, ?, ?, ?, ?)";

				pstmt = con.prepareStatement(query);
				paramIndex = 0;

				pstmt.setInt(++paramIndex, forecast.getAtmId());
				if(mainCurrItem != null){
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutLastHourDemand()+mainCurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, mainCurrInDifference);
					pstmt.setDouble(++paramIndex, mainCurrOutDifference);
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutLastThreeHoursDemand()+mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(secCurrItem != null){
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutLastHourDemand()+secCurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, secCurrInDifference);
					pstmt.setDouble(++paramIndex, secCurrOutDifference);
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutLastThreeHoursDemand()+secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec2CurrItem != null){
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutLastHourDemand()+sec2CurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec2CurrInDifference);
					pstmt.setDouble(++paramIndex, sec2CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutLastThreeHoursDemand()+sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec3CurrItem != null){
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutLastHourDemand()+sec3CurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec3CurrInDifference);
					pstmt.setDouble(++paramIndex, sec3CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutLastThreeHoursDemand()+sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}

				
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
			} else {
				query =
					"UPDATE T_CM_ATM_ACTUAL_STATE "+
					"SET CASH_OUT_STAT_DATE = ?, "+
					"CASH_OUT_ENCASHMENT_ID = ?, "+
					"CASH_IN_STAT_DATE = ?, "+
					"CASH_IN_ENCASHMENT_ID = ?, "+
//					"LAST_UPDATE = ? ," +
					"OUT_OF_CASH_OUT_DATE = ?, " +
					"OUT_OF_CASH_OUT_CURR = ?, " +
					"OUT_OF_CASH_OUT_RESP = ?, " +
					"OUT_OF_CASH_IN_DATE = ?, " +
					"OUT_OF_CASH_IN_RESP = ?, " +
					"LAST_WITHDRAWAL_HOURS = ?, "+
					"LAST_ADDITION_HOURS = ?, " +
					"CURR_REMAINING_ALERT = ? "+
					"WHERE atm_id = ? ";
				pstmt = con.prepareStatement(query);
				
				pstmt.setTimestamp(++paramIndex, new Timestamp(cashOutStat.getKey().getTime()));
				pstmt.setInt(++paramIndex, cashOutStat.getValue());
				if(cashInStat != null && cashInStat.getKey() != null){
					pstmt.setTimestamp(++paramIndex, new Timestamp(cashInStat.getKey().getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				if(cashInStat != null && cashInStat.getValue() != null){
					pstmt.setInt(++paramIndex, cashInStat.getValue());
				} else {
					pstmt.setInt(++paramIndex, 0);
				}
				
				if(forecast.getOutOfCashOutDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashOutDate()).getTime()));
					pstmt.setInt(++paramIndex, forecast.getOutOfCashOutCurr());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
					pstmt.setNull(++paramIndex, java.sql.Types.INTEGER);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashOutResp());
				
				if(forecast.getOutOfCashInDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashInDate()).getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashInResp());
				
				pstmt.setInt(++paramIndex, getCashOutHoursFromLastWithdrawal(con, forecast.getAtmId()));
				pstmt.setInt(++paramIndex, getCashInHoursFromLastAddition(con, forecast.getAtmId()));
				
				pstmt.setBoolean(++paramIndex, forecast.isNeedCurrRemainingAlert());
				
				pstmt.setInt(++paramIndex, forecast.getAtmId());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
				query = 
					"UPDATE T_CM_ATM_AVG_DEMAND "+
					"SET "+
						"MAIN_CURR_CI = ?, "+
						"MAIN_CURR_CO = ?, "+
						"MAIN_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"MAIN_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"MAIN_CURR_CI_LAST_THREE_HOURS = ?, "+
						"MAIN_CURR_CO_LAST_THREE_HOURS = ?, "+
						"SEC_CURR_CI = ?, "+
						"SEC_CURR_CO = ?, "+
						"SEC_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"SEC_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"SEC_CURR_CI_LAST_THREE_HOURS = ?, "+
						"SEC_CURR_CO_LAST_THREE_HOURS = ?, "+
						"SEC2_CURR_CI = ?, "+
						"SEC2_CURR_CO = ?, "+
						"SEC2_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"SEC2_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"SEC2_CURR_CI_LAST_THREE_HOURS = ?, "+
						"SEC2_CURR_CO_LAST_THREE_HOURS = ?, " +
						"SEC3_CURR_CI = ?, "+
						"SEC3_CURR_CO = ?, "+
						"SEC3_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"SEC3_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"SEC3_CURR_CI_LAST_THREE_HOURS = ?, "+
						"SEC3_CURR_CO_LAST_THREE_HOURS = ? " +
					"WHERE atm_id = ? ";

				pstmt = con.prepareStatement(query);
				paramIndex = 0;

				if(mainCurrItem != null){
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutDemand()+mainCurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, mainCurrInDifference);
					pstmt.setDouble(++paramIndex, mainCurrOutDifference);
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutLastThreeHoursDemand()+mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(secCurrItem != null){
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutDemand()+secCurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, secCurrInDifference);
					pstmt.setDouble(++paramIndex, secCurrOutDifference);
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutLastThreeHoursDemand()+secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec2CurrItem != null){
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutDemand()+sec2CurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, sec2CurrInDifference);
					pstmt.setDouble(++paramIndex, sec2CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutLastThreeHoursDemand()+sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec3CurrItem != null){
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutDemand()+sec3CurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, sec3CurrInDifference);
					pstmt.setDouble(++paramIndex, sec3CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutLastThreeHoursDemand()+sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				pstmt.setInt(++paramIndex, forecast.getAtmId());
				
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
				if(updateLastUpdateDate){
					query =
						"UPDATE T_CM_ATM_ACTUAL_STATE "+
						"SET "+
						"LAST_UPDATE = ? " +
						"WHERE atm_id = ? ";
					pstmt = con.prepareStatement(query);
					pstmt.setTimestamp(1, new Timestamp(new Date().getTime()));
					pstmt.setInt(2, forecast.getAtmId());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
			}
		} catch (SQLException e) {
			logger.error("atmID = " + forecast.getAtmId(),e);
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}

	protected static void updateInitialsForAtm(Connection con,int atmId,
			int cashInVolume, int rejectVolume, int cashInRVolume){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
		
			String query =
				"UPDATE T_CM_ATM_ACTUAL_STATE "+
				"SET "+
					"CASH_IN_INITIAL = ?, " +
					"REJECT_INITIAL = ?, " +
					"CASH_IN_R_INITIAL = ? " +
				"WHERE atm_id = ? ";
			pstmt = con.prepareStatement(query);
			
			pstmt.setInt(1, cashInVolume);
			pstmt.setInt(2, rejectVolume);
			pstmt.setInt(3, cashInRVolume);
			pstmt.setInt(4, atmId);
			
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
		
		} catch (SQLException e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}

	public static boolean checkAtmActStateTable(Connection connection) {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cnt = 0;
		try {
			String query =
				"SELECT count(1) as vcheck "+
			    "FROM T_CM_ATM_ACTUAL_STATE";
			pstmt = connection.prepareStatement(query);
			rs = pstmt.executeQuery();
			if(rs.next()){
				cnt = rs.getInt("VCHECK");
			}
		} catch (SQLException e){
			logger.error("",e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return cnt > 0;
	}

	protected static void addCashOutCassettes(Connection connection, int atmId, int ecnashmentId,List<AtmCassetteItem> cassList){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			String query =
				"SELECT distinct CASS_NUMBER,CASS_VALUE,CASS_CURR "+
			    "FROM T_CM_CASHOUT_CASS_STAT " +
			    "WHERE encashment_id = ? and atm_id = ? ";
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, ecnashmentId);
			pstmt.setInt(2, atmId);
			rs = pstmt.executeQuery();
			while(rs.next()){
				cassList.add(new AtmCassetteItem(rs.getInt("CASS_NUMBER"), rs.getInt("CASS_VALUE"), 
						rs.getInt("CASS_CURR"), AtmCassetteType.CASH_OUT_CASS, false));
			}
		} catch (SQLException e){
			logger.error("",e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
	}

	protected static void addCashInRecyclingCassettes(Connection connection, int atmId, int cashInEcnashmentId,
			List<AtmCassetteItem> cassList){
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			String query =
				"SELECT distinct CASS_NUMBER,CASS_VALUE,CASS_CURR "+
			    "FROM T_CM_CASHIN_R_CASS_STAT " +
			    "WHERE cash_in_encashment_id = ? and atm_id = ? ";
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, cashInEcnashmentId);
			pstmt.setInt(2, atmId);
			rs = pstmt.executeQuery();
			while(rs.next()){
				cassList.add(new AtmCassetteItem(rs.getInt("CASS_NUMBER"), rs.getInt("CASS_VALUE"), 
						rs.getInt("CASS_CURR"), AtmCassetteType.CASH_IN_R_CASS, false));
			}
		} catch (SQLException e){
			logger.error("",e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		
	}

	public static void saveAtmCassettes(Connection con, int atmId,
			List<AtmCassetteItem> atmCassList) {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		int check = 0;

		try {
			
			String checkQuery =
				"SELECT count(1) as vcheck "+
			    "FROM T_CM_ATM_CASSETTES "+
			    "where " +
			    	"ATM_ID = ? " +
			    	"AND CASS_TYPE = ? " +
			    	"AND CASS_NUMBER = ? ";
			String insertQuery =
					" INSERT INTO T_CM_ATM_CASSETTES "+
			        "(ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_CURR, CASS_VALUE) "+
			        "VALUES "+
			        "(?, ?, ?, ?, ?)";
			String updateQuery =
					"UPDATE T_CM_ATM_CASSETTES "+
			        "SET CASS_CURR = ? , " +
			        	"CASS_VALUE = ? "+
			        "where " +
			    	"ATM_ID = ? " +
			    	"AND CASS_TYPE = ? " +
			    	"AND CASS_NUMBER = ? ";
			String deleteQuery =
					"DELETE FROM T_CM_ATM_CASSETTES "+
			        "where " +
			    	"ATM_ID = ? " +
			    	"AND COALESCE(CASS_CURR,0) = 0 ";
			
			for(AtmCassetteItem cass : atmCassList){
				pstmt = con.prepareStatement(checkQuery);
				pstmt.setInt(1, atmId);
				pstmt.setInt(2, cass.getType().getId());
				pstmt.setInt(3, cass.getNumber());
				rs = pstmt.executeQuery();
				rs.next();
				check = rs.getInt("vcheck");
	
				JdbcUtils.close(rs);
				JdbcUtils.close(pstmt);
				
				
				if(check == 0){
					pstmt = con.prepareStatement(insertQuery);
					pstmt.setInt(1, atmId);
					pstmt.setInt(2, cass.getType().getId());
					pstmt.setInt(3, cass.getNumber());
					pstmt.setInt(4, cass.getCurr());
					pstmt.setInt(5, cass.getDenom());
					
					
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				} else {
					pstmt = con.prepareStatement(updateQuery);
					
					pstmt.setInt(1, cass.getCurr());
					pstmt.setInt(2, cass.getDenom());

					pstmt.setInt(3, atmId);
					pstmt.setInt(4, cass.getType().getId());
					pstmt.setInt(5, cass.getNumber());
			
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
					
				}
			}
			
			pstmt = con.prepareStatement(deleteQuery);
			
			pstmt.setInt(1, atmId);
			pstmt.executeUpdate();
			
			JdbcUtils.close(pstmt);
		} catch (SQLException e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}

	protected static void updateCalculatedRemainingForAtms(Connection con) throws SQLException{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
		
			String query =
				"update t_cm_intgr_cass_balance intbal "+
				"SET CASS_REMAINING_CALC =  "+
				"(  "+
					"select cs.cass_remaining "+
					"from "+
						"t_cm_cashout_cass_stat cs "+
					"where  "+
						"cs.atm_id = intbal.atm_id "+
						"and "+
						"cs.cass_number = intbal.cass_number "+
						"and "+
						"cs.encashment_id = (select CASH_OUT_ENCASHMENT_ID from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
						"and "+
						"cs.stat_date = (select CASH_OUT_STAT_DATE from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
				") "+
				"WHERE "+
				" intbal.cass_type = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, AtmCassetteType.CASH_OUT_CASS.getId());
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
			
			query =
				"update t_cm_intgr_cass_balance intbal "+
				"SET CASS_REMAINING_CALC =  "+
				"( "+
					"select cs.bills_remaining "+
					"from "+
					"t_cm_cashin_stat cs "+
					"where  "+
						"cs.atm_id = intbal.atm_id "+
						"and "+
						"cs.cash_in_encashment_id = (select CASH_IN_ENCASHMENT_ID from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
						"and "+
						"cs.stat_date = (select CASH_IN_STAT_DATE from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
				") "+
				"WHERE "+
					"intbal.cass_type = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, AtmCassetteType.CASH_IN_CASS.getId());
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
			
			query =
				"update t_cm_intgr_cass_balance intbal "+
				"SET CASS_REMAINING_CALC =  "+
				"(  "+
					"select cs.cass_remaining "+
					"from "+
						"t_cm_cashin_r_cass_stat cs "+
					"where  "+
						"cs.atm_id = intbal.atm_id "+
						"and "+
						"cs.cass_number = intbal.cass_number "+
						"and "+
						"cs.cash_in_encashment_id = (select CASH_IN_ENCASHMENT_ID from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
						"and "+
						"cs.stat_date = (select CASH_IN_STAT_DATE from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
				") "+
				"WHERE "+
					"intbal.cass_type = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, AtmCassetteType.CASH_IN_R_CASS.getId());
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
		
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}

	public static void checkLoadedBalances(Connection connection) throws SQLException, MonitoringException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean balancesAreIncorrect = false;

		try {
			String query = "select ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_REMAINING_LOAD, CASS_REMAINING_CALC, BALANCE_STATUS "
					+ "from t_cm_intgr_cass_balance "
					+ "WHERE abs(COALESCE(CASS_REMAINING_LOAD,0) - COALESCE(CASS_REMAINING_CALC,0)) > ? "
					+ " AND BALANCE_STATUS <> 1 " + "ORDER BY ATM_ID, CASS_TYPE, CASS_NUMBER";
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, BALANCES_CHECK_THRESHHOLD);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				balancesAreIncorrect = true;
				logger.error(BALANCES_CHECK_ERROR_FORMAT, rs.getString("ATM_ID"), rs.getString("CASS_TYPE"),
						rs.getString("CASS_NUMBER"), rs.getString("CASS_REMAINING_LOAD"),
						rs.getString("CASS_REMAINING_CALC"));
			}
			if (balancesAreIncorrect) {
				throw new MonitoringException(
						"Loaded balances are incorrect, should be logged in CASH_MANAGEMENT logger");
			}
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}

	}

	public static void checkLoadedBalances(Connection connection, List<Integer> atmList)
			throws SQLException, MonitoringException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean balancesAreIncorrect = false;

		try {
			StringBuffer query = new StringBuffer(
					"select ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_REMAINING_LOAD, CASS_REMAINING_CALC, BALANCE_STATUS "
							+ "from t_cm_intgr_cass_balance "
							+ "WHERE abs(COALESCE(CASS_REMAINING_LOAD,0) - COALESCE(CASS_REMAINING_CALC,0)) > ? "
							+ " AND BALANCE_STATUS <> 1 and ");
			query.append(JdbcUtils.generateInConditionNumber("atm_id", atmList));
			query.append(" ORDER BY ATM_ID, CASS_TYPE, CASS_NUMBER");
			pstmt = connection.prepareStatement(query.toString());
			pstmt.setInt(1, BALANCES_CHECK_THRESHHOLD);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				balancesAreIncorrect = true;
				logger.error(BALANCES_CHECK_ERROR_FORMAT, rs.getString("ATM_ID"), rs.getString("CASS_TYPE"),
						rs.getString("CASS_NUMBER"), rs.getString("CASS_REMAINING_LOAD"),
						rs.getString("CASS_REMAINING_CALC"));
			}
			if (balancesAreIncorrect) {
				throw new MonitoringException(
						"Loaded balances are incorrect, should be logged in CASH_MANAGEMENT logger");
			}
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}

	}

	public static boolean getAtmDeviceState(Connection con, int atmId) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			String query = "SELECT ATM_STATE   " + "FROM T_CM_ATM_ACTUAL_STATE " + "WHERE " + "ATM_ID = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				return rs.getInt("ATM_STATE") == 0;
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return false;
	}

}
