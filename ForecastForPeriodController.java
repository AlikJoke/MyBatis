package ru.bpc.cm.forecasting.controllers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.config.utils.ORMUtils;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.forecasting.anyatm.items.EncashmentForPeriod;
import ru.bpc.cm.forecasting.anyatm.items.ForecastForPeriod;
import ru.bpc.cm.forecasting.orm.ForecastForPeriodMapper;
import ru.bpc.cm.items.encashments.AtmCurrStatItem;
import ru.bpc.cm.items.forecast.nominal.NominalItem;
import ru.bpc.cm.utils.Pair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class ForecastForPeriodController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<ForecastForPeriodMapper> getMapperClass() {
		return ForecastForPeriodMapper.class;
	}

	public static List<AtmCurrStatItem> getCoStatDetailsCurr(ISessionHolder sessionHolder, int atmId, Integer currCode,
			Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmCurrStatItem> statDetails = new ArrayList<AtmCurrStatItem>();
		try {
			List<AtmCurrStatItem> statDetailsTemp = session.getMapper(getMapperClass()).getCoStatDetailsCurr(atmId,
					JdbcUtils.getSqlDate(startDate), new Timestamp(endDate.getTime()), currCode);

			boolean isEncInHour = false;
			for (AtmCurrStatItem item : statDetailsTemp) {
				item.setCurrCodeN3(currCode);
				if (!isEncInHour)
					item.setCoRemainingStartDay(item.getCoSummTakeOff() + item.getCoRemainingEndDay());

				if (item.getCnt() == item.getRnk()) {
					if (isEncInHour) {
						item.setSummEncToAtm(item.getCoSummTakeOff() + item.getCoRemainingEndDay());
						isEncInHour = false;
					}
					item.setSummEncFromAtm(Long.valueOf(0));
					statDetails.add(item);
				} else {
					item.setCoSummTakeOff(item.getCoSummTakeOff() + item.getCrSummTakeOff());
					isEncInHour = true;
				}
			}
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return statDetails;
	}

	public static List<AtmCurrStatItem> getCrStatDetailsCurr(ISessionHolder sessionHolder, int atmId, Integer currCode,
			Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmCurrStatItem> statDetails = new ArrayList<AtmCurrStatItem>();

		try {
			List<AtmCurrStatItem> statDetailsTemp = session.getMapper(getMapperClass()).getCrStatDetailsCurr(atmId,
					JdbcUtils.getSqlDate(startDate), new Timestamp(endDate.getTime()), currCode);
			boolean isEncInHour = false;
			for (AtmCurrStatItem item : statDetailsTemp) {
				item.setCurrCodeN3(currCode);
				if (!isEncInHour)
					item.setCrRemainingStartDay(
							-item.getCrSummInsert() + item.getCrSummTakeOff() + item.getCrRemainingEndDay());

				if (item.getRnk() == item.getCnt()) {
					if (isEncInHour) {
						item.setSummEncToAtm(
								-item.getCrSummInsert() + item.getCrSummTakeOff() + item.getCrRemainingEndDay());
						isEncInHour = false;
					}
					item.setSummEncFromAtm(Long.valueOf(0));
					statDetails.add(item);
				} else {
					item.setCrSummTakeOff(item.getCrSummTakeOff() + item.getCiSummInsert());
					item.setCrSummInsert(item.getCrSummInsert());
					isEncInHour = true;
				}
			}
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return statDetails;
	}

	public static List<AtmCurrStatItem> getStatDetailsCashIn(ISessionHolder sessionHolder, int atmId, Date startDate,
			Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmCurrStatItem> statDetails = new ArrayList<AtmCurrStatItem>();

		try {
			List<Integer> encIds = getCiEncIdsForPeriod(sessionHolder, atmId, startDate);
			List<AtmCurrStatItem> statDetailsTemp = session.getMapper(getMapperClass()).getStatDetailsCashIn(atmId,
					JdbcUtils.getSqlDate(startDate), new Timestamp(endDate.getTime()), encIds);
			boolean isEncInHour = false;

			for (AtmCurrStatItem item : statDetailsTemp) {
				item.setCurrCodeA3(CashManagementConstants.CASH_IN_CURR_CODE_A3);
				item.setCurrCodeN3(CashManagementConstants.CASH_IN_CURR_CODE);

				if (!isEncInHour)
					item.setCiRemainingStartDay(item.getCiSummInsert() + item.getCiRemainingEndDay());

				if (item.getRnk() == item.getCnt()) {
					if (isEncInHour)
						isEncInHour = false;
					item.setSummEncFromAtm(Long.valueOf(0));
					statDetails.add(item);
				} else {
					item.setCiSummInsert(item.getCiSummInsert());
					isEncInHour = true;
				}
			}
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return statDetails;
	}

	private static List<Integer> getCiEncIdsForPeriod(ISessionHolder sessionHolder, int atmId, Date encDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Integer> encIds = new ArrayList<Integer>();
		try {
			encIds.addAll(session.getMapper(getMapperClass()).getCiEncIdsForPeriod(atmId,
					new java.sql.Timestamp(encDate.getTime())));
		} catch (Exception e) {
			logger.error("atmID = " + atmId, e);
		} finally {
			session.close();
		}
		return encIds;
	}

	public static Date getStatsEnd(ISessionHolder sessionHolder, int atmId, Date startDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Date res = null;
		try {
			res = session.getMapper(getMapperClass()).getStatsEnd(atmId);
		} catch (Exception e) {
			logger.error("atmID = " + atmId, e);
		} finally {
			session.close();
		}
		if (res == null || res.after(startDate))
			return startDate;
		else
			return res;
	}

	public static void insertPeriodForecastData(ISessionHolder sessionHolder, ForecastForPeriod item) {
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			ForecastForPeriodMapper mapper = session.getMapper(getMapperClass());
			mapper.insertPeriodForecastData_deletePeriodDenom(item.getAtmId());
			mapper.insertPeriodForecastData_deletePeriodCurr(item.getAtmId());
			mapper.insertPeriodForecastData_deletePeriodStat(item.getAtmId());
			mapper.insertPeriodForecastData_deletePeriod(item.getAtmId());

			if (item.getEncashments() != null) {
				for (EncashmentForPeriod encashment : item.getEncashments()) {
					mapper.insertPeriodForecastData_insertPeriod(ORMUtils.getNextSequence(session, "SQ_CM_ENC_PLAN_ID"),
							item.getAtmId(), new Timestamp(encashment.getForthcomingEncDate().getTime()),
							encashment.getEncType().getId(), encashment.getForecastResp(), item.isCashInExists(),
							encashment.isEmergencyEncashment());

					int encPlanID = mapper.getSQ(session);
					for (Pair curr : encashment.getAtmCurrencies())
						mapper.insertPeriodForecastData_insertPeriodCurr(encPlanID, curr.getKey(), curr.getLabel());

					for (NominalItem nom : encashment.getAtmCassettes())
						for (int i = 0; i < nom.getCassCount(); i++)
							mapper.insertPeriodForecastData_insertPeriodDenom(encPlanID, nom.getCurrency(),
									nom.getCountInOneCassPlan(), nom.getDenom());
				}
			}
			for (Entry<Integer, List<AtmCurrStatItem>> entry : item.getCurrStat().entrySet()) {
				for (AtmCurrStatItem i : entry.getValue())
					mapper.insertPeriodForecastData_insertPeriodStat(item.getAtmId(), i.getStatDate().getTime(),
							i.getCurrCodeN3(), i.getCoSummTakeOff(), i.getCoRemainingStartDay(),
							i.getCoRemainingEndDay(), i.getSummEncToAtm(), i.getSummEncFromAtm(),
							i.isEmergencyEncashment(), i.isForecast(), i.isCashAddEncashment(), i.getCiSummInsert(),
							i.getCiRemainingStartDay(), i.getCiRemainingEndDay(), i.getCrSummInsert(),
							i.getCrSummTakeOff(), i.getCrRemainingStartDay(), i.getCrRemainingEndDay());
				mapper.flush();
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

}
