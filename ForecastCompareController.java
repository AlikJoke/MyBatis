package ru.bpc.cm.forecasting.controllers;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.config.utils.ORMUtils;
import ru.bpc.cm.forecasting.anyatm.items.EncashmentForPeriod;
import ru.bpc.cm.forecasting.anyatm.items.ForecastForPeriod;
import ru.bpc.cm.forecasting.orm.ForecastCompareMapper;
import ru.bpc.cm.items.encashments.AtmCurrStatItem;
import ru.bpc.cm.items.forecast.nominal.NominalItem;
import ru.bpc.cm.utils.CmUtils;

public class ForecastCompareController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<ForecastCompareMapper> getMapperClass() {
		return ForecastCompareMapper.class;
	}

	public static void insertCompareForecastData(ISessionHolder sessionHolder, ForecastForPeriod item) {
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			ForecastCompareMapper mapper = session.getMapper(getMapperClass());
			mapper.deleteCompareDenomData(item.getAtmId());
			mapper.deleteCompareStatData(item.getAtmId());
			mapper.deleteCompareData(item.getAtmId());

			if (item.getEncashments() != null) {

				for (EncashmentForPeriod encashment : item.getEncashments()) {
					mapper.insertCompareData(ORMUtils.getNextSequence(session, "SQ_CM_ENC_PLAN_ID"), item.getAtmId(),
							new Timestamp(encashment.getForthcomingEncDate().getTime()),
							encashment.getEncType().getId(), encashment.getForecastResp(), item.isCashInExists(),
							encashment.isEmergencyEncashment(), Math.round(encashment.getEncLosts()),
							Math.round(encashment.getEncPrice()), encashment.getEncLostsCurrCode());

					Integer planId = mapper.getPlanId(ORMUtils.getCurrentSequence(session, "SQ_CM_ENC_PLAN_ID"),
							ORMUtils.getFromDummyExpression(session));
					int encPlanID = 0;
					if (planId != null)
						encPlanID = planId;

					for (NominalItem nom : encashment.getAtmCassettes())
						for (int i = 0; i < nom.getCassCount(); i++)
							mapper.insertCompareDenomData(encPlanID, nom.getCurrency(), nom.getCountInOneCassPlan(),
									nom.getDenom());
				}
			}
			for (Entry<Integer, List<AtmCurrStatItem>> entry : item.getCurrStat().entrySet()) {

				for (AtmCurrStatItem i : entry.getValue())
					mapper.insertCompareForecastData(item.getAtmId(), new Timestamp(i.getStatDate().getTime()),
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

	public static int getStatsDatesCount(ISessionHolder sessionHolder, int atmId, Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer count = session.getMapper(getMapperClass()).getStatsDatesCount(atmId,
					new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()));

			if (count != null)
				return count - CmUtils.getHoursBetweenTwoDates(startDate, endDate) + 2;
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return -1;
	}
}
