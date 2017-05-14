package ru.bpc.cm.optimization;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.items.optimization.ForecastCompareCurrStat;
import ru.bpc.cm.optimization.orm.ForecastStatsCompareMapper;
import ru.bpc.cm.orm.common.ORMUtils;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;

public class ForecastStatsCompareController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<ForecastStatsCompareMapper> getMapperClass() {
		return ForecastStatsCompareMapper.class;
	}

	protected static List<ForecastCompareCurrStat> getCurrRemainings(ISessionHolder sessionHolder, int atmId,
			Integer curr, Date startDate, Date endDate) {
		List<ForecastCompareCurrStat> res = new ArrayList<ForecastCompareCurrStat>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ForecastStatsCompareMapper mapper = session.getMapper(getMapperClass());
			res.addAll(mapper.getCurrRemainings(atmId, curr, new Timestamp(endDate.getTime()),
					new Timestamp(startDate.getTime())));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}

	public static ObjectPair<Date, Date> getCalendarAvailableDays(ISessionHolder sessionHolder) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ForecastStatsCompareMapper mapper = session.getMapper(getMapperClass());
			ObjectPair<Timestamp, Timestamp> pair = ORMUtils.getSingleValue(mapper.getCalendarAvailableDays());
			if (pair != null) {
				return new ObjectPair<Date, Date>(CmUtils.getNVLValue(pair.getKey(), new Date()),
						CmUtils.getNVLValue(pair.getValue(), new Date()));
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return new ObjectPair<Date, Date>(new Date(), new Date());
	}

	public static boolean checkForecastPerformedForAtm(ISessionHolder sessionHolder, int atmId, Date startDate,
			Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ForecastStatsCompareMapper mapper = session.getMapper(getMapperClass());
			Integer count = mapper.checkForecastPerformedForAtm(atmId, new Timestamp(startDate.getTime()),
					new Timestamp(endDate.getTime()));
			return ORMUtils.getNotNullValue(count, 0) > 0;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return false;
	}

}
