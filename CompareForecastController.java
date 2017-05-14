package ru.bpc.cm.optimization;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.items.encashments.AtmPeriodEncashmentItem;
import ru.bpc.cm.items.encashments.EncashmentCassItem;
import ru.bpc.cm.optimization.orm.CompareForecastMapper;
import ru.bpc.cm.orm.common.ORMUtils;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

public class CompareForecastController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<CompareForecastMapper> getMapperClass() {
		return CompareForecastMapper.class;
	}

	public static int getEncCount(ISessionHolder sessionHolder, int atmId, Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			CompareForecastMapper mapper = session.getMapper(getMapperClass());
			Integer count = mapper.getEncCount(atmId, new Timestamp(startDate.getTime()),
					new Timestamp(endDate.getTime()));

			return ORMUtils.getNotNullValue(count, 0);
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return 0;
	}

	public static ObjectPair<Integer, Long> getEncPriceWithCurr(ISessionHolder sessionHolder, int atmId, Date startDate,
			Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CompareForecastMapper mapper = session.getMapper(getMapperClass());
			return ORMUtils.getSingleValue(mapper.getEncPriceWithCurr(atmId, new Timestamp(startDate.getTime()),
					new Timestamp(endDate.getTime())), new ObjectPair<Integer, Long>(0, 0L));
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return new ObjectPair<Integer, Long>(0, 0L);
	}

	public static double getEncLostsForCurr(ISessionHolder sessionHolder, int atmId, int currCode, Date startDate,
			Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		double losts = 0;

		Date periodStart = null;
		Date periodEnd = null;
		double loadedFromAtm = 0;
		double loadedToAtm = 0;

		try {
			CompareForecastMapper mapper = session.getMapper(getMapperClass());
			List<TripleObject<Timestamp, Double, Double>> result = mapper.getEncLostsForCurr(atmId,
					new Timestamp(CmUtils.truncateDateToHours(startDate).getTime()),
					new Timestamp(CmUtils.truncateDateToHours(endDate).getTime()), currCode);

			if (!result.isEmpty()) {
				TripleObject<Timestamp, Double, Double> firstElem = ORMUtils.getSingleValue(result);
				periodStart = firstElem.getFirst();
				loadedToAtm = firstElem.getSecond();

				for (TripleObject<Timestamp, Double, Double> item : result) {
					if (item == firstElem) {
						continue;
					}
					periodEnd = item.getFirst();
					loadedFromAtm = item.getThird();

					losts += (loadedFromAtm + loadedToAtm) * CmUtils.getDaysBetweenTwoDates(periodStart, periodEnd)
							* 24;

					periodStart = item.getFirst();
					loadedToAtm = item.getSecond();
				}
			}
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}

		return losts;
	}

	public static ObjectPair<Integer, Long> getEncLostsForLastEnc(ISessionHolder sessionHolder, int atmId,
			Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			CompareForecastMapper mapper = session.getMapper(getMapperClass());
			return ORMUtils
					.getNotNullValue(
							ORMUtils.getSingleValue(mapper.getEncLostsForLastEnc(atmId,
									new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()))),
							new ObjectPair<Integer, Long>(0, 0L));
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return new ObjectPair<Integer, Long>(0, 0L);
	}

	public static List<AtmPeriodEncashmentItem> getEncListForecast(ISessionHolder sessionHolder, int atmId,
			Date startDate, Date endDate) {
		List<AtmPeriodEncashmentItem> res = new ArrayList<AtmPeriodEncashmentItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CompareForecastMapper mapper = session.getMapper(getMapperClass());
			res.addAll(mapper.getEncListForecast(atmId, new Timestamp(startDate.getTime()),
					new Timestamp(endDate.getTime())));
		} catch (Exception e) {
			logger.error("", e);

		} finally {
			session.close();
		}
		for (AtmPeriodEncashmentItem item : res) {
			item.setAtmCurrencies(getEncCurrs(sessionHolder, item.getEncPeriodId()));
			item.setAtmCassettes(getEncDenoms(sessionHolder, item.getEncPeriodId()));
		}
		return res;
	}

	private static List<Pair> getEncCurrs(ISessionHolder sessionHolder, int encPeriodId) {
		List<Pair> res = new ArrayList<Pair>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CompareForecastMapper mapper = session.getMapper(getMapperClass());
			res.addAll(mapper.getEncCurrs(encPeriodId));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}

	private static List<EncashmentCassItem> getEncDenoms(ISessionHolder sessionHolder, int encPeriodId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<EncashmentCassItem> atmEncashmentCassList = new ArrayList<EncashmentCassItem>();
		try {
			CompareForecastMapper mapper = session.getMapper(getMapperClass());
			atmEncashmentCassList.addAll(mapper.getEncDenoms(encPeriodId));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

}
