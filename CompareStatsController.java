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
import ru.bpc.cm.optimization.orm.CompareStatsMapper;
import ru.bpc.cm.orm.common.ORMUtils;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

public class CompareStatsController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<CompareStatsMapper> getMapperClass() {
		return CompareStatsMapper.class;
	}

	public static int getEncCount(ISessionHolder sessionHolder, int atmId, Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int cnt = 0;

		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			Integer count = 0;
			count = mapper.getEncCount_1(atmId, new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()));
			cnt += ORMUtils.getNotNullValue(count, 0);

			count = mapper.getEncCount_2(atmId, new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()));
			cnt += ORMUtils.getNotNullValue(count, 0);

			count = mapper.getEncCount_3(atmId, new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()));
			cnt += ORMUtils.getNotNullValue(count, 0);
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return cnt;
	}

	public static double getPeriodStartLostsForCurr(ISessionHolder sessionHolder, int atmId, int currCode,
			Date startDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			Double losts = mapper.getPeriodStartLostsForCurr(atmId,
					new Timestamp(CmUtils.truncateDateToHours(startDate).getTime()), currCode);
			return ORMUtils.getNotNullValue(losts, 0.0);
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return 0.0;
	}

	public static double getPeriodEndLostsForCurr(ISessionHolder sessionHolder, int atmId, int currCode, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			Double losts = mapper.getPeriodEndLostsForCurr(atmId,
					new Timestamp(CmUtils.truncateDateToHours(endDate).getTime()), currCode);
			return ORMUtils.getNotNullValue(losts, 0.0);
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return 0.0;
	}

	public static double getEncLostsForCurr(ISessionHolder sessionHolder, int atmId, int currCode, Date startDate,
			Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			Double losts = mapper.getEncLostsForCurr(atmId, new Timestamp(startDate.getTime()),
					new Timestamp(endDate.getTime()), currCode);
			return ORMUtils.getNotNullValue(losts, 0.0);
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return 0.0;
	}

	public static List<List<ObjectPair<Integer, Long>>> getSplitEncCurrList(ISessionHolder sessionHolder, int atmId,
			Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<List<ObjectPair<Integer, Long>>> encStatsCurrList = null;
		List<ObjectPair<Integer, Long>> encCurrList = null;

		int encId = 0;
		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			List<TripleObject<Integer, Integer, Long>> result = mapper.getSplitEncCurrList(atmId,
					new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()));
			encStatsCurrList = new ArrayList<List<ObjectPair<Integer, Long>>>();
			encCurrList = new ArrayList<ObjectPair<Integer, Long>>();

			for (TripleObject<Integer, Integer, Long> item : result) {
				if (encId != item.getFirst()) {
					if (encId != 0) {
						encStatsCurrList.add(encCurrList);
					}
					encCurrList = new ArrayList<ObjectPair<Integer, Long>>();
					encId = item.getFirst();

					encCurrList.add(new ObjectPair<Integer, Long>(item.getSecond(), item.getThird()));
				} else {
					encCurrList.add(new ObjectPair<Integer, Long>(item.getSecond(), item.getThird()));
				}
			}
			if (encId != 0) {
				encStatsCurrList.add(encCurrList);
			}
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return encStatsCurrList == null ? new ArrayList<List<ObjectPair<Integer, Long>>>() : encStatsCurrList;
	}

	public static List<List<ObjectPair<Integer, Long>>> getJointEncCurrList(ISessionHolder sessionHolder, int atmId,
			Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<List<ObjectPair<Integer, Long>>> encStatsCurrList = null;
		List<ObjectPair<Integer, Long>> encCurrList = null;

		int encId = 0;
		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			List<TripleObject<Integer, Integer, Long>> result = mapper.getJointEncCurrList(atmId,
					new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()));

			encStatsCurrList = new ArrayList<List<ObjectPair<Integer, Long>>>();
			encCurrList = new ArrayList<ObjectPair<Integer, Long>>();

			for (TripleObject<Integer, Integer, Long> item : result) {
				if (encId != item.getFirst()) {
					if (encId != 0) {
						encStatsCurrList.add(encCurrList);
					}
					encCurrList = new ArrayList<ObjectPair<Integer, Long>>();
					encId = item.getFirst();

					encCurrList.add(new ObjectPair<Integer, Long>(item.getSecond(), item.getThird()));
				} else {
					encCurrList.add(new ObjectPair<Integer, Long>(item.getSecond(), item.getThird()));
				}
			}
			if (encId != 0) {
				encStatsCurrList.add(encCurrList);
			}
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return encStatsCurrList == null ? new ArrayList<List<ObjectPair<Integer, Long>>>() : encStatsCurrList;
	}

	public static int getSplitCiEncCount(ISessionHolder sessionHolder, int atmId, Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int cnt = 0;

		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			Integer count = mapper.getSplitCiEncCount(atmId, new Timestamp(startDate.getTime()),
					new Timestamp(endDate.getTime()));
			cnt += ORMUtils.getNotNullValue(count, 0);
		} catch (Exception e) {
			logger.error(String.valueOf(atmId), e);
		} finally {
			session.close();
		}
		return cnt;
	}

	public static List<AtmPeriodEncashmentItem> getEncListStats(ISessionHolder sessionHolder, int atmId, Date startDate,
			Date endDate) {
		List<AtmPeriodEncashmentItem> res = new ArrayList<AtmPeriodEncashmentItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
			res.addAll(mapper.getEncListStats(atmId, new Timestamp(startDate.getTime()),
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
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
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
			CompareStatsMapper mapper = session.getMapper(getMapperClass());
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
