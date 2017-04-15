package ru.bpc.cm.encashments;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.encashments.orm.AtmEncashmentSummsMapper;
import ru.bpc.cm.items.encashments.EncashmentCassItem;
import ru.bpc.cm.utils.Pair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class AtmEncashmentSummsController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<AtmEncashmentSummsMapper> getMapperClass() {
		return AtmEncashmentSummsMapper.class;
	}

	protected static int getEncsCount(ISessionHolder sessionHolder, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer count = session.getMapper(getMapperClass()).getEncsCount(JdbcUtils.getSqlDate(date));
			return count == null ? 0 : count;
		} catch (Exception e) {
			logger.error("", e);
			return 0;
		} finally {
			session.close();
		}
	}

	protected static List<EncashmentCassItem> getEncsDenoms(ISessionHolder sessionHolder, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<EncashmentCassItem> atmEncashmentCassList = new ArrayList<EncashmentCassItem>();
		try {
			atmEncashmentCassList.addAll(session.getMapper(getMapperClass()).getEncsDenoms(JdbcUtils.getSqlDate(date)));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getEncsCurrs(ISessionHolder sessionHolder, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			atmEncashmentCassList.addAll(session.getMapper(getMapperClass()).getEncsCurrs(JdbcUtils.getSqlDate(date)));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<EncashmentCassItem> getEncsDenoms(ISessionHolder sessionHolder, int atmId, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<EncashmentCassItem> atmEncashmentCassList = new ArrayList<EncashmentCassItem>();
		try {
			atmEncashmentCassList
					.addAll(session.getMapper(getMapperClass()).getEncsDenomsById(JdbcUtils.getSqlDate(date), atmId));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getEncsCurrs(ISessionHolder sessionHolder, int atmId, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			atmEncashmentCassList
					.addAll(session.getMapper(getMapperClass()).getEncsCurrsById(JdbcUtils.getSqlDate(date), atmId));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static int getEncsCountForPeriod(ISessionHolder sessionHolder, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer count = session.getMapper(getMapperClass()).getEncsCountForPeriod(JdbcUtils.getSqlDate(date));
			return count == null ? 0 : count;
		} catch (Exception e) {
			logger.error("", e);
			return 0;
		} finally {
			session.close();
		}
	}

	protected static List<EncashmentCassItem> getEncsDenomsForPeriod(ISessionHolder sessionHolder, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<EncashmentCassItem> atmEncashmentCassList = new ArrayList<EncashmentCassItem>();
		try {
			atmEncashmentCassList
					.addAll(session.getMapper(getMapperClass()).getEncsDenomsForPeriod(JdbcUtils.getSqlDate(date)));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getEncsCurrsForPeriod(ISessionHolder sessionHolder, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			atmEncashmentCassList
					.addAll(session.getMapper(getMapperClass()).getEncsCurrsForPeriod(JdbcUtils.getSqlDate(date)));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<EncashmentCassItem> getEncsDenomsForPeriod(ISessionHolder sessionHolder, int atmId,
			Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<EncashmentCassItem> atmEncashmentCassList = new ArrayList<EncashmentCassItem>();
		try {
			atmEncashmentCassList.addAll(
					session.getMapper(getMapperClass()).getEncsDenomsForPeriodById(JdbcUtils.getSqlDate(date), atmId));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getEncsCurrsForPeriod(ISessionHolder sessionHolder, int atmId, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			atmEncashmentCassList.addAll(
					session.getMapper(getMapperClass()).getEncsCurrsForPeriodById(JdbcUtils.getSqlDate(date), atmId));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

}
