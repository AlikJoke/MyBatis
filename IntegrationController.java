package ru.bpc.cm.integration;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.integration.db.ICashManagementAPI;
import ru.bpc.cm.integration.db.IntegrationAtm;
import ru.bpc.cm.integration.db.IntegrationCashInTransaction;
import ru.bpc.cm.integration.db.IntegrationCassBalance;
import ru.bpc.cm.integration.db.IntegrationCassette;
import ru.bpc.cm.integration.db.IntegrationCurrencyRate;
import ru.bpc.cm.integration.db.IntegrationDowntime;
import ru.bpc.cm.integration.db.IntegrationInst;
import ru.bpc.cm.integration.db.IntegrationTransaction;
import ru.bpc.cm.integration.orm.IntegrationMapper;
import ru.bpc.cm.integration.ws.ICashManagementWSAPI;
import ru.bpc.cm.orm.common.ORMUtils;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class IntegrationController {
	public static final int BATCH_MAX_SIZE = 100;

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<IntegrationMapper> getMapperClass() {
		return IntegrationMapper.class;
	}

	public static Date getLastStatDate(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Date startDate = new Date();
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());
			startDate = ORMUtils.getSingleValue(mapper.getLastStatDate(), new Timestamp(startDate.getTime()));
		} finally {
			session.close();
		}
		return startDate;
	}

	public static long getLastUtrnno(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		long lastUtrnno;
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			lastUtrnno = ORMUtils.getSingleValue(mapper.getLastUtrnno(), 0L);
		} finally {
			session.close();
		}
		return lastUtrnno;
	}

	public static void load_atm_trans(AtomicInteger interruptFlag, ISessionHolder sessionHolder, ICashManagementAPI api)
			throws SQLException {
		_logger.debug("Executing method load_atm_trans");
		List<IntegrationTransaction> vTransList = new ArrayList<IntegrationTransaction>();
		List<IntegrationCashInTransaction> vTransListCashIn = new ArrayList<IntegrationCashInTransaction>();
		List<Integer> vAtmList = new ArrayList<Integer>();
		Long vLastUtrnno;
		Long vLatestUtrnno;
		int vCashInTransCount = 1;
		Date sysDate = new Date();
		Date vDateStart = DateUtils.truncate(sysDate, Calendar.MONTH);
		Date vDateFinish = sysDate;

		Integer tempAtmId = null;
		Date tempDateTime = null;

		SqlSession session = sessionHolder.getSession(getMapperClass());

		String selectAtmsSQL = "SELECT ATM_ID as result from T_CM_ATM";// EXTERNAL_ATM_ID

		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());
			List<TripleObject<Long, Timestamp, Timestamp>> params = mapper.loadAtmTrans_getIntgrParams();
			if (!params.isEmpty()) {
				TripleObject<Long, Timestamp, Timestamp> param = ORMUtils.getSingleValue(params);
				vLastUtrnno = param.getFirst();
				vDateStart = JdbcUtils.getTimestamp(param.getSecond());
				vDateFinish = JdbcUtils.getTimestamp(param.getThird());
			} else {
				vLastUtrnno = 0L;
				vDateStart = DateUtils.truncate(sysDate, Calendar.MONTH);
				vDateFinish = DateUtils.addMinutes(sysDate, -20);
			}

			vDateFinish = DateUtils.addMinutes(sysDate, -20).compareTo(vDateFinish) < 0
					? DateUtils.addMinutes(sysDate, -20) : vDateFinish;

			vAtmList.addAll(mapper.getIntegerValueByQuery(selectAtmsSQL));

			_logger.debug("Executing api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) with parameters:");
			_logger.debug("vAtmList: " + Arrays.toString(vAtmList.toArray()));
			_logger.debug("vLastUtrnno: " + vLastUtrnno);
			_logger.debug("vDateStart: " + vDateStart.toString());
			_logger.debug("vDateFinish: " + vDateFinish.toString());
			if (!(interruptFlag.get() > 0)) {
				vTransList = api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish);
				if (vTransList != null) {
					_logger.debug("Received " + vTransList.size() + " transactions");
				} else {
					_logger.debug(
							"Method api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) returned null");
				}
				_logger.debug("Method api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) executed");
			}

			if (vTransList != null && vTransList.size() > 0) {
				for (IntegrationTransaction vTrans : vTransList) {
					mapper.loadAtmTrans_insertIntgrParams(vTrans.getTerminalId(), vTrans.getOperationId(),
							JdbcUtils.getSqlTimestamp(vTrans.getDatetime()), vTrans.getOpertype(),
							vTrans.getNoteRetracted(), vTrans.getNoteRejected(), vTrans.getBillCass1(),
							vTrans.getDenomCass1(), vTrans.getCurrencyCass1(), vTrans.getTypeCass1(),
							vTrans.getBillCass2(), vTrans.getDenomCass2(), vTrans.getCurrencyCass2(),
							vTrans.getTypeCass2(), vTrans.getBillCass3(), vTrans.getDenomCass3(),
							vTrans.getCurrencyCass3(), vTrans.getTypeCass3(), vTrans.getBillCass4(),
							vTrans.getDenomCass4(), vTrans.getCurrencyCass4(), vTrans.getTypeCass4());
				}
				vTransList.clear();

				ObjectPair<Long, Long> minMaxUntrnno = mapper.loadAtmTrans_getIntgrLastUtrnno();
				if (minMaxUntrnno != null) {
					vLastUtrnno = minMaxUntrnno.getKey();
					vLatestUtrnno = minMaxUntrnno.getValue();
				} else {
					vLastUtrnno = 0L;
					vLatestUtrnno = 0L;
				}

				while (vCashInTransCount > 0) {
					_logger.debug("Executing api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) with parameters:");
					_logger.debug("vLastUtrnno: " + vLastUtrnno);
					_logger.debug("vLatestUtrnno: " + vLatestUtrnno);
					vTransListCashIn = api.getAtmTransCashIn(vLastUtrnno, vLatestUtrnno);
					if (vTransListCashIn != null) {
						_logger.debug("Received " + vTransListCashIn.size() + " transactions");
					} else {
						_logger.debug("Method api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) returned null");
					}
					_logger.debug("Method api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) executed");

					if (vTransListCashIn != null) {
						vCashInTransCount = vTransListCashIn.size();

						if (vCashInTransCount > 0) {
							vLastUtrnno = vTransListCashIn.get(vTransListCashIn.size() - 1).getOperId();
							for (IntegrationCashInTransaction vTransCashIn : vTransListCashIn) {
								ObjectPair<Integer, Timestamp> atmIdDt = ORMUtils.getSingleValue(
										mapper.loadAtmTrans_getIntgrTransAtmIdDt(vTransCashIn.getOperId()));

								if (atmIdDt != null) {
									tempAtmId = atmIdDt.getKey();
									tempDateTime = JdbcUtils.getTimestamp(atmIdDt.getValue());
								} else {
									tempAtmId = null;
									tempDateTime = null;
								}

								if (tempAtmId != null && tempDateTime != null) {
									mapper.loadAtmTrans_insertIntgrCashInTrans(tempAtmId, vTransCashIn.getOperId(),
											JdbcUtils.getSqlTimestamp(tempDateTime), vTransCashIn.getOperType(),
											vTransCashIn.getBillDenom(), vTransCashIn.getBillCurr(),
											vTransCashIn.getBillNum());
								}
							}
						} else {
							vCashInTransCount = 0;
						}
						vTransListCashIn.clear();
					} else {
						vCashInTransCount = 0;
					}

				}
			}
			vTransListCashIn.clear();
		} finally {
			session.close();
		}

		_logger.debug("method load_atm_trans executed");
	}

	public static void load_atm_error_trans(ISessionHolder sessionHolder, ICashManagementAPI api) throws SQLException {
		_logger.debug("Executing method load_atm_trans");
		List<IntegrationTransaction> vTransList = new ArrayList<IntegrationTransaction>();
		List<IntegrationCashInTransaction> vTransListCashIn = new ArrayList<IntegrationCashInTransaction>();
		List<Integer> vAtmList = new ArrayList<Integer>();
		Long vLastUtrnno;
		Long vLatestUtrnno;
		int vCashInTransCount = 1;
		Date sysDate = new Date();
		Date vDateStart = DateUtils.truncate(sysDate, Calendar.MONTH);
		Date vDateFinish = sysDate;

		Integer tempAtmId = null;
		Date tempDateTime = null;

		SqlSession session = sessionHolder.getSession(getMapperClass());

		String selectMaxCoStatTime = "select max(STAT_DATE) as result from T_CM_CASHOUT_CASS_STAT where ATM_ID in (select ATM_ID from T_CM_ATM_INTGR_ERROR)";
		String selectAtmsSQL = "select ATM_ID as result from T_CM_ATM_INTGR_ERROR";

		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());
			ObjectPair<Long, Timestamp> pair = mapper.loadAtmErrorTrans_getMinStartTrans();
			if (pair != null) {
				vLastUtrnno = pair.getKey();
				vDateStart = JdbcUtils.getTimestamp(pair.getValue());
			} else {
				TripleObject<Long, Timestamp, Timestamp> params = ORMUtils
						.getSingleValue(mapper.loadAtmTrans_getIntgrParams());

				if (params != null) {
					vLastUtrnno = params.getFirst();
					vDateStart = JdbcUtils.getTimestamp(params.getSecond());
					vDateFinish = JdbcUtils.getTimestamp(params.getThird());
				} else {
					vLastUtrnno = 0L;
					vDateStart = DateUtils.truncate(sysDate, Calendar.MONTH);
					vDateFinish = DateUtils.addMinutes(sysDate, -20);
				}
			}

			if (vDateStart == null) { // max stat date if null from
										// T_CM_ATM_INTGR_LAST or
										// T_CM_INTGR_PARAMS
				Timestamp statTime = ORMUtils.getSingleValue(mapper.getTimestampValueByQuery(selectMaxCoStatTime));
				vDateStart = statTime;
			}

			vDateFinish = DateUtils.addMinutes(sysDate, -20).compareTo(vDateFinish) < 0
					? DateUtils.addMinutes(sysDate, -20) : vDateFinish;

			vAtmList.addAll(mapper.getIntegerValueByQuery(selectAtmsSQL));

			_logger.debug("Executing api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) with parameters:");
			_logger.debug("vAtmList: " + Arrays.toString(vAtmList.toArray()));
			_logger.debug("vLastUtrnno: " + vLastUtrnno);
			_logger.debug("vDateStart: " + vDateStart.toString());
			_logger.debug("vDateFinish: " + vDateFinish.toString());
			vTransList = api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish);
			if (vTransList != null) {
				_logger.debug("Received " + vTransList.size() + " transactions");
			} else {
				_logger.debug("Method api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) returned null");
			}
			_logger.debug("Method api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) executed");

			if (vTransList != null && vTransList.size() > 0) {
				for (IntegrationTransaction vTrans : vTransList) {
					mapper.loadAtmTrans_insertIntgrParams(vTrans.getTerminalId(), vTrans.getOperationId(),
							JdbcUtils.getSqlTimestamp(vTrans.getDatetime()), vTrans.getOpertype(),
							vTrans.getNoteRetracted(), vTrans.getNoteRejected(), vTrans.getBillCass1(),
							vTrans.getDenomCass1(), vTrans.getCurrencyCass1(), vTrans.getTypeCass1(),
							vTrans.getBillCass2(), vTrans.getDenomCass2(), vTrans.getCurrencyCass2(),
							vTrans.getTypeCass2(), vTrans.getBillCass3(), vTrans.getDenomCass3(),
							vTrans.getCurrencyCass3(), vTrans.getTypeCass3(), vTrans.getBillCass4(),
							vTrans.getDenomCass4(), vTrans.getCurrencyCass4(), vTrans.getTypeCass4());
				}

				vTransList.clear();

				ObjectPair<Long, Long> minMaxUntrnno = mapper.loadAtmTrans_getIntgrLastUtrnno();
				if (minMaxUntrnno != null) {
					vLastUtrnno = minMaxUntrnno.getKey();
					vLatestUtrnno = minMaxUntrnno.getValue();
				} else {
					vLastUtrnno = 0L;
					vLatestUtrnno = 0L;
				}

				while (vCashInTransCount > 0) {
					_logger.debug("Executing api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) with parameters:");
					_logger.debug("vLastUtrnno: " + vLastUtrnno);
					_logger.debug("vLatestUtrnno: " + vLatestUtrnno);
					vTransListCashIn = api.getAtmTransCashIn(vLastUtrnno, vLatestUtrnno);
					if (vTransListCashIn != null) {
						_logger.debug("Received " + vTransListCashIn.size() + " transactions");
					} else {
						_logger.debug("Method api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) returned null");
					}
					_logger.debug("Method api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) executed");

					if (vTransListCashIn != null) {
						vCashInTransCount = vTransListCashIn.size();

						if (vCashInTransCount > 0) {
							vLastUtrnno = vTransListCashIn.get(vTransListCashIn.size() - 1).getOperId();
							for (IntegrationCashInTransaction vTransCashIn : vTransListCashIn) {
								ObjectPair<Integer, Timestamp> atmIdDt = ORMUtils.getSingleValue(
										mapper.loadAtmTrans_getIntgrTransAtmIdDt(vTransCashIn.getOperId()));
								if (atmIdDt != null) {
									tempAtmId = atmIdDt.getKey();
									tempDateTime = JdbcUtils.getTimestamp(atmIdDt.getValue());
								} else {
									tempAtmId = null;
									tempDateTime = null;
								}

								if (tempAtmId != null && tempDateTime != null) {
									mapper.loadAtmTrans_insertIntgrCashInTrans(tempAtmId, vTransCashIn.getOperId(),
											JdbcUtils.getSqlTimestamp(tempDateTime), vTransCashIn.getOperType(),
											vTransCashIn.getBillDenom(), vTransCashIn.getBillCurr(),
											vTransCashIn.getBillNum());
								}
							}
						} else {
							vCashInTransCount = 0;
						}
						vTransListCashIn.clear();
					} else {
						vCashInTransCount = 0;
					}
				}
			}
			vTransListCashIn.clear();
		} finally {
			session.close();
		}
		clearExcessIntgrTrans(sessionHolder, api);
		_logger.debug("method load_atm_trans executed");
	}

	public static void clearExcessIntgrTrans(ISessionHolder sessionHolder, ICashManagementAPI api) throws SQLException {//
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).clearExcessIntgrTrans();
		} finally {
			session.close();
		}
	}

	public static void load_atm_trans(Connection connection, ICashManagementWSAPI restEjb) {
		restEjb.loadAtmTrans();
	}

	public static void load_atm_trans(ISessionHolder sessionHolder, ICashManagementAPI api, Date dateFrom,
			List<Integer> atmList, Long lastUtrnno) throws SQLException {
		_logger.debug("Executing method load_atm_trans");
		List<IntegrationTransaction> vTransList = new ArrayList<IntegrationTransaction>();
		List<IntegrationCashInTransaction> vTransListCashIn = new ArrayList<IntegrationCashInTransaction>();
		List<Integer> vAtmList = new ArrayList<Integer>();
		Long vLastUtrnno;
		Long vLatestUtrnno;
		int vCashInTransCount = 1;
		Date sysDate = new Date();
		Date vDateStart = DateUtils.truncate(sysDate, Calendar.MONTH);
		Date vDateFinish = sysDate;

		Integer tempAtmId = null;
		Date tempDateTime = null;

		String selectIntgrParamsSQL = "SELECT cass_check_datetime as result " + "FROM t_cm_intgr_params";

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());
			List<Timestamp> checkDatetimes = mapper.getTimestampValueByQuery(selectIntgrParamsSQL);

			if (!checkDatetimes.isEmpty()) {
				vLastUtrnno = lastUtrnno;
				vDateStart = dateFrom;
				vDateFinish = JdbcUtils.getTimestamp(ORMUtils.getSingleValue(checkDatetimes));
			} else {
				vLastUtrnno = lastUtrnno;// 0L
				vDateStart = dateFrom;// DateUtils.truncate(sysDate,
										// Calendar.MONTH);
				vDateFinish = DateUtils.addMinutes(sysDate, -20);
			}

			vDateFinish = DateUtils.addMinutes(sysDate, -20).compareTo(vDateFinish) < 0
					? DateUtils.addMinutes(sysDate, -20) : vDateFinish;

			vAtmList = atmList;

			_logger.debug("Executing api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) with parameters:");
			_logger.debug("vAtmList: " + Arrays.toString(vAtmList.toArray()));
			_logger.debug("vLastUtrnno: " + vLastUtrnno);
			_logger.debug("vDateStart: " + vDateStart.toString());
			_logger.debug("vDateFinish: " + vDateFinish.toString());
			vTransList = api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish);
			if (vTransList != null) {
				_logger.debug("Received " + vTransList.size() + " transactions");
			} else {
				_logger.debug("Method api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) returned null");
			}
			_logger.debug("Method api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish) executed");

			if (vTransList != null && vTransList.size() > 0) {
				for (IntegrationTransaction vTrans : vTransList) {
					mapper.loadAtmTrans_insertIntgrParams(vTrans.getTerminalId(), vTrans.getOperationId(),
							JdbcUtils.getSqlTimestamp(vTrans.getDatetime()), vTrans.getOpertype(),
							vTrans.getNoteRetracted(), vTrans.getNoteRejected(), vTrans.getBillCass1(),
							vTrans.getDenomCass1(), vTrans.getCurrencyCass1(), vTrans.getTypeCass1(),
							vTrans.getBillCass2(), vTrans.getDenomCass2(), vTrans.getCurrencyCass2(),
							vTrans.getTypeCass2(), vTrans.getBillCass3(), vTrans.getDenomCass3(),
							vTrans.getCurrencyCass3(), vTrans.getTypeCass3(), vTrans.getBillCass4(),
							vTrans.getDenomCass4(), vTrans.getCurrencyCass4(), vTrans.getTypeCass4());
				}

				vTransList.clear();

				ObjectPair<Long, Long> minMaxUntrnno = ORMUtils
						.getSingleValue(mapper.loadAtmTrans_getIntgrLastUtrnnoComplex(vAtmList));
				if (minMaxUntrnno != null) {
					vLastUtrnno = minMaxUntrnno.getKey();
					vLatestUtrnno = minMaxUntrnno.getValue();
				} else {
					vLastUtrnno = 0L;
					vLatestUtrnno = 0L;
				}

				while (vCashInTransCount > 0) {
					_logger.debug("Executing api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) with parameters:");
					_logger.debug("vLastUtrnno: " + vLastUtrnno);
					_logger.debug("vLatestUtrnno: " + vLatestUtrnno);
					vTransListCashIn = api.getAtmTransCashIn(vLastUtrnno, vLatestUtrnno);
					if (vTransListCashIn != null) {
						_logger.debug("Received " + vTransListCashIn.size() + " transactions");
					} else {
						_logger.debug("Method api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) returned null");
					}
					_logger.debug("Method api.getAtmTransCashIn(vLastUtrnno,vLatestUtrnno) executed");

					if (vTransListCashIn != null) {
						vCashInTransCount = vTransListCashIn.size();

						if (vCashInTransCount > 0) {
							vLastUtrnno = vTransListCashIn.get(vTransListCashIn.size() - 1).getOperId();
							for (IntegrationCashInTransaction vTransCashIn : vTransListCashIn) {
								ObjectPair<Integer, Timestamp> atmIdDt = ORMUtils.getSingleValue(
										mapper.loadAtmTrans_getIntgrTransAtmIdDt(vTransCashIn.getOperId()));
								if (atmIdDt != null) {
									tempAtmId = atmIdDt.getKey();
									tempDateTime = atmIdDt.getValue();
								} else {
									tempAtmId = null;
									tempDateTime = null;
								}

								if (tempAtmId != null && tempDateTime != null) {
									mapper.loadAtmTrans_insertIntgrCashInTrans(tempAtmId, vTransCashIn.getOperId(),
											JdbcUtils.getSqlTimestamp(tempDateTime), vTransCashIn.getOperType(),
											vTransCashIn.getBillDenom(), vTransCashIn.getBillCurr(),
											vTransCashIn.getBillNum());
								}
							}
						} else {
							vCashInTransCount = 0;
						}
						vTransListCashIn.clear();
					} else {
						vCashInTransCount = 0;
					}
				}
			}
			vTransListCashIn.clear();
		} finally {
			session.close();
		}

		_logger.debug("method load_atm_trans executed");
	}

	public static void load_atm_trans(ISessionHolder sessionHolder, ICashManagementWSAPI restEjb, Date dateFrom,
			List<Integer> atmList, Long lastUtrnno) throws SQLException {
		Long vLastUtrnno;
		Date sysDate = new Date();
		Date vDateFinish = sysDate;
		String selectIntgrParamsSQL = "SELECT cass_check_datetime as result " + "FROM t_cm_intgr_params";

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());
			Timestamp checkTimestamp = ORMUtils.getSingleValue(mapper.getTimestampValueByQuery(selectIntgrParamsSQL));
			if (checkTimestamp != null) {
				vLastUtrnno = lastUtrnno;
				vDateFinish = JdbcUtils.getTimestamp(checkTimestamp);
			} else {
				vLastUtrnno = lastUtrnno;// 0L
				vDateFinish = DateUtils.addMinutes(sysDate, -20);
			}
		} finally {
			session.close();
		}

		vDateFinish = DateUtils.addMinutes(sysDate, -20).compareTo(vDateFinish) < 0 ? DateUtils.addMinutes(sysDate, -20)
				: vDateFinish;
		restEjb.loadAtmTrans(atmList, lastUtrnno != null ? lastUtrnno : vLastUtrnno, dateFrom, vDateFinish);
	}

	public static void load_atm_trans(ISessionHolder sessionHolder, ICashManagementAPI api, Date dateStart,
			Date dateEnd) throws SQLException {
		List<IntegrationTransaction> vTransList = new ArrayList<IntegrationTransaction>();
		List<IntegrationCashInTransaction> vTransListCashIn = new ArrayList<IntegrationCashInTransaction>();
		List<Integer> vAtmList = new ArrayList<Integer>();
		Long vLastUtrnno;
		Long vLatestUtrnno;
		int vCashInTransCount = 1;
		Date sysDate = new Date();
		Date vDateStart = dateStart;
		Date vDateFinish = dateEnd;
		// Date vDateStart = DateUtils.truncate(sysDate, Calendar.MONTH);
		// Date vDateFinish = sysDate;

		Integer tempAtmId = null;
		Date tempDateTime = null;

		String selectAtmsSQL = "SELECT ATM_ID as result from T_CM_ATM";// EXTERNAL_ATM_ID

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());
			TripleObject<Long, Timestamp, Timestamp> params = ORMUtils
					.getSingleValue(mapper.loadAtmTrans_getIntgrParams());
			if (params != null) {
				vLastUtrnno = params.getFirst();
				vDateStart = params.getSecond();
				vDateFinish = params.getThird();
			} else {
				vLastUtrnno = null;
				vDateStart = dateStart;
				// vDateStart = DateUtils.truncate(sysDate, Calendar.MONTH);
				// vDateFinish =DateUtils.addMinutes(sysDate, -20);
			}

			vDateFinish = DateUtils.addMinutes(sysDate, -20).compareTo(vDateFinish) < 0
					? DateUtils.addMinutes(sysDate, -20) : vDateFinish;

			vAtmList.addAll(mapper.getIntegerValueByQuery(selectAtmsSQL));
			vTransList = api.getAtmTrans(vAtmList, vLastUtrnno, vDateStart, vDateFinish);

			if (vTransList != null && vTransList.size() > 0) {
				for (IntegrationTransaction vTrans : vTransList) {
					mapper.loadAtmTrans_insertIntgrParams(vTrans.getTerminalId(), vTrans.getOperationId(),
							JdbcUtils.getSqlTimestamp(vTrans.getDatetime()), vTrans.getOpertype(),
							vTrans.getNoteRetracted(), vTrans.getNoteRejected(), vTrans.getBillCass1(),
							vTrans.getDenomCass1(), vTrans.getCurrencyCass1(), vTrans.getTypeCass1(),
							vTrans.getBillCass2(), vTrans.getDenomCass2(), vTrans.getCurrencyCass2(),
							vTrans.getTypeCass2(), vTrans.getBillCass3(), vTrans.getDenomCass3(),
							vTrans.getCurrencyCass3(), vTrans.getTypeCass3(), vTrans.getBillCass4(),
							vTrans.getDenomCass4(), vTrans.getCurrencyCass4(), vTrans.getTypeCass4());
				}

				vTransList.clear();

				ObjectPair<Long, Long> minMaxUntrnno = mapper.loadAtmTrans_getIntgrLastUtrnno();
				if (minMaxUntrnno != null) {
					vLastUtrnno = minMaxUntrnno.getKey();
					vLatestUtrnno = minMaxUntrnno.getValue();
				} else {
					vLastUtrnno = 0L;
					vLatestUtrnno = 0L;
				}

				while (vCashInTransCount > 0) {
					vTransListCashIn = api.getAtmTransCashIn(vLastUtrnno, vLatestUtrnno);
					// vTransListCashIn = api.getAtmTransCashIn(vDateStart,
					// vDateFinish); //questionable
					if (vTransListCashIn != null) {
						vCashInTransCount = vTransListCashIn.size();

						if (vCashInTransCount > 0) {
							vLastUtrnno = vTransListCashIn.get(vTransListCashIn.size() - 1).getOperId();
							for (IntegrationCashInTransaction vTransCashIn : vTransListCashIn) {
								ObjectPair<Integer, Timestamp> atmIdDt = ORMUtils.getSingleValue(
										mapper.loadAtmTrans_getIntgrTransAtmIdDt(vTransCashIn.getOperId()));
								if (atmIdDt != null) {
									tempAtmId = atmIdDt.getKey();
									tempDateTime = atmIdDt.getValue();
								} else {
									tempAtmId = null;
									tempDateTime = null;
								}

								if (tempAtmId != null && tempDateTime != null) {
									mapper.loadAtmTrans_insertIntgrCashInTrans(tempAtmId, vTransCashIn.getOperId(),
											JdbcUtils.getSqlTimestamp(tempDateTime), vTransCashIn.getOperType(),
											vTransCashIn.getBillDenom(), vTransCashIn.getBillCurr(),
											vTransCashIn.getBillNum());
								}
							}
						} else {
							vCashInTransCount = 0;
						}
						vTransListCashIn.clear();
					} else {
						vCashInTransCount = 0;
					}
				}
			}
			vTransListCashIn.clear();
		} finally {
			session.close();
		}

	}

	public static void load_atm_downtime(AtomicInteger interruptFlag, ISessionHolder sessionHolder,
			ICashManagementAPI api) throws SQLException {
		_logger.debug("Executing load_atm_downtime method");
		List<IntegrationDowntime> vDownTimeList = new ArrayList<IntegrationDowntime>();
		List<Integer> vAtmList = new ArrayList<Integer>();
		Date vLastDowntimeDatetime;
		Date vMinStatDate = null;

		String selectMinStatDateSQL = "SELECT MIN(stat_date) as result FROM t_cm_cashout_curr_stat";
		String selectAtmsSQL = "SELECT ATM_ID as result from T_CM_ATM";

		SqlSession session = sessionHolder.getSession(getMapperClass());

		if (!(interruptFlag.get() > 0)) {
			try {
				IntegrationMapper mapper = session.getMapper(getMapperClass());

				Timestamp minStatDAte = ORMUtils.getSingleValue(mapper.getTimestampValueByQuery(selectMinStatDateSQL));
				if (minStatDAte != null) {
					vMinStatDate = JdbcUtils.getTimestamp(minStatDAte);
				}

				Timestamp lastDownTime = ORMUtils.getSingleValue(
						mapper.loadAtmDowntime_getLastDownTime(JdbcUtils.getSqlTimestamp(vMinStatDate)));
				if (lastDownTime != null) {
					vLastDowntimeDatetime = JdbcUtils.getTimestamp(lastDownTime);
				} else {
					vLastDowntimeDatetime = vMinStatDate;
				}

				vAtmList.addAll(mapper.getIntegerValueByQuery(selectAtmsSQL));

				_logger.debug("Executing api.getAtmDowntime(vAtmList,dateFrom, dateTo) with parameters:");
				_logger.debug("vAtmList: " + Arrays.toString(vAtmList.toArray()));
				_logger.debug(
						"dateFrom: " + (vLastDowntimeDatetime != null ? vLastDowntimeDatetime.toString() : "null"));
				Date dateTo = new Date();
				_logger.debug("dateTo: " + dateTo.toString());
				vDownTimeList = api.getAtmDowntime(vAtmList, vLastDowntimeDatetime, dateTo);
				if (vDownTimeList != null) {
					_logger.debug("Received " + vDownTimeList.size() + " transactions");
				} else {
					_logger.debug("Method api.getAtmDowntime(vAtmList,dateFrom, dateTo) returned null");
				}
				_logger.debug("Method api.getAtmDowntime(vAtmList,dateFrom, dateTo) executed");

				if (!(interruptFlag.get() > 0)) {
					if (vDownTimeList != null && vDownTimeList.size() > 0) {
						for (IntegrationDowntime vDownTime : vDownTimeList) {
							mapper.loadAtmDowntime_insertIntgrDowntimePeriod(vDownTime.getTerminalId(),
									JdbcUtils.getSqlTimestamp(vDownTime.getDateFrom()),
									JdbcUtils.getSqlTimestamp(vDownTime.getDateTo()), vDownTime.getDownTimeType());
						}
					}
				}
			} finally {
				session.close();
			}
		}
		_logger.debug("method load_atm_downtime executed");
	}

	public static void load_atm_downtime(Connection connection, ICashManagementWSAPI restEjb) {
		restEjb.loadAtmDowntime();

	}

	public static void load_atm_downtime(ISessionHolder sessionHolder, ICashManagementAPI api, List<Integer> atmList)
			throws SQLException {
		_logger.debug("Executing load_atm_downtime method for atms: " + atmList.toString());
		List<IntegrationDowntime> vDownTimeList = new ArrayList<IntegrationDowntime>();
		List<Integer> vAtmList;
		Date vLastDowntimeDatetime;
		Date vMinStatDate = null;

		String selectMinStatDateSQL = "SELECT MIN(stat_date) as result FROM t_cm_cashout_curr_stat";

		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			Timestamp minStatDAte = ORMUtils.getSingleValue(mapper.getTimestampValueByQuery(selectMinStatDateSQL));
			if (minStatDAte != null) {
				vMinStatDate = JdbcUtils.getTimestamp(minStatDAte);
			}

			Timestamp lastDownTime = ORMUtils
					.getSingleValue(mapper.loadAtmDowntime_getLastDownTime(JdbcUtils.getSqlTimestamp(vMinStatDate)));
			if (lastDownTime != null) {
				vLastDowntimeDatetime = JdbcUtils.getTimestamp(lastDownTime);
			} else {
				vLastDowntimeDatetime = vMinStatDate;
			}

			vAtmList = atmList;

			_logger.debug("Executing api.getAtmDowntime(vAtmList,dateFrom, dateTo) with parameters:");
			_logger.debug("vAtmList: " + Arrays.toString(vAtmList.toArray()));
			_logger.debug("dateFrom: " + (vLastDowntimeDatetime != null ? vLastDowntimeDatetime.toString() : "null"));
			Date dateTo = new Date();
			_logger.debug("dateTo: " + dateTo.toString());
			vDownTimeList = api.getAtmDowntime(vAtmList, vLastDowntimeDatetime, dateTo);
			if (vDownTimeList != null) {
				_logger.debug("Received " + vDownTimeList.size() + " transactions");
			} else {
				_logger.debug("Method api.getAtmDowntime(vAtmList,dateFrom, dateTo) returned null");
			}
			_logger.debug("Method api.getAtmDowntime(vAtmList,dateFrom, dateTo) executed");

			if (vDownTimeList != null && vDownTimeList.size() > 0) {
				for (IntegrationDowntime vDownTime : vDownTimeList) {
					mapper.loadAtmDowntime_insertIntgrDowntimePeriod(vDownTime.getTerminalId(),
							JdbcUtils.getSqlTimestamp(vDownTime.getDateFrom()),
							JdbcUtils.getSqlTimestamp(vDownTime.getDateTo()), vDownTime.getDownTimeType());
				}
			}
		} finally {
			session.close();
		}

		_logger.debug("method load_atm_downtime executed for atms: " + atmList.toString());
	}

	public static void load_atm_downtime(Connection connection, ICashManagementWSAPI restEjb, List<Integer> atmList) {
		restEjb.loadAtmDowntime(atmList, null, null);// check!!!!
	}

	public static void load_atm_downtime(ISessionHolder sessionHolder, ICashManagementAPI api, Date dateStart,
			Date dateEnd) throws SQLException {
		List<IntegrationDowntime> vDownTimeList = new ArrayList<IntegrationDowntime>();
		List<Integer> vAtmList = new ArrayList<Integer>();
		Date vLastDowntimeDatetime;
		Date vMinStatDate = null;

		Date sysDate = new Date();

		String selectMinStatDateSQL = "SELECT MIN(stat_date) as result " + "FROM t_cm_cashout_curr_stat";
		String selectAtmsSQL = "SELECT ATM_ID from T_CM_ATM";

		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			Timestamp minStatDAte = ORMUtils.getSingleValue(mapper.getTimestampValueByQuery(selectMinStatDateSQL));
			if (minStatDAte != null) {
				vMinStatDate = JdbcUtils.getTimestamp(minStatDAte);
			}

			Timestamp lastDownTime = ORMUtils
					.getSingleValue(mapper.loadAtmDowntime_getLastDownTime(JdbcUtils.getSqlTimestamp(vMinStatDate)));
			if (lastDownTime != null) {
				vLastDowntimeDatetime = JdbcUtils.getTimestamp(lastDownTime);
			} else {
				vLastDowntimeDatetime = vMinStatDate;
			}

			vAtmList.addAll(mapper.getIntegerValueByQuery(selectAtmsSQL));

			if (vLastDowntimeDatetime != null) {
				vDownTimeList = api.getAtmDowntime(vAtmList, vLastDowntimeDatetime,
						sysDate.compareTo(dateEnd) < 0 ? sysDate : dateEnd);
			} else {
				vDownTimeList = api.getAtmDowntime(vAtmList, dateStart,
						sysDate.compareTo(dateEnd) < 0 ? sysDate : dateEnd);
			}

			if (vDownTimeList != null && vDownTimeList.size() > 0) {
				for (IntegrationDowntime vDownTime : vDownTimeList) {
					mapper.loadAtmDowntime_insertIntgrDowntimePeriod(vDownTime.getTerminalId(),
							JdbcUtils.getSqlTimestamp(vDownTime.getDateFrom()),
							JdbcUtils.getSqlTimestamp(vDownTime.getDateTo()), vDownTime.getDownTimeType());
				}
			}

		} finally {
			session.close();
		}
	}

	public static void save_params(ISessionHolder sessionHolder) throws SQLException {
		_logger.debug("Executing save_params method");
		int vLastUtrnno = 0;
		int vCheck = 0;
		Date vLastDowntimeDatetime = null;
		Date vLastTransDatetime = null;

		String selectLastDownTimeDatetimeSQL = "SELECT MAX(end_date) as result " + "FROM t_cm_intgr_downtime_period";
		String checkSQL = "SELECT count(1) as result from t_cm_intgr_params";

		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			ObjectPair<Integer, Timestamp> pair = mapper.saveParams_getLastUtrnnoAndDatetime();
			if (pair != null) {
				vLastUtrnno = pair.getKey();
				vLastTransDatetime = pair.getValue();
			}

			Timestamp lastDowntime = ORMUtils
					.getSingleValue(mapper.getTimestampValueByQuery(selectLastDownTimeDatetimeSQL));
			if (lastDowntime != null) {
				vLastDowntimeDatetime = JdbcUtils.getTimestamp(lastDowntime);
			}

			if (vLastUtrnno > 0) {
				Integer check = ORMUtils.getSingleValue(mapper.getIntegerValueByQuery(checkSQL));
				vCheck = ORMUtils.getNotNullValue(check, 0);

				if (vCheck == 0) {
					_logger.debug("Saving integration parameters: ");
					_logger.debug("vLastUtrnno: " + vLastUtrnno);
					_logger.debug("vLastDowntimeDatetime: " + vLastDowntimeDatetime.toString());
					_logger.debug("vLastTransDatetime: " + vLastTransDatetime.toString());
					mapper.saveParams_insert(vLastUtrnno, JdbcUtils.getSqlTimestamp(vLastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(vLastTransDatetime));
				} else {
					_logger.debug("Updating integration parameters: ");
					_logger.debug("vLastUtrnno: " + vLastUtrnno);
					_logger.debug("vLastDowntimeDatetime: " + vLastDowntimeDatetime.toString());
					_logger.debug("vLastTransDatetime: " + vLastTransDatetime.toString());
					mapper.saveParams_update(vLastUtrnno, JdbcUtils.getSqlTimestamp(vLastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(vLastTransDatetime));
				}
			}
		} finally {
			session.close();
		}
		_logger.debug("Method save_params executed");
	}

	public static void save_params_multi_disp(ISessionHolder sessionHolder) throws SQLException {
		int vLastUtrnno = 0;
		int vCheck = 0;
		Date vLastDowntimeDatetime = null;
		Date vLastTransDatetime = null;

		String selectLastDownTimeDatetimeSQL = "SELECT MAX(end_date) as result " + "FROM t_cm_intgr_downtime_period";
		String checkSQL = "SELECT count(1) as result from t_cm_intgr_params";

		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			ObjectPair<Integer, Timestamp> pair = mapper.saveParamsMultiDisp_getLastUtrnnoAndDatetime();
			if (pair != null) {
				vLastUtrnno = pair.getKey();
				vLastTransDatetime = JdbcUtils.getTimestamp(pair.getValue());
			}

			Timestamp lastDowntime = ORMUtils
					.getSingleValue(mapper.getTimestampValueByQuery(selectLastDownTimeDatetimeSQL));
			if (lastDowntime != null) {
				vLastDowntimeDatetime = JdbcUtils.getTimestamp(lastDowntime);
			}

			if (vLastUtrnno > 0) {
				vCheck = ORMUtils.getNotNullValue(ORMUtils.getSingleValue(mapper.getIntegerValueByQuery(checkSQL)), 0);

				if (vCheck == 0) {
					mapper.saveParams_insert(vLastUtrnno, JdbcUtils.getSqlTimestamp(vLastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(vLastTransDatetime));
				} else {
					mapper.saveParams_update(vLastUtrnno, JdbcUtils.getSqlTimestamp(vLastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(vLastTransDatetime));
				}
			}
		} finally {
			session.close();
		}
	}

	public static void load_inst_list(ISessionHolder sessionHolder, ICashManagementAPI api) throws SQLException {
		int vCheck = 0;
		List<IntegrationInst> vInstList = new ArrayList<IntegrationInst>();

		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());
			vInstList = api.getInstList();

			if (vInstList != null && vInstList.size() > 0) {
				int recordsCount = 0;
				for (IntegrationInst vInst : vInstList) {
					vCheck = ORMUtils.getNotNullValue(mapper.loadInstList_selectCount(vInst.getInstId()), 0);
					if (vCheck == 0) {
						if (!(recordsCount > 0 && recordsCount < BATCH_MAX_SIZE)) {
							mapper.flush();
							recordsCount = 0;
						}
						mapper.loadInstList_insert(vInst.getInstId(), vInst.getDescription());
					}
				}
				mapper.flush();
			}
		} finally {
			session.close();
		}
	}

	public static void load_inst_list(Connection connection, ICashManagementWSAPI restEjb) {
		restEjb.loadInstList();
	}

	public static void load_atm_list(ISessionHolder sessionHolder, ICashManagementAPI api) throws SQLException {
		int vCheck = 0;
		List<IntegrationAtm> vAtmList = new ArrayList<IntegrationAtm>();

		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			vAtmList = api.getAtmList();

			if (vAtmList != null && vAtmList.size() > 0) {
				int recordsCount = 0;

				for (IntegrationAtm vAtm : vAtmList) {
					vCheck = ORMUtils.getNotNullValue(mapper.loadAtmList_selectCount(vAtm.getTerminalId()), 0);

					if (!(recordsCount > 0 && recordsCount < BATCH_MAX_SIZE)) {
						mapper.flush();
						recordsCount = 0;
					}
					if (vCheck == 0) {
						mapper.loadAtmList_insert(vAtm.getTerminalId(), vAtm.getState(), vAtm.getCity(),
								vAtm.getStreet(), vAtm.getInstId(), vAtm.getExternalId(), vAtm.getName());
						recordsCount++;
					} else {
						mapper.loadAtmList_update(vAtm.getTerminalId(), vAtm.getState(), vAtm.getCity(),
								vAtm.getStreet(), vAtm.getInstId(), vAtm.getExternalId(), vAtm.getName());
						recordsCount++;
					}
				}
				mapper.flush();
			}
		} finally {
			session.close();
		}
	}

	public static void load_atm_list(Connection connection, ICashManagementWSAPI restEjb) {
		restEjb.loadAtmList();
	}

	public static void load_currency_convert_rates(ISessionHolder sessionHolder, ICashManagementAPI api)
			throws SQLException {
		List<IntegrationCurrencyRate> vCnvtRateList = new ArrayList<IntegrationCurrencyRate>();

		vCnvtRateList = api.getCurrencyConvertRates();

		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			if (vCnvtRateList != null && vCnvtRateList.size() > 0) {
				for (IntegrationCurrencyRate vCnvtRate : vCnvtRateList) {
					if (vCnvtRate.getCnvtDate().after(DateUtils.addDays(new Date(), -7))) {
						mapper.loadCurrencyConvertRates_insert(JdbcUtils.getSqlTimestamp(vCnvtRate.getCnvtDate()),
								Integer.valueOf(vCnvtRate.getSrcCurrencyCode()),
								Integer.valueOf(vCnvtRate.getDestCurrencyCode()), new Float(vCnvtRate.getRate()),
								vCnvtRate.getMultipleFlag(), vCnvtRate.getSrcInstId(), vCnvtRate.getDestInstId());
					}
				}
			}
			mapper.loadCurrencyConvertRates_delete();
		} finally {
			session.close();
		}
	}

	public static void load_currency_convert_rates(Connection connection, ICashManagementWSAPI restEjb) {
		restEjb.loadCurrencyConvertRates();
	}

	public static void load_atm_cassettes_balances(ISessionHolder sessionHolder, ICashManagementAPI api)
			throws SQLException {
		int vCheck = 0;
		Date vDateLoad = new Date();
		List<IntegrationCassBalance> vCassList = new ArrayList<IntegrationCassBalance>();
		List<Integer> vAtmList = new ArrayList<Integer>();
		String selectAtmsSQL = "SELECT ATM_ID as result from T_CM_ATM";
		StringBuilder deleteSQL = new StringBuilder("DELETE FROM T_CM_INTGR_CASS_BALANCE " + "WHERE 1=1 ");
		String checkSQL = "SELECT count(1) as result from t_cm_intgr_params";

		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			vAtmList.addAll(mapper.getIntegerValueByQuery(selectAtmsSQL));

			vDateLoad = DateUtils.addMinutes(DateUtils.truncate(new Date(), Calendar.MINUTE), -1);

			vCassList = api.getAtmCassettesBalances(vAtmList);

			int lastAtmIndex = 0;
			int originalSqlSize = deleteSQL.length();

			if (vCassList != null && vCassList.size() > 0) {
				// StringUtils.substringBeforeLast(deleteSQL.toString(),
				// separator)\
				// deleteSQL.replace(CmUtils.getIdListInClause(vAtmList.subList(0,
				// 999),"");
				while (lastAtmIndex < vAtmList.size()) {
					deleteSQL.append(CmUtils.getIdListInClause(
							vAtmList.subList(lastAtmIndex,
									lastAtmIndex + 999 > vAtmList.size() ? vAtmList.size() : lastAtmIndex + 999),
							"ATM_ID"));
					lastAtmIndex += 1000;
					session.delete(deleteSQL.toString());
					deleteSQL.delete(originalSqlSize + 1, deleteSQL.length());
				}

				for (IntegrationCassBalance vCass : vCassList) {
					mapper.loadAtmCassettesBalances_insertCassBalance(vCass.getTerminalId(), vCass.getCasstypeInd(),
							vCass.getCassNumber(), vCass.getCassBalance(), vCass.getBalanceStatus());
				}

				vCheck = ORMUtils.getSingleValue(mapper.getIntegerValueByQuery(checkSQL), 0);

				if (vCheck == 0) {
					mapper.loadAtmCassettesBalances_insertIntgrParams(JdbcUtils.getSqlTimestamp(vDateLoad));
				} else {
					mapper.loadAtmCassettesBalances_updateIntgrParams(JdbcUtils.getSqlTimestamp(vDateLoad));
				}
			}
		} finally {
			session.close();
		}
	}

	public static void load_atm_cassettes_balances(Connection connection, ICashManagementWSAPI restEjb) {
		restEjb.loadAtmCassettesBalances();
	}

	public static void load_atm_cassettes_balances(ISessionHolder sessionHolder, ICashManagementAPI api,
			List<Integer> atms) throws SQLException {
		int vCheck = 0;
		Date vDateLoad = new Date();
		List<IntegrationCassBalance> vCassList = new ArrayList<IntegrationCassBalance>();

		StringBuilder deleteSQL = new StringBuilder("DELETE FROM T_CM_INTGR_CASS_BALANCE " + "WHERE 1=1 ");
		String checkSQL = "SELECT count(1) as result from t_cm_intgr_params";

		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			vDateLoad = DateUtils.addMinutes(DateUtils.truncate(new Date(), Calendar.MINUTE), -1);

			vCassList = api.getAtmCassettesBalances(atms);

			int lastAtmIndex = 0;
			int originalSqlSize = deleteSQL.length();

			if (vCassList != null && vCassList.size() > 0) {
				// StringUtils.substringBeforeLast(deleteSQL.toString(),
				// separator)\
				// deleteSQL.replace(CmUtils.getIdListInClause(vAtmList.subList(0,
				// 999),"");
				while (lastAtmIndex < atms.size()) {
					deleteSQL.append(CmUtils.getIdListInClause(atms.subList(lastAtmIndex,
							lastAtmIndex + 999 > atms.size() ? atms.size() : lastAtmIndex + 999), "ATM_ID"));
					lastAtmIndex += 1000;
					session.delete(deleteSQL.toString());
					deleteSQL.delete(originalSqlSize + 1, deleteSQL.length());
				}

				for (IntegrationCassBalance vCass : vCassList) {
					mapper.loadAtmCassettesBalances_insertCassBalance(vCass.getTerminalId(), vCass.getCasstypeInd(),
							vCass.getCassNumber(), vCass.getCassBalance(), vCass.getBalanceStatus());
				}

				vCheck = ORMUtils.getSingleValue(mapper.getIntegerValueByQuery(checkSQL), 0);

				if (vCheck == 0) {
					mapper.loadAtmCassettesBalances_insertIntgrParams(JdbcUtils.getSqlTimestamp(vDateLoad));
				} else {
					mapper.loadAtmCassettesBalances_updateIntgrParams(JdbcUtils.getSqlTimestamp(vDateLoad));
				}
			}
		} finally {
			session.close();
		}
	}

	public static void load_atm_cassettes_balances(Connection connection, ICashManagementWSAPI restEjb,
			List<Integer> atms) {
		restEjb.loadAtmCassettesBalances(atms);
	}

	public static void load_atm_cassettes_statuses(AtomicInteger interruptFlag, ISessionHolder sessionHolder,
			ICashManagementAPI api) throws SQLException {
		_logger.debug("Executing load_atm_cassettes_statuses method");
		int vCheck = 0;
		List<IntegrationCassette> vCassList = new ArrayList<IntegrationCassette>();
		List<Integer> vAtmList = new ArrayList<Integer>();

		String selectAtmsSQL = "SELECT ATM_ID as result from T_CM_ATM";
		StringBuilder updateCassettesNotPresentSQL = new StringBuilder(
				"UPDATE T_CM_ATM_CASSETTES " + "SET CASS_IS_PRESENT = 0 " + "WHERE 1=1");

		SqlSession session = sessionHolder.getBatchSession(getMapperClass());

		if (!(interruptFlag.get() > 0)) {
			try {
				IntegrationMapper mapper = session.getMapper(getMapperClass());

				vAtmList.addAll(mapper.getIntegerValueByQuery(selectAtmsSQL));

				_logger.debug("Executing api.getAtmCassettesStatuses(vAtmList) with parameters:");
				_logger.debug("vAtmList: " + Arrays.toString(vAtmList.toArray()));
				vCassList = api.getAtmCassettesStatuses(vAtmList);
				if (vCassList != null) {
					_logger.debug("Received " + vCassList.size() + " cassettes");
				} else {
					_logger.debug("Method api.getAtmCassettesStatuses(vAtmList) returned null");
				}
				_logger.debug("Method api.getAtmCassettesStatuses(vAtmList) executed");

				int lastAtmIndex = 0;
				int originalSqlSize = updateCassettesNotPresentSQL.length();

				if (vCassList != null && vCassList.size() > 0) {
					while (lastAtmIndex < vAtmList.size()) {
						updateCassettesNotPresentSQL.append(CmUtils.getIdListInClause(
								vAtmList.subList(lastAtmIndex,
										lastAtmIndex + 999 > vAtmList.size() ? vAtmList.size() : lastAtmIndex + 999),
								"ATM_ID"));
						lastAtmIndex += 1000;
						session.update(updateCassettesNotPresentSQL.toString());
						updateCassettesNotPresentSQL.delete(originalSqlSize + 1, updateCassettesNotPresentSQL.length());
					}

					for (IntegrationCassette vCass : vCassList) {
						vCheck = ORMUtils.getNotNullValue(mapper
								.loadAtmCassettesStatuses_selectCount(vCass.getTerminalId(), vCass.getCassNumber()), 0);

						if (vCheck == 0) {
							mapper.loadAtmCassettesStatuses_insertCassetes(vCass.getTerminalId(), vCass.getCassType(),
									vCass.getCassNumber(), vCass.getCassState());
						} else {
							mapper.loadAtmCassettesStatuses_updateCassetes(vCass.getTerminalId(), vCass.getCassType(),
									vCass.getCassNumber(), vCass.getCassState());
						}
					}

					mapper.loadAtmCassettesStatuses_delete();
				}
			} finally {
				session.close();
			}
		}

		_logger.debug("Method load_atm_cassettes_statuses executed");
	}

	public static void load_atm_cassettes_statuses(Connection connection, ICashManagementWSAPI restEjb) {
		restEjb.loadAtmCassettesStatuses();

	}

	public static void load_atm_cassettes_statuses(ISessionHolder sessionHolder, ICashManagementAPI api,
			List<Integer> atmList) throws SQLException {
		_logger.debug("Executing load_atm_cassettes_statuses method");
		int vCheck = 0;
		List<IntegrationCassette> vCassList = new ArrayList<IntegrationCassette>();
		List<Integer> vAtmList = atmList;

		StringBuilder updateCassettesNotPresentSQL = new StringBuilder(
				"UPDATE T_CM_ATM_CASSETTES " + "SET CASS_IS_PRESENT = 0 " + "WHERE 1=1");

		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			IntegrationMapper mapper = session.getMapper(getMapperClass());

			_logger.debug("Executing api.getAtmCassettesStatuses(vAtmList) with parameters:");
			_logger.debug("vAtmList: " + Arrays.toString(vAtmList.toArray()));
			vCassList = api.getAtmCassettesStatuses(vAtmList);
			if (vCassList != null) {
				_logger.debug("Received " + vCassList.size() + " cassettes");
			} else {
				_logger.debug("Method api.getAtmCassettesStatuses(vAtmList) returned null");
			}
			_logger.debug("Method api.getAtmCassettesStatuses(vAtmList) executed");

			int lastAtmIndex = 0;
			int originalSqlSize = updateCassettesNotPresentSQL.length();

			if (vCassList != null && vCassList.size() > 0) {
				while (lastAtmIndex < vAtmList.size()) {
					updateCassettesNotPresentSQL.append(CmUtils.getIdListInClause(
							vAtmList.subList(lastAtmIndex,
									lastAtmIndex + 999 > vAtmList.size() ? vAtmList.size() : lastAtmIndex + 999),
							"ATM_ID"));
					lastAtmIndex += 1000;
					session.update(updateCassettesNotPresentSQL.toString());
					updateCassettesNotPresentSQL.delete(originalSqlSize + 1, updateCassettesNotPresentSQL.length());
				}

				for (IntegrationCassette vCass : vCassList) {
					vCheck = ORMUtils.getNotNullValue(
							mapper.loadAtmCassettesStatuses_selectCount(vCass.getTerminalId(), vCass.getCassNumber()),
							0);

					if (vCheck == 0) {
						mapper.loadAtmCassettesStatuses_insertCassetes(vCass.getTerminalId(), vCass.getCassType(),
								vCass.getCassNumber(), vCass.getCassState());
					} else {
						mapper.loadAtmCassettesStatuses_updateCassetes(vCass.getTerminalId(), vCass.getCassType(),
								vCass.getCassNumber(), vCass.getCassState());
					}
				}

				mapper.loadAtmCassettesStatuses_delete();
			}
		} finally {
			session.close();
		}

		_logger.debug("Method load_atm_cassettes_statuses executed");
	}

	public static void load_atm_cassettes_statuses(Connection connection, ICashManagementWSAPI restEjb,
			List<Integer> atms) {
		restEjb.loadAtmCassettesStatuses(atms);

	}
}
