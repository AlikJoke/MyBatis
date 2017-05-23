package ru.bpc.cm.integration;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.CmDbJobBean;
import ejbs.cm.svcm.CmWebTask;
import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.integration.orm.DataLoadMapper;
import ru.bpc.cm.items.integration.IAtm;
import ru.bpc.cm.items.integration.IAtmCassBalance;
import ru.bpc.cm.items.integration.IAtmCassStatus;
import ru.bpc.cm.items.integration.ICassette;
import ru.bpc.cm.items.integration.ICurrencyRate;
import ru.bpc.cm.items.integration.ICurrencyScale;
import ru.bpc.cm.items.integration.IDowntime;
import ru.bpc.cm.items.integration.IInst;
import ru.bpc.cm.items.integration.ITransaction;
import ru.bpc.cm.items.tasks.DateParamItem;
import ru.bpc.cm.items.tasks.LongParamItem;
import ru.bpc.cm.items.tasks.WebTaskParamItem;
import ru.bpc.cm.orm.common.ORMUtils;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class DataLoadController {

	private static final int BATCH_MAX_SIZE = 999;

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<DataLoadMapper> getMapperClass() {
		return DataLoadMapper.class;
	}

	public static void insertAtms(ISessionHolder sessionHolder, List<IAtm> atmList) throws SQLException {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());

		int recordsCount = 0;
		int check = 0;
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			for (IAtm filter : atmList) {
				Integer count = mapper.insertAtms_selectCount(filter.getTerminalId());
				check = ORMUtils.getNotNullValue(count, 0);

				if (recordsCount < BATCH_MAX_SIZE) {
					if (check == 0) {
						mapper.insertAtms_insert(filter.getTerminalId(), filter.getStreet(), filter.getCity(),
								filter.getState(), filter.getInstId(), String.valueOf(filter.getExternalId()),
								CmUtils.getNVLValue(filter.getName(), CmUtils.getAtmFullAdrress(filter.getState(),
										filter.getCity(), filter.getStreet())));
					} else {
						mapper.insertAtms_update(String.valueOf(filter.getTerminalId()), filter.getStreet(),
								filter.getCity(), filter.getState(), filter.getInstId(),
								String.valueOf(filter.getExternalId()), CmUtils.getNVLValue(filter.getName(), CmUtils
										.getAtmFullAdrress(filter.getState(), filter.getCity(), filter.getStreet())));
					}
					recordsCount++;
				} else {
					mapper.flush();
					recordsCount = 0;
				}
			}
			if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
				mapper.flush();
			}
		} finally {
			session.close();
		}
	}

	public static void insertInsts(ISessionHolder sessionHolder, List<IInst> instList) throws SQLException {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());

		int recordsCount = 0;
		int check = 0;
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			for (IInst filter : instList) {
				Integer count = mapper.insertInsts_selectCount(String.valueOf(filter.getInstId()));
				check = ORMUtils.getNotNullValue(count, 0);

				if (check == 0) {
					if (recordsCount < BATCH_MAX_SIZE) {
						mapper.insertInsts_insert(String.valueOf(filter.getInstId()), filter.getDescription());
						recordsCount++;
					} else {
						mapper.flush();
						recordsCount = 0;
					}
				} else {
					check = 0;
				}
			}
			if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
				mapper.flush();
			}
		} finally {
			session.close();
		}
	}

	public static void insertCurrencyCnvtRates(ISessionHolder sessionHolder, List<ICurrencyRate> currCnvtRateLsit)
			throws SQLException {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		int recordsCount = 0;
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			for (ICurrencyRate filter : currCnvtRateLsit) {
				if (recordsCount < BATCH_MAX_SIZE) {
					ICurrencyScale src = filter.getSrcCurrency();
					ICurrencyScale dst = filter.getDstCurrency();
					double rate = dst.getScale() * CmUtils.getNVLValue(src.getExponentScale(), Float.valueOf(1))
							/ CmUtils.getNVLValue(dst.getExponentScale(), Float.valueOf(1)) / src.getScale();

					mapper.insertCurrencyCnvtRates(rate, Integer.valueOf(src.getCurrency()),
							Integer.valueOf(dst.getCurrency()),
							new Timestamp(filter.getEffectiveDate().toGregorianCalendar().getTime().getTime()),
							CmUtils.getNVLValue(filter.getInverted(), 0) == 0 ? "M" : "D",
							String.valueOf(filter.getInstId()));
					recordsCount++;
				} else {
					mapper.flush();
					recordsCount = 0;
				}
			}
			if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
				mapper.flush();
			}
		} finally {
			session.close();
		}
	}

	public static void insertDowntimes(ISessionHolder sessionHolder, List<IDowntime> downtimeList) throws SQLException {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		int recordsCount = 0;
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			for (IDowntime filter : downtimeList) {
				if (recordsCount < BATCH_MAX_SIZE) {
					mapper.insertDowntimes(filter.getTerminalId(),
							new Timestamp(filter.getDateform().toGregorianCalendar().getTime().getTime()),
							new Timestamp(filter.getDateto().toGregorianCalendar().getTime().getTime()),
							filter.getDowntimeType());
					recordsCount++;
				} else {
					mapper.flush();
					recordsCount = 0;
				}
			}
			if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
				mapper.flush();
			}
		} finally {
			session.close();
		}
	}

	public static void insertTransactionsMultiDisp(ISessionHolder sessionHolder, List<ITransaction> transList)
			throws SQLException {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		int recordsCount = 0;
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			for (ITransaction trans : transList) {
				mapper.insertTransactionsMultiDisp_heads(trans.getTerminalId(), trans.getOperId(),
						new Timestamp(trans.getDatetime().toGregorianCalendar().getTime().getTime()),
						trans.getOperType(), CmUtils.getNVLValue(trans.getAmount(), Long.valueOf(0)),
						trans.getNoteRetracted(), trans.getNoteRejected());

				for (ICassette disp : trans.getCassettes().getCassette()) {
					mapper.insertTransactionsMultiDisp_disps(Integer.valueOf(trans.getOperId()), disp.getCassNumber(),
							disp.getCassType(), Integer.parseInt(CmUtils.getNVLValue(disp.getFace(), "0")),
							Integer.valueOf(CmUtils.getNVLValue(disp.getCurrency(), String.valueOf(0))),
							CmUtils.getNVLValue(disp.getNoteOper(), Integer.valueOf(0)),
							CmUtils.getNVLValue(disp.getNoteRemained(), Integer.valueOf(0)));
					recordsCount++;
				}

				if (recordsCount > BATCH_MAX_SIZE) {
					mapper.flush();
					recordsCount = 0;
				}
			}
			if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
				mapper.flush();
			}
		} finally {
			session.close();
		}
	}

	public static void insertAtmCassStatuses(ISessionHolder sessionHolder, List<IAtmCassStatus> atmCassStatList)
			throws SQLException {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());

		int recordsCount = 0;
		int check = 0;
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			for (IAtmCassStatus filter : atmCassStatList) {
				Integer count = mapper.insertAtmCassStatuses_selectCount(filter.getTerminalId(),
						filter.getCassTypeInd(), filter.getCassNumber());
				check = ORMUtils.getNotNullValue(count, 0);

				if (recordsCount < BATCH_MAX_SIZE) {
					if (check == 0) {
						mapper.insertAtmCassStatuses_insert(filter.getTerminalId(), filter.getCassTypeInd(),
								filter.getCassNumber(), filter.getCassStateInd());
					} else {
						mapper.insertAtmCassStatuses_update(filter.getTerminalId(), filter.getCassTypeInd(),
								filter.getCassNumber(), filter.getCassStateInd());
					}
					recordsCount++;
				} else {
					mapper.flush();
					recordsCount = 0;
				}
			}
			if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
				mapper.flush();
			}

			mapper.truncate("DELETE FROM T_CM_ATM_CASSETTES WHERE CASS_IS_PRESENT = 0");

		} finally {
			session.close();
		}
	}

	public static void insertAtmCassBalances(ISessionHolder sessionHolder, List<IAtmCassBalance> atmCassBalList)
			throws SQLException {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		Date vDateLoad = new Date();

		int recordsCount = 0;
		int check = 0;
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			mapper.truncate("DELETE FROM T_CM_INTGR_CASS_BALANCE");

			for (IAtmCassBalance filter : atmCassBalList) {

				if (recordsCount < BATCH_MAX_SIZE) {
					mapper.insertAtmCassStatuses_insertBalance(filter.getTerminalId(), filter.getCassTypeInd(),
							filter.getCassNumber(), filter.getCassNoteBalance(), filter.getBalanceStatus());
					recordsCount++;
				} else {
					mapper.flush();
					recordsCount = 0;
				}
			}
			if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
				mapper.flush();
			}

			Integer count = mapper.insertAtmCassBalances_selectCount();
			check = ORMUtils.getNotNullValue(count, 0);

			if (check == 0) {
				mapper.insertAtmCassBalances_insertParams(new Timestamp(vDateLoad.getTime()));
			} else {
				mapper.insertAtmCassBalances_update(new Timestamp(vDateLoad.getTime()));
			}
			mapper.flush();
		} finally {
			session.close();
		}
	}

	public static Date getDowntimeLastLoadedDate(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			List<Timestamp> results = mapper
					.getTimestampValues("SELECT last_downtime_datetime as result FROM t_cm_intgr_params");
			if (ORMUtils.getSingleValue(results) != null) {
				return ORMUtils.getSingleValue(results);
			} else {
				Timestamp result = mapper
						.getTimestampValue("SELECT MIN(stat_date) as result FROM t_cm_cashout_curr_stat");
				return result;
			}
		} finally {
			session.close();
		}
	}

	public static long getLastLoadedTransactionId(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			List<Long> result = mapper.getLongValues("SELECT last_utrnno as result FROM t_cm_intgr_params");
			return ORMUtils.getSingleValue(result, 0L);
		} finally {
			session.close();
		}
	}

	public static Date getCassCheckDatetime(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			List<Timestamp> results = mapper
					.getTimestampValues("SELECT cass_check_datetime as result FROM t_cm_intgr_params");
			Timestamp result = null;
			if ((result = ORMUtils.getSingleValue(results)) != null) {
				return CmUtils.getNVLValue(result, new Date());
			}
		} finally {
			session.close();
		}
		return new Date();
	}

	public static long getLastInsertedOperationIdMultiDisp(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			List<Long> results = mapper
					.getLongValues("SELECT COALESCE(max(OPER_ID),0) as last_utrnno FROM t_cm_intgr_trans_md");
			return ORMUtils.getSingleValue(results, 0L);
		} finally {
			session.close();
		}
	}

	public static void prepareDowntimes(AtomicInteger interruptFlag, ISessionHolder sessionHolder) throws SQLException {
		AggregationController.prepare_downtimes(interruptFlag, sessionHolder);
	}

	public static void truncateTrans(Connection connection) throws SQLException {
		java.sql.Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_trans"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_trans_md"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_downtime_period"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_downtime_cashout"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_downtime_cashin"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_trans_cash_in"));
			stmt.executeBatch();
		} catch (Exception e) {
			
		} finally {
			stmt.close();
		}
	}

	public static void truncateTrans(ISessionHolder sessionHolder, Date dateFrom, Date dateTo) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		java.sql.Statement stmt = null;
		try {
			stmt = session.getConnection().createStatement();
			truncateTransTable(sessionHolder, "t_cm_intgr_trans", "DATETIME", dateFrom, dateTo);
			truncateTransTable(sessionHolder, "t_cm_intgr_trans_md_disp", "DATETIME", dateFrom, dateTo);
			stmt.addBatch(ORMUtils.getTruncateTableUnrecoverable(session, "t_cm_intgr_trans_md_disp"));
			stmt.addBatch(ORMUtils.getTruncateTableUnrecoverable(session, "t_cm_intgr_trans_md"));
			stmt.addBatch(ORMUtils.getTruncateTableUnrecoverable(session, "t_cm_intgr_downtime_period"));
			truncateTransTable(sessionHolder, "t_cm_intgr_downtime_cashout", "STAT_DATE", dateFrom, dateTo);
			truncateTransTable(sessionHolder, "t_cm_intgr_downtime_cashin", "STAT_DATE", dateFrom, dateTo);
			// stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection,
			// "t_cm_intgr_downtime_cashout"));
			// stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection,
			// "t_cm_intgr_downtime_cashin"));
			truncateTransTable(sessionHolder, "t_cm_intgr_trans_cash_in", "DATETIME", dateFrom, dateTo);
			// stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection,
			// "t_cm_intgr_trans_cash_in"));
			stmt.executeBatch();
		} finally {
			stmt.close();
			session.close();
		}
	}

	private static void truncateTransTable(ISessionHolder sessionHolder, String tableName, String dateField,
			Date dateFrom, Date dateTo) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			mapper.truncateTrans(tableName, dateField, JdbcUtils.getSqlTimestamp(dateFrom),
					JdbcUtils.getSqlTimestamp(dateTo));
		} finally {
			session.close();
		}
	}

	@SuppressWarnings("unused")
	private static void truncateDowntimePeriodTrans(ISessionHolder sessionHolder, String tableName, String dateField,
			Date dateFrom, Date dateTo) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			mapper.truncateTrans(tableName, dateField, JdbcUtils.getSqlTimestamp(dateFrom),
					JdbcUtils.getSqlTimestamp(dateTo));
		} finally {
			session.close();
		}
	}

	public static void delete_old_stats(ISessionHolder sessionHolder, int days) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());
			List<ObjectPair<Integer, Integer>> pairs = mapper.deleteOldStats_selectMaxEncId(JdbcUtils
					.getSqlTimestamp(DateUtils.addHours(DateUtils.truncate(new Date(), Calendar.HOUR_OF_DAY), -days)));

			for (ObjectPair<Integer, Integer> pair : pairs) {
				mapper.deleteOldStats_delete("t_cm_cashout_curr_stat", pair.getKey(), pair.getValue());
				mapper.deleteOldStats_delete("t_cm_cashout_cass_stat", pair.getKey(), pair.getValue());
				mapper.deleteOldStats_deleteDetails(pair.getKey(), pair.getValue());
				mapper.deleteOldStats_delete("t_cm_enc_cashout_stat", pair.getKey(), pair.getValue());
			}

			List<ObjectPair<Integer, Integer>> pairsCashIn = mapper.deleteOldStats_selectMaxCashEncId(JdbcUtils
					.getSqlTimestamp(DateUtils.addHours(DateUtils.truncate(new Date(), Calendar.HOUR_OF_DAY), -days)));
			for (ObjectPair<Integer, Integer> pair : pairsCashIn) {
				mapper.deleteOldStats_delete("t_cm_cashin_stat", pair.getKey(), pair.getValue());
				mapper.deleteOldStats_delete("t_cm_cashin_denom_stat", pair.getKey(), pair.getValue());
				mapper.deleteOldStats_delete("t_cm_cashin_r_cass_stat", pair.getKey(), pair.getValue());
				mapper.deleteOldStats_delete("t_cm_cashin_r_curr_stat", pair.getKey(), pair.getValue());
				mapper.deleteOldStats_deleteDetailsCashIn(pair.getKey(), pair.getValue());
				mapper.deleteOldStats_delete("t_cm_enc_cashin_stat", pair.getKey(), pair.getValue());
			}
		} finally {
			session.close();
		}
	}

	public static void aggregateCashOutMultiDisp(DataSource dataSource, CmDbJobBean jobBean) throws SQLException {
		AggregationController.aggregate_cash_out_multi_disp(dataSource);
	}

	public static void aggregateCashInMultiDisp(DataSource dataSource) throws SQLException {
		AggregationController.aggregate_cash_in_multi_disp(dataSource);
	}

	public static void saveParams(AtomicInteger interruptFlag, ISessionHolder sessionHolder) throws SQLException {
		_logger.debug("Executing saveParams method");
		SqlSession session = sessionHolder.getSession(getMapperClass());

		String lastDowntimeSQL = "SELECT MAX(end_date) as result " + " FROM t_cm_intgr_downtime_period";
		String checkSQL = "SELECT count(1) as result from t_cm_intgr_params";

		if (!(interruptFlag.get() > 0)) {

		} else {
			try {
				DataLoadMapper mapper = session.getMapper(getMapperClass());

				int vCheck = 0;
				long lastUtrnno = 0;
				Date lastDowntimeDatetime = null;
				Date lastTransDatetime = null;

				ObjectPair<Long, Timestamp> result = ORMUtils.getSingleValue(mapper.saveParams_selectLastTransInfo());
				if (result != null) {
					lastUtrnno = result.getKey();
					lastTransDatetime = JdbcUtils.getTimestamp(result.getValue());
				}

				Timestamp downtime = mapper.getTimestampValue(lastDowntimeSQL);
				if (downtime != null) {
					lastDowntimeDatetime = JdbcUtils.getTimestamp(downtime);
				}

				if (lastUtrnno > 0) {
					Integer count = mapper.getIntegerValue(checkSQL);
					vCheck = ORMUtils.getNotNullValue(count, 0);

					if (vCheck == 0) {
						_logger.debug("Saving integration parameters: ");
						_logger.debug("lastUtrnno: " + lastUtrnno);
						_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime.toString());
						_logger.debug("lastTransDatetime: " + lastTransDatetime.toString());
						mapper.saveParams_insert(lastUtrnno, JdbcUtils.getSqlTimestamp(lastDowntimeDatetime),
								JdbcUtils.getSqlTimestamp(lastTransDatetime));
					} else {
						_logger.debug("Updating integration parameters: ");
						_logger.debug("lastUtrnno: " + lastUtrnno);
						if (lastDowntimeDatetime != null) {
							_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime.toString());
						} else {
							_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime);
						}

						if (lastTransDatetime != null) {
							_logger.debug("lastTransDatetime: " + lastTransDatetime.toString());
						} else {
							_logger.debug("lastTransDatetime: " + lastTransDatetime);
						}

						mapper.saveParams_update(lastUtrnno, JdbcUtils.getSqlTimestamp(lastDowntimeDatetime),
								JdbcUtils.getSqlTimestamp(lastTransDatetime));
					}
				}
			} finally {
				session.close();
			}
		}
		_logger.debug("Method saveParams executed");
	}

	public static void saveParamsForErrAtm(ISessionHolder sessionHolder) throws SQLException {
		_logger.debug("Executing saveParams method");
		SqlSession session = sessionHolder.getSession(getMapperClass());

		String lastDowntimeSQL = "SELECT MAX(end_date) as result " + " FROM t_cm_intgr_downtime_period";
		String checkSQL = "SELECT count(1) as result from t_cm_intgr_params";

		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			long lastParamUtrnno = 0;
			Date lastTransParamDatetime = null;
			int vCheck = 0;
			long lastUtrnno = 0;
			Date lastDowntimeDatetime = null;
			Date lastTransDatetime = null;

			ObjectPair<Long, Timestamp> result = ORMUtils.getSingleValue(mapper.saveParams_selectLastTransInfo());
			if (result != null) {
				lastUtrnno = result.getKey();
				lastTransDatetime = JdbcUtils.getTimestamp(result.getValue());
			}

			Timestamp downtime = mapper.getTimestampValue(lastDowntimeSQL);
			if (downtime != null) {
				lastDowntimeDatetime = JdbcUtils.getTimestamp(downtime);
			}

			if (lastUtrnno > 0) {
				Integer count = mapper.getIntegerValue(checkSQL);
				vCheck = ORMUtils.getNotNullValue(count, 0);

				if (vCheck == 0) {
					_logger.debug("Saving integration parameters: ");
					_logger.debug("lastUtrnno: " + lastUtrnno);
					_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime.toString());
					_logger.debug("lastTransDatetime: " + lastTransDatetime.toString());
					mapper.saveParams_insert(lastUtrnno, JdbcUtils.getSqlTimestamp(lastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(lastTransDatetime));
				} else {
					ObjectPair<Long, Timestamp> pair = ORMUtils.getSingleValue(mapper.saveParams_selectPairs());
					if (pair != null) {// TODO: consider downtimes
						lastParamUtrnno = pair.getKey();
						lastTransParamDatetime = JdbcUtils.getTimestamp(pair.getValue());
					}

					if (lastUtrnno > lastParamUtrnno || lastTransParamDatetime.after(lastTransDatetime)) {
						lastUtrnno = lastParamUtrnno;
						lastTransDatetime = lastTransParamDatetime;
					}
					_logger.debug("Updating integration parameters: ");
					_logger.debug("lastUtrnno: " + lastUtrnno);
					if (lastDowntimeDatetime != null) {
						_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime.toString());
					} else {
						_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime);
					}

					if (lastTransDatetime != null) {
						_logger.debug("lastTransDatetime: " + lastTransDatetime.toString());
					} else {
						_logger.debug("lastTransDatetime: " + lastTransDatetime);
					}

					mapper.saveParams_update(lastUtrnno, JdbcUtils.getSqlTimestamp(lastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(lastTransDatetime));
				}
			}
		} finally {
			session.close();
		}
		_logger.debug("Method saveParams executed");
	}

	public static void saveParamsForTask(ISessionHolder sessionHolder, CmWebTask taskEjb, int taskId,
			List<Integer> atmList) throws SQLException {
		List<WebTaskParamItem> paramList = null;
		SqlSession session = sessionHolder.getSession(getMapperClass());

		StringBuffer lastDowntimeSQL = new StringBuffer(
				"SELECT MAX(end_date) as lastDowntimeDatetime " + " FROM t_cm_intgr_downtime_period where ");
		lastDowntimeSQL.append(JdbcUtils.generateInConditionNumber("pid", atmList));

		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			Long lastUtrnno = 0L;
			Date lastDowntimeDatetime = null;
			Date lastTransDatetime = null;

			ObjectPair<Long, Timestamp> result = ORMUtils.getSingleValue(mapper.saveParamsForTask_lastTransInfo());
			if (result != null) {
				lastUtrnno = result.getKey();
				lastTransDatetime = JdbcUtils.getTimestamp(result.getValue());
			}

			Timestamp downtime = mapper.getTimestampValue(lastDowntimeSQL.toString());
			if (downtime != null) {
				lastDowntimeDatetime = JdbcUtils.getTimestamp(downtime);
			}

			if (lastUtrnno > 0) {
				_logger.debug("Updating integration parameters for task: " + taskId);
				_logger.debug("lastUtrnno: " + lastUtrnno);
				if (lastDowntimeDatetime != null) {
					_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime.toString());
				} else {
					_logger.debug("lastDowntimeDatetime: " + lastDowntimeDatetime);
				}

				if (lastTransDatetime != null) {
					_logger.debug("lastTransDatetime: " + lastTransDatetime.toString());
				} else {
					_logger.debug("lastTransDatetime: " + lastTransDatetime);
				}

				paramList = taskEjb.getWebTaskParamList(taskId);
				for (WebTaskParamItem item : paramList) {
					switch (item.getType()) {
					case DATE_FROM_PATTERN:
						((DateParamItem) item).setValue(lastTransDatetime);
						break;
					case LAST_UTRNNO:
						((LongParamItem) item).setValue(lastUtrnno);
						break;
					default:
						break;
					}

				}
				taskEjb.updateWebTaskParams(taskId, paramList);
				// lastUtrnno;
				// lastDowntimeDatetime;
				// lastTransDatetime;

			}

		} finally {
			session.close();
		}
		_logger.debug("Method saveParams executed");
	}

	public static void saveParamsMultiDisp(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());

		String lastDowntimeSQL = "SELECT MAX(end_date) as lastDowntimeDatetime " + " FROM t_cm_intgr_downtime_period";
		String checkSQL = "SELECT count(1) from t_cm_intgr_params";
		try {
			DataLoadMapper mapper = session.getMapper(getMapperClass());

			int vCheck = 0;
			long lastUtrnno = 0;
			Date lastDowntimeDatetime = null;
			Date lastTransDatetime = null;

			ObjectPair<Long, Timestamp> result = ORMUtils.getSingleValue(mapper.saveParamsMultiDisp_lastTransInfo());
			if (result != null) {
				lastUtrnno = result.getKey();
				lastTransDatetime = JdbcUtils.getTimestamp(result.getValue());
			}

			Timestamp downtime = mapper.getTimestampValue(lastDowntimeSQL.toString());
			if (downtime != null) {
				lastDowntimeDatetime = JdbcUtils.getTimestamp(downtime);
			}

			if (lastUtrnno > 0) {
				vCheck = ORMUtils.getNotNullValue(mapper.getIntegerValue(checkSQL), 0);

				if (vCheck == 0) {
					mapper.saveParams_insert(lastUtrnno, JdbcUtils.getSqlTimestamp(lastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(lastTransDatetime));
				} else {
					mapper.saveParams_update(lastUtrnno, JdbcUtils.getSqlTimestamp(lastDowntimeDatetime),
							JdbcUtils.getSqlTimestamp(lastTransDatetime));
				}
			}
		} finally {
			session.close();
		}
	}

	public static void truncateTrans(ISessionHolder sessionHolder, List<Integer> atmList) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.REUSE);
		java.sql.Statement stmt = null;
		try {
			stmt = session.getConnection().createStatement();
			stmt.addBatch(
					ORMUtils.getDeleteFromTableFieldInConditional(session, "t_cm_intgr_trans", "atm_id", atmList));
			stmt.addBatch(ORMUtils.getTruncateTableUnrecoverable(session, "t_cm_intgr_trans_md_disp"));
			stmt.addBatch(ORMUtils.getDeleteFromTableFieldInConditional(session, "t_cm_intgr_trans_md", "terminal_id",
					atmList));
			stmt.addBatch(ORMUtils.getDeleteFromTableFieldInConditional(session, "t_cm_intgr_downtime_period", "pid",
					atmList));
			stmt.addBatch(ORMUtils.getDeleteFromTableFieldInConditional(session, "t_cm_intgr_downtime_cashout", "pid",
					atmList));
			stmt.addBatch(ORMUtils.getDeleteFromTableFieldInConditional(session, "t_cm_intgr_downtime_cashin", "pid",
					atmList));
			stmt.addBatch(ORMUtils.getDeleteFromTableFieldInConditional(session, "t_cm_intgr_trans_cash_in", "atm_id",
					atmList));
			stmt.executeBatch();
		} finally {
			stmt.close();
			session.close();
		}
	}

	public static void truncateAllTrans(Connection connection) throws SQLException {
		java.sql.Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_trans"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_trans_md_disp"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_trans_md"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_downtime_period"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_downtime_cashout"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_downtime_cashin"));
			stmt.addBatch(JdbcUtils.getTruncateTableUnrecoverable(connection, "t_cm_intgr_trans_cash_in"));
			stmt.executeBatch();
		} finally {
			stmt.close();
		}
	}

}
