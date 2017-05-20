package ru.bpc.cm.integration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//imports for workmanger
import commonj.work.Work;
import commonj.work.WorkException;
import commonj.work.WorkItem;
import commonj.work.WorkManager;
import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.integration.orm.AggregationMapper;
import ru.bpc.cm.orm.common.ORMUtils;
import ru.bpc.cm.orm.items.MultiObject;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class AggregationController {

	private static final int BATCH_MAX_SIZE = 999;
	public static final int CO_719_ENC_TRANSACTION_TYPE = 0;
	public static final int CO_743_ENC_TRANSACTION_TYPE = 6;

	public static final int DEBIT_TRANSACTION_TYPE = 1;
	public static final int CREDIT_TRANSACTION_TYPE = 2;
	public static final int EXCHANGE_TRANSACTION_TYPE = 3;
	public static final int CI_ENC_TRANSACTION_TYPE = 4;
	public static final int CO_CA_ENC_TRANSACTION_TYPE = 5;

	public static final int CR_909_ENC_TRANSACTION_TYPE = 7;
	public static final int CR_910_ENC_TRANSACTION_TYPE = 8;

	private static final int OFFLINE_DOWNTIME_TYPE = 0;
	private static final int CASHOUT_DOWNTIME_TYPE = 1;
	private static final int CASHIN_DOWNTIME_TYPE = 2;
	public static final int CO_ENC_DET_UNLOADED = 1;
	public static final int CO_ENC_DET_LOADED = 2;
	public static final int CO_ENC_DET_NOT_UNLOADED = 3;
	public static final int CO_ENC_DET_NOT_UNLOADED_CA = 4;
	public static final int CASS_TYPE_CASH_OUT = 1;
	// private static final int CASS_TYPE_CASH_IN = 2;
	public static final int CASS_TYPE_RECYCLING = 3;

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<AggregationMapper> getMapperClass() {
		return AggregationMapper.class;
	}

	private static WorkManager getWorkManager() {
		WorkManager wm = null;
		try {
			InitialContext ic = new InitialContext();
			// wm = (WorkManager)ic.lookup("java:comp/env/CmWorkManager");
			wm = (WorkManager) ic.lookup("java:comp/env/wm/default");
		} catch (NamingException e) {

			e.printStackTrace();

		}
		return wm;
	}

	public static void aggregate_cash_out(final ISessionHolder sessionHolder) throws SQLException {
		_logger.debug("Executing aggregate_cash_out method");
		int cores = Runtime.getRuntime().availableProcessors();
		int worksadded = 0;
		WorkManager wm;

		wm = getWorkManager();
		int worksaddedall = 0;
		List<WorkItem> worklist = new ArrayList<WorkItem>();
		final List<ObjectPair<Integer, Boolean>> atmIdList = new ArrayList<ObjectPair<Integer, Boolean>>();
		int atmCount = 0;

		final AtomicBoolean intgr_trans_write_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_curr_stat_write_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_cass_stat_write_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_write_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_details_write_lock = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_write_lock = new AtomicBoolean(false);
		final AtomicBoolean reject_stat_write_lock = new AtomicBoolean(false);

		final AtomicBoolean intgr_trans_read_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_curr_stat_read_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_cass_stat_read_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_read_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_details_read_lock = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_read_lock = new AtomicBoolean(false);
		final AtomicBoolean reject_stat_read_lock = new AtomicBoolean(false);

		String transSelectLoopSQL = "SELECT DISTINCT ATM_ID as result FROM t_cm_intgr_trans";

		final SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			final AggregationMapper mapper = session.getMapper(getMapperClass());
			List<String> pidList = mapper.getStringValueByQuery(transSelectLoopSQL);
			for (String item : pidList) {
				atmIdList.add(new ObjectPair<Integer, Boolean>(Integer.parseInt(item), false));
			}

			atmCount = atmIdList.size();

			while (worksaddedall < atmCount) {
				for (final ObjectPair<Integer, Boolean> item : atmIdList) {
					if (worksadded < cores) {
						if (item.getValue() == false) {
							worklist.add(wm.schedule(new Work() {

								@Override
								public void run() {
									_logger.debug("Thread created for ATM:" + item.getKey());

									int lastEncId = 0;
									int encID = 0;
									Date nextIncass = null;

									List<ObjectPair<Integer, Timestamp>> encList = new ArrayList<ObjectPair<Integer, Timestamp>>();

									try {
										lastEncId = ORMUtils.getNotNullValue(
												mapper.aggregateCashOut_getLastEncId(String.valueOf(item.getKey())), 0);

										insertEncashments(sessionHolder, String.valueOf(item.getKey()),
												intgr_trans_read_lock, intgr_trans_write_lock,
												enc_cashout_stat_read_lock, enc_cashout_stat_write_lock,
												enc_cashout_stat_details_read_lock,
												enc_cashout_stat_details_write_lock);

										encList.addAll(mapper.aggregateCashOut_getCoEncStat(
												String.valueOf(item.getKey()), lastEncId));

										for (ObjectPair<Integer, Timestamp> objectPair : encList) {

											encID = objectPair.getKey();
											if (encID > lastEncId) {
												mapper.simpleDeleteCashOutQuery("T_CM_REJECT_STAT",
														String.valueOf(item.getKey()), lastEncId,
														objectPair.getValue());
												mapper.simpleDeleteCashOutQuery("T_CM_CASHOUT_CASS_STAT",
														String.valueOf(item.getKey()), lastEncId,
														objectPair.getValue());
												mapper.simpleDeleteCashOutQuery("T_CM_CASHOUT_CURR_STAT",
														String.valueOf(item.getKey()), lastEncId,
														objectPair.getValue());
											}

											try {
												Timestamp nextIncDate = mapper.aggregateCashOut_getNextIncass(
														String.valueOf(item.getKey()), objectPair.getValue());

												if (nextIncDate != null) {
													nextIncass = JdbcUtils.getTimestamp(nextIncDate);
												}
											} catch (Exception ex) {
												nextIncass = new Date();
											}

											insertCurrStat(sessionHolder, String.valueOf(item.getKey()), encID,
													JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
													intgr_trans_read_lock, intgr_trans_write_lock,
													cashout_curr_stat_read_lock, cashout_curr_stat_read_lock);

											insertCassStat(session.getConnection(), String.valueOf(item.getKey()), encID,
													JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
													intgr_trans_read_lock, intgr_trans_write_lock,
													cashout_cass_stat_read_lock, cashout_cass_stat_write_lock);

											insertEncashmentsPartAndOut(session.getConnection(), String.valueOf(item.getKey()),
													encID, JdbcUtils.getTimestamp(objectPair.getValue()),
													enc_cashout_stat_details_read_lock,
													enc_cashout_stat_details_write_lock, enc_cashout_stat_read_lock,
													enc_cashout_stat_write_lock, cashout_cass_stat_read_lock,
													cashout_cass_stat_write_lock);

											insertRemainingsForCass(session.getConnection(), String.valueOf(item.getKey()), encID,
													JdbcUtils.getTimestamp(objectPair.getValue()),
													enc_cashout_stat_details_read_lock,
													enc_cashout_stat_details_write_lock, cashout_cass_stat_read_lock,
													cashout_cass_stat_write_lock);

											insertZeroTakeOffsForCass(session.getConnection(), String.valueOf(item.getKey()),
													encID, JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
													enc_cashout_stat_details_read_lock,
													enc_cashout_stat_details_write_lock, cashout_cass_stat_read_lock,
													cashout_cass_stat_write_lock, intgr_downtime_cashout_read_lock,
													intgr_downtime_cashout_write_lock);

											insertZeroTakeOffsForCurr(session.getConnection(), String.valueOf(item.getKey()),
													encID, JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
													enc_cashout_stat_details_read_lock,
													enc_cashout_stat_details_write_lock, cashout_curr_stat_read_lock,
													cashout_curr_stat_write_lock);

											insertRemainingsForCurr(session.getConnection(), String.valueOf(item.getKey()), encID,
													cashout_cass_stat_read_lock, cashout_cass_stat_write_lock,
													cashout_curr_stat_read_lock, cashout_curr_stat_write_lock);

											insertRejects(session.getConnection(), String.valueOf(item.getKey()), encID,
													JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
													intgr_trans_read_lock, intgr_trans_write_lock,
													reject_stat_read_lock, reject_stat_write_lock);

											insertZeroDaysForRejects(session.getConnection(), String.valueOf(item.getKey()),
													encID, JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
													reject_stat_read_lock, reject_stat_write_lock);

											insertRemainingsForRejects(session.getConnection(), String.valueOf(item.getKey()),
													encID, reject_stat_read_lock, reject_stat_write_lock);
										}
										item.setValue(true);

									} catch (SQLException e) {
										e.printStackTrace();
									}
								}

								@Override
								public void release() {
								}

								@Override
								public boolean isDaemon() {
									return false;
								}
							}));
							worksadded++;
							worksaddedall++;
						}
					} else {
						wm.waitForAll(worklist, 25920000);
						worklist.clear();
						worksadded = 0;
					}
				}
			}
			wm.waitForAll(worklist, 25920000);
			worklist.clear();

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (WorkException e) {
			e.printStackTrace();
		} finally {
			session.close();
		}
	}

	public static String locker(String tableName) {
		String lock = "LOCK TABLE " + tableName + " IN EXCLUSIVE MODE";
		return lock;
	}

	public static String unlocker(String tableName) {
		String unlock = "ALTER TABLE " + tableName + " DISABLE TABLE LOCK";
		return unlock;
	}

	public static String enableLock(String tableName) {
		String lockEnable = "ALTER TABLE " + tableName + " ENABLE TABLE LOCK";
		return lockEnable;
	}

	public static void aggregate_cash_out(final ISessionHolder sessionHolder, Date dateFrom, List<Integer> atmList)
			throws SQLException {
		_logger.debug("Executing aggregate_cash_out method for atms: " + atmList.toString());

		int cores = Runtime.getRuntime().availableProcessors();
		_logger.debug("Cores number = " + cores);
		int worksadded = 0;
		int worksaddedall = 0;
		WorkManager wm;
		final SqlSession session = sessionHolder.getSession(getMapperClass());
		Connection connection = session.getConnection();
		wm = getWorkManager();
		List<WorkItem> worklist = new ArrayList<WorkItem>();
		final List<ObjectPair<Integer, Boolean>> atmIdList = new ArrayList<ObjectPair<Integer, Boolean>>();
		int atmCount = 0;

		final AtomicBoolean intgr_trans_write_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_curr_stat_write_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_cass_stat_write_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_write_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_details_write_lock = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_write_lock = new AtomicBoolean(false);
		final AtomicBoolean reject_stat_write_lock = new AtomicBoolean(false);

		final AtomicBoolean intgr_trans_read_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_curr_stat_read_lock = new AtomicBoolean(false);
		final AtomicBoolean cashout_cass_stat_read_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_read_lock = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_details_read_lock = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_read_lock = new AtomicBoolean(false);
		final AtomicBoolean reject_stat_read_lock = new AtomicBoolean(false);

		// getStringValueByQuery (rename PID to result)
		StringBuffer transSelectLoopSQL = new StringBuffer(
				"SELECT DISTINCT ATM_ID as PID FROM t_cm_intgr_trans where ");

		transSelectLoopSQL.append(JdbcUtils.generateInConditionNumber("atm_id", atmList));
		// aggregateCashOut_getCoEncStat()
		final String coEncStatSelectLoopSQL = "SELECT ENC_DATE,ENCASHMENT_ID " + " FROM T_CM_ENC_CASHOUT_STAT "
				+ " WHERE ATM_ID = ? " + " AND ENCASHMENT_ID >= ? " + " ORDER BY ENC_DATE";
		// aggregateCashOut_getLastEncId()
		final String lastEncIdSelectSQL = "SELECT COALESCE(MAX(ENCASHMENT_ID),0) " + " FROM T_CM_ENC_CASHOUT_STAT "
				+ " WHERE ATM_ID = ?";
		// aggregateCashOut_getNextIncass()
		final String nextIncassSelectSQL = "SELECT COALESCE(MIN(DISTINCT ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE "
				+ " FROM T_CM_ENC_CASHOUT_STAT " + " WHERE ATM_ID = ? " + " AND ENC_DATE > ?";
		// simpleDeleteQuery()
		final String rejectStatDeleteSQL = "DELETE FROM T_CM_REJECT_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String coCassStatDeleteSQL = "DELETE FROM T_CM_CASHOUT_CASS_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String coCurrStatDeleteSQl = "DELETE FROM T_CM_CASHOUT_CURR_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		PreparedStatement pstmtLoop = null;

		ResultSet rsLoop = null;

		try {

			pstmtLoop = connection.prepareStatement(transSelectLoopSQL.toString());
			rsLoop = pstmtLoop.executeQuery();
			while (rsLoop.next()) {
				atmIdList.add(new ObjectPair<Integer, Boolean>(Integer.parseInt(rsLoop.getString("PID")), false));
			}
			JdbcUtils.close(rsLoop);
			JdbcUtils.close(pstmtLoop);
			JdbcUtils.close(connection);
			atmCount = atmIdList.size();
			_logger.debug("ATMS count = " + atmCount);
			// pstmtLoop2 = connection.prepareStatement(coEncStatSelectLoopSQL);
			while (worksaddedall < atmCount) {
				for (final ObjectPair<Integer, Boolean> item : atmIdList) {
					if (worksadded < cores) {
						worklist.add(wm.schedule(new Work() {

							@Override

							public void run() {
								_logger.debug("Thread created");
								Connection connect = null;
								PreparedStatement pstmtLoop2 = null;
								PreparedStatement pstmtSelect = null;
								PreparedStatement pstmtDelete = null;
								ResultSet rsLoop2 = null;
								ResultSet rs = null;
								int lastEncId = 0;
								int encID = 0;
								Date nextIncass = null;

								List<ObjectPair<Integer, Timestamp>> encList = new ArrayList<ObjectPair<Integer, Timestamp>>();

								try {
									connect = session.getConnection();
								} catch (Exception e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								try {
									pstmtSelect = connect.prepareStatement(lastEncIdSelectSQL);
									// pstmtSelect.setString(1,
									// rsLoop.getString("PID"));
									pstmtSelect.setString(1, String.valueOf(item));
									rs = pstmtSelect.executeQuery();
									if (rs.next()) {
										lastEncId = rs.getInt(1);
									}

									JdbcUtils.close(rs);
									JdbcUtils.close(pstmtSelect);

									// insertEncashments(connection,
									// rsLoop.getString("PID"));
									// insertEncashments(connect,
									// String.valueOf(item.getKey()),
									// intgr_trans_status,
									// enc_cashout_stat_status,
									// enc_cashout_stat_details_status);
									insertEncashments(sessionHolder, String.valueOf(item.getKey()),
											intgr_trans_read_lock, intgr_trans_write_lock, enc_cashout_stat_read_lock,
											enc_cashout_stat_write_lock, enc_cashout_stat_details_read_lock,
											enc_cashout_stat_details_write_lock);
									// Cash out stats insert
									pstmtLoop2 = connect.prepareStatement(coEncStatSelectLoopSQL);
									// pstmtLoop2.setString(1,
									// rsLoop.getString("PID"));
									pstmtLoop2.setString(1, String.valueOf(item));
									pstmtLoop2.setInt(2, lastEncId);
									rsLoop2 = pstmtLoop2.executeQuery();
									while (rsLoop2.next()) {
										encList.add(new ObjectPair<Integer, Timestamp>(rsLoop2.getInt("ENCASHMENT_ID"),
												rsLoop2.getTimestamp("ENC_DATE")));
									}
									JdbcUtils.close(rsLoop2);
									JdbcUtils.close(pstmtLoop2);
									// while (rsLoop2.next()) {
									for (ObjectPair<Integer, Timestamp> objectPair : encList) {
										// encID =
										// rsLoop2.getInt("ENCASHMENT_ID");
										encID = objectPair.getKey();
										if (encID > lastEncId) {
											pstmtDelete = connect.prepareStatement(rejectStatDeleteSQL);
											// pstmtDelete.setString(1,
											// rsLoop.getString("PID"));
											pstmtDelete.setString(1, String.valueOf(item));
											pstmtDelete.setInt(2, lastEncId);
											// pstmtDelete.setTimestamp(3,
											// rsLoop2.getTimestamp("ENC_DATE"));
											pstmtDelete.setTimestamp(3, objectPair.getValue());
											pstmtDelete.executeUpdate();
											JdbcUtils.close(pstmtDelete);

											pstmtDelete = connect.prepareStatement(coCassStatDeleteSQL);
											// pstmtDelete.setString(1,
											// rsLoop.getString("PID"));
											pstmtDelete.setString(1, String.valueOf(item));
											pstmtDelete.setInt(2, lastEncId);
											// pstmtDelete.setTimestamp(3,
											// rsLoop2.getTimestamp("ENC_DATE"));
											pstmtDelete.setTimestamp(3, objectPair.getValue());
											pstmtDelete.executeUpdate();
											JdbcUtils.close(pstmtDelete);

											pstmtDelete = connect.prepareStatement(coCurrStatDeleteSQl);
											// pstmtDelete.setString(1,
											// rsLoop.getString("PID"));
											pstmtDelete.setString(1, String.valueOf(item));
											pstmtDelete.setInt(2, lastEncId);
											// pstmtDelete.setTimestamp(3,
											// rsLoop2.getTimestamp("ENC_DATE"));
											pstmtDelete.setTimestamp(3, objectPair.getValue());
											pstmtDelete.executeUpdate();
											JdbcUtils.close(pstmtDelete);
										}
										// while (rsLoop2.next()) {

										try {
											pstmtSelect = connect.prepareStatement(nextIncassSelectSQL);
											// pstmtSelect.setString(1,
											// rsLoop.getString("PID"));
											pstmtSelect.setString(1, String.valueOf(item));
											// pstmtSelect.setTimestamp(2,
											// rsLoop2.getTimestamp("ENC_DATE"));
											pstmtSelect.setTimestamp(2, objectPair.getValue());
											rs = pstmtSelect.executeQuery();
											if (rs.next()) {
												nextIncass = JdbcUtils.getTimestamp(rs.getTimestamp(1));
											}
										} catch (SQLException ex) {
											nextIncass = new Date();// not sure
										} finally {
											JdbcUtils.close(rs);
											JdbcUtils.close(pstmtSelect);
										}

										insertCurrStat(sessionHolder, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
												intgr_trans_read_lock, intgr_trans_write_lock,
												cashout_curr_stat_read_lock, cashout_curr_stat_read_lock);
	
										insertCassStat(connect, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
												intgr_trans_read_lock, intgr_trans_write_lock,
												cashout_cass_stat_read_lock, cashout_cass_stat_write_lock);
										
										insertEncashmentsPartAndOut(connect, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()),
												enc_cashout_stat_details_read_lock, enc_cashout_stat_details_write_lock,
												enc_cashout_stat_read_lock, enc_cashout_stat_write_lock,
												cashout_cass_stat_read_lock, cashout_cass_stat_write_lock);
										
										insertRemainingsForCass(connect, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()),
												enc_cashout_stat_details_read_lock, enc_cashout_stat_details_write_lock,
												cashout_cass_stat_read_lock, cashout_cass_stat_write_lock);
										
										insertZeroTakeOffsForCass(connect, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
												enc_cashout_stat_details_read_lock, enc_cashout_stat_details_write_lock,
												cashout_cass_stat_read_lock, cashout_cass_stat_write_lock,
												intgr_downtime_cashout_read_lock, intgr_downtime_cashout_write_lock);
										
										insertZeroTakeOffsForCurr(connect, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
												enc_cashout_stat_details_read_lock, enc_cashout_stat_details_write_lock,
												cashout_curr_stat_read_lock, cashout_curr_stat_write_lock);
										
										insertRemainingsForCurr(connect, String.valueOf(item.getKey()), encID,
												cashout_cass_stat_read_lock, cashout_cass_stat_write_lock,
												cashout_curr_stat_read_lock, cashout_curr_stat_write_lock);
										insertRejects(connect, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
												intgr_trans_read_lock, intgr_trans_write_lock, reject_stat_read_lock,
												reject_stat_write_lock);
										
										insertZeroDaysForRejects(connect, String.valueOf(item.getKey()), encID,
												JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
												reject_stat_read_lock, reject_stat_write_lock);
										
										insertRemainingsForRejects(connect, String.valueOf(item.getKey()), encID,
												reject_stat_read_lock, reject_stat_write_lock);

									}
									item.setValue(true);
								} catch (SQLException e) {
									e.printStackTrace();
								}
								JdbcUtils.close(connect);
								_logger.debug("Thread ended");
							}

							@Override

							public void release() {

							}

							@Override

							public boolean isDaemon() {

								return false;

							}
						}));
						worksadded++;
						worksaddedall++;
						_logger.debug("Threads added:" + worksadded);
						_logger.debug("All Threads added:" + worksaddedall);
					} else {
						wm.waitForAll(worklist, 25920000);
						worklist.clear();
						worksadded = 0;
					}
				}
			}
			wm.waitForAll(worklist, 25920000);
			worklist.clear();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (WorkException e) {
			e.printStackTrace();
		} finally {
			connection.close();
			session.close();
		}
		_logger.debug("Method aggregate_cash_out executed for atms: " + atmList.toString());

	}

	public static void aggregate_cash_out_multi_disp(final DataSource dataSource) throws SQLException {
		_logger.debug("Executing aggregate_cash_out_multi_disp method");
		Date nextIncass = null;

		final AtomicBoolean intgr_trans_status = new AtomicBoolean(false);
		final AtomicBoolean cashout_curr_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashout_cass_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashout_stat_details_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_status = new AtomicBoolean(false);
		final AtomicBoolean reject_stat_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_trans_md_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_trans_md_disp_status = new AtomicBoolean(false);

		int cores = Runtime.getRuntime().availableProcessors();
		_logger.debug("Cores number = " + cores);
		int worksadded = 0;
		WorkManager wm;
		Connection connection = dataSource.getConnection();
		wm = getWorkManager();
		List<WorkItem> worklist = new ArrayList<WorkItem>();
		final List<Integer> atmIdList = new ArrayList<Integer>();

		// getStringValueByQuery() rename to result
		String transSelectLoopSQL = "SELECT DISTINCT terminal_id FROM t_cm_intgr_trans_md";

		// aggregateCashOut_getCoEncStat()
		final String coEncStatSelectLoopSQL = "SELECT ENC_DATE,ENCASHMENT_ID " + " FROM T_CM_ENC_CASHOUT_STAT "
				+ " WHERE ATM_ID = ? " + " AND ENCASHMENT_ID >= ? " + " ORDER BY ENC_DATE";

		// aggregateCashOut_getLastEncId()
		final String lastEncIdSelectSQL = "SELECT COALESCE(MAX(ENCASHMENT_ID),0) " + "FROM T_CM_ENC_CASHOUT_STAT "
				+ "WHERE ATM_ID = ?";

		// aggregateCashOut_getNextIncass()
		final String nextIncassSelectSQL = "SELECT COALESCE(MIN(DISTINCT ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE "
				+ " FROM T_CM_ENC_CASHOUT_STAT " + " WHERE ATM_ID = ? " + " AND ENC_DATE > ?";

		PreparedStatement pstmtLoop = null;

		ResultSet rsLoop = null;

		try {
			pstmtLoop = connection.prepareStatement(transSelectLoopSQL);
			rsLoop = pstmtLoop.executeQuery();
			while (rsLoop.next()) {
				atmIdList.add(Integer.parseInt(rsLoop.getString("PID")));
			}
			JdbcUtils.close(rsLoop);
			JdbcUtils.close(pstmtLoop);
			JdbcUtils.close(connection);

			for (final Integer item : atmIdList) {
				if (worksadded < cores) {
					worklist.add(wm.schedule(new Work() {
						@Override

						public void run() {
							_logger.debug("Thread created");
							Connection connect = null;
							PreparedStatement pstmtLoop2 = null;
							PreparedStatement pstmtSelect = null;
							ResultSet rsLoop2 = null;
							ResultSet rs = null;
							int lastEncId = 0;
							int encID = 0;
							Date nextIncass = null;

							List<ObjectPair<Integer, Timestamp>> encList = new ArrayList<ObjectPair<Integer, Timestamp>>();
							try {
								connect = dataSource.getConnection();
							} catch (SQLException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							try {
								pstmtLoop2 = connect.prepareStatement(coEncStatSelectLoopSQL);
								pstmtSelect = connect.prepareStatement(lastEncIdSelectSQL);
								pstmtSelect.setString(1, String.valueOf(item));
								rs = pstmtSelect.executeQuery();
								lastEncId = rs.getInt(1);
								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);

								insertEncashmentsMd(connect, String.valueOf(item), intgr_trans_md_status,
										intgr_trans_md_disp_status, enc_cashout_stat_status,
										enc_cashout_stat_details_status);
								// Cash out stats insert
								pstmtLoop2.setString(1, String.valueOf(item));
								pstmtLoop2.setInt(2, lastEncId);
								rsLoop2 = pstmtLoop2.executeQuery();
								while (rsLoop2.next()) {
									encList.add(new ObjectPair<Integer, Timestamp>(rsLoop2.getInt("ENCASHMENT_ID"),
											rsLoop2.getTimestamp("ENC_DATE")));
								}
								JdbcUtils.close(rsLoop2);
								JdbcUtils.close(pstmtLoop2);
								for (ObjectPair<Integer, Timestamp> objectPair : encList) {
									encID = objectPair.getKey();

									try {
										pstmtSelect = connect.prepareStatement(nextIncassSelectSQL);
										pstmtSelect.setString(1, String.valueOf(item));
										pstmtSelect.setTimestamp(2, objectPair.getValue());
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											nextIncass = JdbcUtils.getTimestamp(rs.getTimestamp(1));
										}

									} catch (SQLException ex) {
										nextIncass = new Date();// not sure
									} finally {
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);
									}

									// Curr stat
									// insert---------------------------------
									insertCurrStatMd(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_md_status, intgr_trans_md_disp_status,
											cashout_curr_stat_status);
									// Cass stat
									// insert---------------------------------
									insertCassStatMd(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_md_status, intgr_trans_md_disp_status,
											cashout_cass_stat_status);
									// Dealing with partial encashments--------
									// insertEncashmentsPartAndOut(connect,
									// String.valueOf(item), encID,
									// JdbcUtils.getTimestamp(objectPair.getValue()),
									// enc_cashout_stat_details_status,
									// enc_cashout_stat_status,
									// cashout_cass_stat_status);
									// Filling zero take offs - cash out cass
									// stat------------------
									// insertZeroTakeOffsForCass(connect,
									// String.valueOf(item), encID,
									// JdbcUtils.getTimestamp(objectPair.getValue()),
									// nextIncass,
									// enc_cashout_stat_details_status,
									// cashout_cass_stat_status,
									// intgr_downtime_cashout_status);
									// Filling zero take offs - cash out curr
									// stat------------------
									// insertZeroTakeOffsForCurr(connect,
									// String.valueOf(item), encID,
									// JdbcUtils.getTimestamp(objectPair.getValue()),
									// nextIncass,
									// enc_cashout_stat_details_status,
									// cashout_curr_stat_status);
									// Inserting remainings for curr
									// stat------------------
									// insertRemainingsForCurr(connect,
									// String.valueOf(item), encID,
									// cashout_cass_stat_status,
									// cashout_curr_stat_status);
									// Rejects
									// insert------------------------------------
									insertRejectsMd(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_md_status, reject_stat_status);
									// Zero days insert-----------------
									// insertZeroDaysForRejects(connect,
									// String.valueOf(item), encID,
									// JdbcUtils.getTimestamp(objectPair.getValue()),
									// nextIncass, reject_stat_status);
									// Remaining insert-----------------
									// insertRemainingsForRejects(connect,
									// String.valueOf(item), encID,
									// reject_stat_status);

								}

							} catch (SQLException e) {
								e.printStackTrace();
							}
							JdbcUtils.close(connect);
							_logger.debug("Thread ended");

						}

						@Override

						public void release() {

						}

						@Override

						public boolean isDaemon() {

							return false;

						}
					}));
					worksadded++;
				} else {
					wm.waitForAll(worklist, 25920000);
					worklist.clear();
					worksadded = 0;
				}
			}

			wm.waitForAll(worklist, 25920000);
			worklist.clear();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (WorkException e) {
			e.printStackTrace();
		}
		_logger.debug("Method aggregate_cash_out_multi_disp executed");

	}

	public static void aggregate_cash_in(final ISessionHolder sessionHolder) throws SQLException {
		_logger.debug("Executing aggregate_cash_in method");
		int cores = Runtime.getRuntime().availableProcessors();
		_logger.debug("Cores number = " + cores);
		int worksadded = 0;
		WorkManager wm;
		final SqlSession session = sessionHolder.getSession(getMapperClass());
		final Connection connection = session.getConnection();

		final AtomicBoolean intgr_trans_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashin_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_trans_cash_in_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_denom_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_r_cass_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_r_curr_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashin_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashin_stat_details_status = new AtomicBoolean(false);

		wm = getWorkManager();

		List<WorkItem> worklist = new ArrayList<WorkItem>();
		final List<Integer> atmIdList = new ArrayList<Integer>();

		// getStringValueByQuery() rename to result
		String transSelectLoopSQL = "SELECT DISTINCT ATM_ID as PID FROM t_cm_intgr_trans";

		// aggregateCashIn_getCiEncStat()
		final String ciEncStatSelectLoopSQL = "SELECT CASH_IN_ENC_DATE as ENC_DATE ,CASH_IN_ENCASHMENT_ID as ENCASHMENT_ID "
				+ "FROM T_CM_ENC_CASHIN_STAT " + " WHERE ATM_ID = ? " + " AND CASH_IN_ENCASHMENT_ID >= ? "
				+ " ORDER BY CASH_IN_ENC_DATE";

		// aggregateCashIn_getLastEncId()
		final String lastEncIdSelectSQL = "SELECT COALESCE(MAX(CASH_IN_ENCASHMENT_ID),0) "
				+ " FROM T_CM_ENC_CASHIN_STAT " + " WHERE ATM_ID = ?";

		// aggregateCashIn_getNextIncass()
		final String nextIncassSelectSQL = "SELECT COALESCE(MIN(DISTINCT CASH_IN_ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE "
				+ " FROM T_CM_ENC_CASHIN_STAT " + " WHERE ATM_ID = ? " + " AND CASH_IN_ENC_DATE > ?";

		final String ciStatDeleteSQL = "DELETE FROM T_CM_CASHIN_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String ciDenomStatDeleteSQL = "DELETE FROM T_CM_CASHIN_DENOM_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String ciRCassStatDeleteSQl = "DELETE FROM T_CM_CASHIN_R_CASS_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String ciRCurrStatDeleteSQl = "DELETE FROM T_CM_CASHIN_R_CURR_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		PreparedStatement pstmtLoop = null;
		ResultSet rsLoop = null;

		try {
			pstmtLoop = connection.prepareStatement(transSelectLoopSQL);
			rsLoop = pstmtLoop.executeQuery();
			while (rsLoop.next()) {
				atmIdList.add(Integer.parseInt(rsLoop.getString("PID")));
			}
			JdbcUtils.close(rsLoop);
			JdbcUtils.close(pstmtLoop);
			JdbcUtils.close(connection);
			for (final Integer item : atmIdList) {
				if (worksadded < cores) {
					worklist.add(wm.schedule(new Work() {

						@Override

						public void run() {
							_logger.debug("Thread created");
							Connection connect = null;
							PreparedStatement pstmtLoop2 = null;
							PreparedStatement pstmtSelect = null;
							PreparedStatement pstmtDelete = null;
							ResultSet rsLoop2 = null;
							ResultSet rs = null;
							int lastEncId = 0;
							int encID = 0;
							Date nextIncass = null;

							List<ObjectPair<Integer, Timestamp>> encList = new ArrayList<ObjectPair<Integer, Timestamp>>();

							try {
								connect = session.getConnection();
							} catch (Exception e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							try {
								pstmtSelect = connect.prepareStatement(lastEncIdSelectSQL);
								pstmtSelect.setString(1, String.valueOf(item));
								rs = pstmtSelect.executeQuery();
								if (rs.next()) {
									lastEncId = rs.getInt(1);
								}

								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);

								insertEcnashmentsCashIn(connect, String.valueOf(item), intgr_trans_status,
										enc_cashin_stat_status, enc_cashin_stat_details_status);
								pstmtLoop2 = connect.prepareStatement(ciEncStatSelectLoopSQL);
								pstmtLoop2.setString(1, String.valueOf(item));
								pstmtLoop2.setInt(2, lastEncId);
								rsLoop2 = pstmtLoop2.executeQuery();
								while (rsLoop2.next()) {
									encList.add(new ObjectPair<Integer, Timestamp>(rsLoop2.getInt("ENCASHMENT_ID"),
											rsLoop2.getTimestamp("ENC_DATE")));
								}
								JdbcUtils.close(rsLoop2);
								JdbcUtils.close(pstmtLoop2);
								for (ObjectPair<Integer, Timestamp> objectPair : encList) {
									encID = objectPair.getKey();
									if (encID > lastEncId) {
										pstmtDelete = connect.prepareStatement(ciStatDeleteSQL);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);

										pstmtDelete = connection.prepareStatement(ciDenomStatDeleteSQL);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);

										pstmtDelete = connection.prepareStatement(ciRCassStatDeleteSQl);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);

										pstmtDelete = connection.prepareStatement(ciRCurrStatDeleteSQl);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);
									}

									try {
										pstmtSelect = connect.prepareStatement(nextIncassSelectSQL);
										pstmtSelect.setString(1, String.valueOf(item));
										pstmtSelect.setTimestamp(2, objectPair.getValue());
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											nextIncass = JdbcUtils.getTimestamp(rs.getTimestamp(1));
										}
									} catch (SQLException ex) {
										nextIncass = new Date();// not sure
									} finally {
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);
										JdbcUtils.close(pstmtDelete);
									}

									insertCashInStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_cash_in_status, intgr_trans_status,
											intgr_downtime_cashin_status, cashin_stat_status);
									// -------------------------------------------------
									insertCiEncashmentsPartAndOut(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), enc_cashin_stat_status,
											enc_cashin_stat_details_status, cashin_r_cass_stat_status);
									// Curr stat
									// insert---------------------------------
									insertCashInCurrStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_status, cashin_r_curr_stat_status);
									// Cass stat
									// insert---------------------------------
									insertCashInCassStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_status, intgr_downtime_cashin_status,
											cashin_r_curr_stat_status);
									// Inserting remainings for cass
									// stat------------------
									insertRemainingsForCashInCass(sessionHolder, String.valueOf(item), encID,
											enc_cashin_stat_details_status, cashin_r_cass_stat_status);
									// Filling zero take offs - cash out cass
									// stat------------------
									insertZeroTakeOffsForCICass(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											cashin_r_cass_stat_status, intgr_downtime_cashout_status);
									// Filling zero take offs - cash out curr
									// stat------------------
									insertZeroTakeOffsForCICurr(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											cashin_r_curr_stat_status, cashin_r_cass_stat_status);
									// Inserting remainings for curr
									// stat------------------
									insertRemainingsForCashInCurr(connect, String.valueOf(item), encID,
											cashin_r_cass_stat_status, cashin_r_curr_stat_status);
									insertCashInDenomStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_cash_in_status, intgr_trans_status,
											intgr_downtime_cashin_status, cashin_denom_stat_status);
									// Zero days insert-----------------
									insertZeroDaysForCashIn(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											cashin_denom_stat_status, intgr_downtime_cashin_status, cashin_stat_status);
									// Remaining insert-----------------
									insertRemainingsForCashIn(connect, String.valueOf(item), encID, cashin_stat_status);

								}

							} catch (SQLException e) {
								e.printStackTrace();
							}
							JdbcUtils.close(connect);
							_logger.debug("Thread ended");
						}

						@Override

						public void release() {

						}

						@Override

						public boolean isDaemon() {

							return false;

						}
					}));
					worksadded++;
				} else {
					wm.waitForAll(worklist, 25920000);
					worklist.clear();
					worksadded = 0;
				}
			}

			wm.waitForAll(worklist, 25920000);
			worklist.clear();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (WorkException e) {
			e.printStackTrace();
		}
		_logger.debug("Method aggregate_cash_in executed");
	}

	public static void aggregate_cash_in(final ISessionHolder sessionHolder, Date dateFrom, List<Integer> atmList)
			throws SQLException {
		_logger.debug("Executing aggregate_cash_in method for atms: " + atmList.toString());
		int cores = Runtime.getRuntime().availableProcessors();
		_logger.debug("Cores number = " + cores);
		int worksadded = 0;
		WorkManager wm;
		final SqlSession session = sessionHolder.getSession(getMapperClass());
		Connection connection = session.getConnection();

		final AtomicBoolean intgr_trans_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashin_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_trans_cash_in_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_denom_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_r_cass_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_r_curr_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashin_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashin_stat_details_status = new AtomicBoolean(false);

		wm = getWorkManager();

		List<WorkItem> worklist = new ArrayList<WorkItem>();
		final List<Integer> atmIdList = new ArrayList<Integer>();

		// getStringValueByQuery() rename to result
		StringBuffer transSelectLoopSQL = new StringBuffer(
				"SELECT DISTINCT ATM_ID as PID FROM t_cm_intgr_trans where ");
		transSelectLoopSQL.append(JdbcUtils.generateInConditionNumber("atm_id", atmList));

		// aggregateCashIn_getCiEncStat()
		final String ciEncStatSelectLoopSQL = "SELECT CASH_IN_ENC_DATE as ENC_DATE ,CASH_IN_ENCASHMENT_ID as ENCASHMENT_ID "
				+ "FROM T_CM_ENC_CASHIN_STAT " + " WHERE ATM_ID = ? " + " AND CASH_IN_ENCASHMENT_ID >= ? "
				+ " ORDER BY CASH_IN_ENC_DATE";

		// aggregateCashIn_getLastEncId()
		final String lastEncIdSelectSQL = "SELECT COALESCE(MAX(CASH_IN_ENCASHMENT_ID),0) "
				+ " FROM T_CM_ENC_CASHIN_STAT " + " WHERE ATM_ID = ?";

		// aggregateCashIn_getNextIncass()
		final String nextIncassSelectSQL = "SELECT COALESCE(MIN(DISTINCT CASH_IN_ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE "
				+ " FROM T_CM_ENC_CASHIN_STAT " + " WHERE ATM_ID = ? " + " AND CASH_IN_ENC_DATE > ?";

		// simpleDeleteCashInQuery()
		final String ciStatDeleteSQL = "DELETE FROM T_CM_CASHIN_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String ciDenomStatDeleteSQL = "DELETE FROM T_CM_CASHIN_DENOM_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String ciRCassStatDeleteSQl = "DELETE FROM T_CM_CASHIN_R_CASS_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		final String ciRCurrStatDeleteSQl = "DELETE FROM T_CM_CASHIN_R_CURR_STAT " + " WHERE " + " ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE >= ?";

		PreparedStatement pstmtLoop = null;
		ResultSet rsLoop = null;

		try {
			pstmtLoop = connection.prepareStatement(transSelectLoopSQL.toString());
			rsLoop = pstmtLoop.executeQuery();
			while (rsLoop.next()) {
				atmIdList.add(Integer.parseInt(rsLoop.getString("PID")));
			}
			JdbcUtils.close(rsLoop);
			JdbcUtils.close(pstmtLoop);
			JdbcUtils.close(connection);

			for (final Integer item : atmIdList) {
				if (worksadded < cores) {
					worklist.add(wm.schedule(new Work() {

						@Override

						public void run() {
							_logger.debug("Thread created");
							Connection connect = null;
							PreparedStatement pstmtLoop2 = null;
							PreparedStatement pstmtSelect = null;
							PreparedStatement pstmtDelete = null;
							ResultSet rsLoop2 = null;
							ResultSet rs = null;
							int lastEncId = 0;
							int encID = 0;
							Date nextIncass = null;

							List<ObjectPair<Integer, Timestamp>> encList = new ArrayList<ObjectPair<Integer, Timestamp>>();
							try {
								connect = session.getConnection();
							} catch (Exception e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							try {
								pstmtSelect = connect.prepareStatement(lastEncIdSelectSQL);
								pstmtSelect.setString(1, String.valueOf(item));
								rs = pstmtSelect.executeQuery();
								if (rs.next()) {
									lastEncId = rs.getInt(1);
								}

								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);

								insertEcnashmentsCashIn(connect, String.valueOf(item), intgr_trans_status,
										enc_cashin_stat_status, enc_cashin_stat_details_status);
								pstmtLoop2 = connect.prepareStatement(ciEncStatSelectLoopSQL);
								pstmtLoop2.setString(1, String.valueOf(item));
								pstmtLoop2.setInt(2, lastEncId);
								rsLoop2 = pstmtLoop2.executeQuery();
								while (rsLoop2.next()) {
									encList.add(new ObjectPair<Integer, Timestamp>(rsLoop2.getInt("ENCASHMENT_ID"),
											rsLoop2.getTimestamp("ENC_DATE")));
								}
								JdbcUtils.close(rsLoop2);
								JdbcUtils.close(pstmtLoop2);
								for (ObjectPair<Integer, Timestamp> objectPair : encList) {
									encID = objectPair.getKey();
									if (encID > lastEncId) {
										pstmtDelete = connect.prepareStatement(ciStatDeleteSQL);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);

										pstmtDelete = connect.prepareStatement(ciDenomStatDeleteSQL);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);

										pstmtDelete = connect.prepareStatement(ciRCassStatDeleteSQl);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);

										pstmtDelete = connect.prepareStatement(ciRCurrStatDeleteSQl);
										pstmtDelete.setString(1, String.valueOf(item));
										pstmtDelete.setInt(2, lastEncId);
										pstmtDelete.setTimestamp(3, objectPair.getValue());
										pstmtDelete.executeUpdate();
										JdbcUtils.close(pstmtDelete);
									}

									try {
										pstmtSelect = connect.prepareStatement(nextIncassSelectSQL);
										pstmtSelect.setString(1, String.valueOf(item));
										pstmtSelect.setTimestamp(2, rsLoop2.getTimestamp("ENC_DATE"));
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											nextIncass = JdbcUtils.getTimestamp(rs.getTimestamp(1));
										}
									} catch (SQLException ex) {
										nextIncass = new Date();// not sure
									} finally {
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);
										JdbcUtils.close(pstmtDelete);
									}
									insertCashInStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_cash_in_status, intgr_trans_status,
											intgr_downtime_cashin_status, cashin_stat_status);
									// -------------------------------------------------
									insertCiEncashmentsPartAndOut(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), enc_cashin_stat_status,
											enc_cashin_stat_details_status, cashin_r_cass_stat_status);
									// Curr stat
									// insert---------------------------------
									insertCashInCurrStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_status, cashin_r_curr_stat_status);
									// Cass stat
									// insert---------------------------------
									insertCashInCassStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_status, intgr_downtime_cashin_status,
											cashin_r_curr_stat_status);
									// Inserting remainings for cass
									// stat------------------
									insertRemainingsForCashInCass(sessionHolder, String.valueOf(item), encID,
											enc_cashin_stat_details_status, cashin_r_cass_stat_status);
									// Filling zero take offs - cash out cass
									// stat------------------
									insertZeroTakeOffsForCICass(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											cashin_r_cass_stat_status, intgr_downtime_cashout_status);
									// Filling zero take offs - cash out curr
									// stat------------------
									insertZeroTakeOffsForCICurr(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											cashin_r_curr_stat_status, cashin_r_cass_stat_status);
									// Inserting remainings for curr
									// stat------------------
									insertRemainingsForCashInCurr(connect, String.valueOf(item), encID,
											cashin_r_cass_stat_status, cashin_r_curr_stat_status);
									insertCashInDenomStat(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_cash_in_status, intgr_trans_status,
											intgr_downtime_cashin_status, cashin_denom_stat_status);
									// Zero days insert-----------------
									insertZeroDaysForCashIn(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											cashin_denom_stat_status, intgr_downtime_cashin_status, cashin_stat_status);
									// Remaining insert-----------------
									insertRemainingsForCashIn(connect, String.valueOf(item), encID, cashin_stat_status);

								}

							} catch (SQLException e) {
								e.printStackTrace();
							}
							JdbcUtils.close(connect);
							_logger.debug("Thread ended");
						}

						@Override

						public void release() {

						}

						@Override

						public boolean isDaemon() {

							return false;

						}
					}));
					worksadded++;
				} else {
					wm.waitForAll(worklist, 25920000);
					worklist.clear();
					worksadded = 0;
				}
			}

			wm.waitForAll(worklist, 25920000);
			worklist.clear();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (WorkException e) {
			e.printStackTrace();
		} finally {
			connection.close();
			session.close();
		}
		_logger.debug("Method aggregate_cash_in executed for atms: " + atmList.toString());
	}

	public static void aggregate_cash_in_multi_disp(final DataSource dataSource) throws SQLException {
		int cores = Runtime.getRuntime().availableProcessors();
		_logger.debug("Cores number = " + cores);
		int worksadded = 0;
		WorkManager wm;
		final Connection connection = dataSource.getConnection();

		final AtomicBoolean intgr_trans_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashin_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_downtime_cashout_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_trans_cash_in_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_denom_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_r_cass_stat_status = new AtomicBoolean(false);
		final AtomicBoolean cashin_r_curr_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashin_stat_status = new AtomicBoolean(false);
		final AtomicBoolean enc_cashin_stat_details_status = new AtomicBoolean(false);
		final AtomicBoolean intgr_trans_md_status = new AtomicBoolean(false);

		wm = getWorkManager();

		List<WorkItem> worklist = new ArrayList<WorkItem>();
		final List<Integer> atmIdList = new ArrayList<Integer>();

		// getStringValueByQuery() rename to result
		String transSelectLoopSQL = "SELECT DISTINCT terminal_id FROM t_cm_intgr_trans_md";

		// aggregateCashIn_getCiEncStat()
		final String ciEncStatSelectLoopSQL = "SELECT CASH_IN_ENC_DATE as ENC_DATE ,CASH_IN_ENCASHMENT_ID as ENCASHMENT_ID "
				+ "FROM T_CM_ENC_CASHIN_STAT " + "WHERE ATM_ID = ? " + "AND CASH_IN_ENCASHMENT_ID >= ? "
				+ "ORDER BY CASH_IN_ENC_DATE";

		// aggregateCashIn_getLastEncId()
		final String lastEncIdSelectSQL = "SELECT COALESCE(MAX(CASH_IN_ENCASHMENT_ID),0) "
				+ "FROM T_CM_ENC_CASHIN_STAT " + "WHERE ATM_ID = ?";

		// aggregateCashIn_getNextIncass()
		final String nextIncassSelectSQL = "SELECT COALESCE(MIN(DISTINCT CASH_IN_ENC_DATE),CURRENT_TIMESTAMP) as ENC_DATE "
				+ " FROM T_CM_ENC_CASHIN_STAT " + " WHERE ATM_ID = ? " + " AND CASH_IN_ENC_DATE > ?";

		PreparedStatement pstmtLoop = null;
		ResultSet rsLoop = null;

		try {
			pstmtLoop = connection.prepareStatement(transSelectLoopSQL);
			rsLoop = pstmtLoop.executeQuery();
			while (rsLoop.next()) {
				atmIdList.add(Integer.parseInt(rsLoop.getString("PID")));
			}
			JdbcUtils.close(rsLoop);
			JdbcUtils.close(pstmtLoop);
			JdbcUtils.close(connection);

			for (final Integer item : atmIdList) {
				if (worksadded < cores) {
					worklist.add(wm.schedule(new Work() {

						@Override

						public void run() {
							_logger.debug("Thread created");
							Connection connect = null;
							PreparedStatement pstmtLoop2 = null;
							PreparedStatement pstmtSelect = null;
							PreparedStatement pstmtDelete = null;
							ResultSet rsLoop2 = null;
							ResultSet rs = null;
							int lastEncId = 0;
							int encID = 0;
							Date nextIncass = null;

							List<ObjectPair<Integer, Timestamp>> encList = new ArrayList<ObjectPair<Integer, Timestamp>>();

							try {
								connect = dataSource.getConnection();
							} catch (SQLException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							try {
								pstmtLoop2 = connect.prepareStatement(ciEncStatSelectLoopSQL);
								pstmtSelect = connect.prepareStatement(lastEncIdSelectSQL);
								pstmtSelect.setString(1, String.valueOf(item));
								rs = pstmtSelect.executeQuery();
								lastEncId = rs.getInt(1);
								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);
								// Encashments insert
								insertEcnashmentsCashInMd(connect, String.valueOf(item), intgr_trans_md_status,
										enc_cashin_stat_status);
								// Stats insert
								pstmtLoop2.setString(1, String.valueOf(item));
								pstmtLoop2.setInt(2, lastEncId);
								rsLoop2 = pstmtLoop2.executeQuery();
								while (rsLoop2.next()) {
									encList.add(new ObjectPair<Integer, Timestamp>(rsLoop2.getInt("ENCASHMENT_ID"),
											rsLoop2.getTimestamp("ENC_DATE")));
								}
								JdbcUtils.close(rsLoop2);
								JdbcUtils.close(pstmtLoop2);
								for (ObjectPair<Integer, Timestamp> objectPair : encList) {
									encID = objectPair.getKey();

									try {
										pstmtSelect = connect.prepareStatement(nextIncassSelectSQL);
										pstmtSelect.setString(1, String.valueOf(item));
										pstmtSelect.setTimestamp(2, objectPair.getValue());
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											nextIncass = JdbcUtils.getTimestamp(rs.getTimestamp(1));
										}
									} catch (SQLException ex) {
										nextIncass = new Date();// not sure
									} finally {
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);
									}

									insertCashInStatMd(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											intgr_trans_md_status, intgr_downtime_cashin_status, cashin_stat_status);
									// Zero days insert-----------------
									insertZeroDaysForCashIn(connect, String.valueOf(item), encID,
											JdbcUtils.getTimestamp(objectPair.getValue()), nextIncass,
											cashin_denom_stat_status, intgr_downtime_cashin_status, cashin_stat_status);
									// Remaining insert-----------------
									insertRemainingsForCashIn(connect, String.valueOf(item), encID, cashin_stat_status);

								}

							} catch (SQLException e) {
								e.printStackTrace();
							}
							JdbcUtils.close(connect);
							_logger.debug("Thread ended");
						}

						@Override

						public void release() {

						}

						@Override

						public boolean isDaemon() {

							return false;

						}
					}));
					worksadded++;
				} else {
					wm.waitForAll(worklist, 25920000);
					worklist.clear();
					worksadded = 0;
				}
			}

			wm.waitForAll(worklist, 25920000);
			worklist.clear();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (WorkException e) {
			e.printStackTrace();
		}
		_logger.debug("Method aggregate_cash_in_multi_disp executed");

	}

	public static void prepare_downtimes(AtomicInteger interruptFlag, ISessionHolder sessionHolder) throws SQLException {
		_logger.debug("Executing prepare_downtimes method");

		double downTime;
		double koeff;
		Date hourToUpdate;

		String downtimePeriodSelectLoopSQL = "select PID,START_DATE,COALESCE(END_DATE, CURRENT_TIMESTAMP) as END_DATE "
				+ "FROM t_cm_intgr_downtime_period " + "WHERE DOWNTIME_TYPE_IND in (" + OFFLINE_DOWNTIME_TYPE + ", ?)";// "+CASHOUT_DOWNTIME_TYPE+"
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Connection connection = session.getConnection();
		PreparedStatement pstmtLoop = null;
		ResultSet rsLoop = null;
		if (!(interruptFlag.get() > 0)) {
			try {
				pstmtLoop = connection.prepareStatement(downtimePeriodSelectLoopSQL);
				pstmtLoop.setInt(1, CASHOUT_DOWNTIME_TYPE);
				rsLoop = pstmtLoop.executeQuery();
				while (rsLoop.next()) {
					hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")),
							Calendar.HOUR_OF_DAY);
					if (DateUtils
							.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")), Calendar.HOUR_OF_DAY)
							.equals(DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
									Calendar.HOUR_OF_DAY))) {
						downTime = (DateUtils
								.toCalendar(DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
										Calendar.HOUR_OF_DAY))
								.getTimeInMillis()
								- DateUtils.toCalendar(
										DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")),
												Calendar.HOUR_OF_DAY))
										.getTimeInMillis())
								/ 86400000;
						koeff = downTime * 24;
						insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
					} else {
						downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")))
								.getTimeInMillis()
								- DateUtils.toCalendar(
										DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")),
												Calendar.HOUR_OF_DAY))
										.getTimeInMillis())
								/ 86400000;
						if (downTime > 0) {
							koeff = 1 - downTime * 24;
							insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
							hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
						}

						while (hourToUpdate.compareTo(DateUtils.truncate(
								JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY)) < 0) {
							koeff = 1;
							insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
							hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
						}
						downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")))
								.getTimeInMillis()
								- DateUtils.toCalendar(DateUtils.truncate(
										JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY))
										.getTimeInMillis())
								/ 86400000;
						if (downTime > 0) {
							koeff = downTime * 24;
							hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
									Calendar.HOUR_OF_DAY);
							insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
						}

					}

				}

				JdbcUtils.close(rsLoop);
				pstmtLoop.clearParameters();
				pstmtLoop = connection.prepareStatement(downtimePeriodSelectLoopSQL);
				pstmtLoop.setInt(1, CASHIN_DOWNTIME_TYPE);
				rsLoop = pstmtLoop.executeQuery();
				while (rsLoop.next()) {
					hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")),
							Calendar.HOUR_OF_DAY);
					if (DateUtils
							.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")), Calendar.HOUR_OF_DAY)
							.equals(DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
									Calendar.HOUR_OF_DAY))) {
						downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")))
								.getTimeInMillis()
								- DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")))
										.getTimeInMillis())
								/ 86400000;
						koeff = downTime * 24;
						insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
					} else {
						downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")))
								.getTimeInMillis()
								- DateUtils.toCalendar(
										DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")),
												Calendar.HOUR_OF_DAY))
										.getTimeInMillis())
								/ 86400000;
						if (downTime > 0) {
							koeff = 1 - downTime * 24;
							insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
							hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
						}
						while (hourToUpdate.compareTo(DateUtils.truncate(
								JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY)) < 0) {
							koeff = 1;
							insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
							hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
						}

						downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")))
								.getTimeInMillis()
								- DateUtils.toCalendar(DateUtils.truncate(
										JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY))
										.getTimeInMillis())
								/ 86400000;
						if (downTime > 0) {
							koeff = downTime * 24;
							hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
									Calendar.HOUR_OF_DAY);
							insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
						}
					}
				}

			} finally {
				JdbcUtils.close(rsLoop);
				JdbcUtils.close(pstmtLoop);
				connection.close();
				session.close();
			}
		} else {

		}

		_logger.debug("Method prepare_downtimes executed");

	}

	public static void prepare_downtimes(Connection connection, List<Integer> atmList) throws SQLException {
		_logger.debug("Executing prepare_downtimes method for atms: " + atmList.toString());
		double downTime;
		double koeff;
		Date hourToUpdate;

		StringBuffer downtimePeriodSelectLoopSQL = new StringBuffer(
				"select PID,START_DATE,COALESCE(END_DATE, CURRENT_TIMESTAMP) as END_DATE "
						+ "FROM t_cm_intgr_downtime_period " + "WHERE DOWNTIME_TYPE_IND in (" + OFFLINE_DOWNTIME_TYPE
						+ ", ?) and (");// "+CASHOUT_DOWNTIME_TYPE+"
		downtimePeriodSelectLoopSQL.append(JdbcUtils.generateInConditionNumber("pid", atmList));
		downtimePeriodSelectLoopSQL.append(") ");
		PreparedStatement pstmtLoop = null;
		ResultSet rsLoop = null;
		try {
			pstmtLoop = connection.prepareStatement(downtimePeriodSelectLoopSQL.toString());
			pstmtLoop.setInt(1, CASHOUT_DOWNTIME_TYPE);
			rsLoop = pstmtLoop.executeQuery();
			while (rsLoop.next()) {
				hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")),
						Calendar.HOUR_OF_DAY);
				if (DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")), Calendar.HOUR_OF_DAY)
						.equals(DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
								Calendar.HOUR_OF_DAY))) {
					downTime = (DateUtils
							.toCalendar(DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
									Calendar.HOUR_OF_DAY))
							.getTimeInMillis()
							- DateUtils.toCalendar(DateUtils.truncate(
									JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")), Calendar.HOUR_OF_DAY))
									.getTimeInMillis())
							/ 86400000;
					koeff = downTime * 24;
					insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
				} else {
					downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")))
							.getTimeInMillis()
							- DateUtils.toCalendar(DateUtils.truncate(
									JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")), Calendar.HOUR_OF_DAY))
									.getTimeInMillis())
							/ 86400000;
					if (downTime > 0) {
						koeff = 1 - downTime * 24;
						insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
						hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
					}

					while (hourToUpdate.compareTo(DateUtils.truncate(
							JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY)) < 0) {
						koeff = 1;
						insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
						hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
					}
					downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")))
							.getTimeInMillis()
							- DateUtils.toCalendar(DateUtils.truncate(
									JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY))
									.getTimeInMillis())
							/ 86400000;
					if (downTime > 0) {
						koeff = downTime * 24;
						hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
								Calendar.HOUR_OF_DAY);
						insert_downtime_stat_cashout(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
					}

				}

			}

			JdbcUtils.close(rsLoop);
			pstmtLoop.clearParameters();
			pstmtLoop = connection.prepareStatement(downtimePeriodSelectLoopSQL.toString());
			pstmtLoop.setInt(1, CASHIN_DOWNTIME_TYPE);
			rsLoop = pstmtLoop.executeQuery();
			while (rsLoop.next()) {
				hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")),
						Calendar.HOUR_OF_DAY);
				if (DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")), Calendar.HOUR_OF_DAY)
						.equals(DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
								Calendar.HOUR_OF_DAY))) {
					downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")))
							.getTimeInMillis()
							- DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")))
									.getTimeInMillis())
							/ 86400000;
					koeff = downTime * 24;
					insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
				} else {
					downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")))
							.getTimeInMillis()
							- DateUtils.toCalendar(DateUtils.truncate(
									JdbcUtils.getTimestamp(rsLoop.getTimestamp("START_DATE")), Calendar.HOUR_OF_DAY))
									.getTimeInMillis())
							/ 86400000;
					if (downTime > 0) {
						koeff = 1 - downTime * 24;
						insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
						hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
					}
					while (hourToUpdate.compareTo(DateUtils.truncate(
							JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY)) < 0) {
						koeff = 1;
						insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
						hourToUpdate = DateUtils.addHours(hourToUpdate, 1);
					}

					downTime = (DateUtils.toCalendar(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")))
							.getTimeInMillis()
							- DateUtils.toCalendar(DateUtils.truncate(
									JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")), Calendar.HOUR_OF_DAY))
									.getTimeInMillis())
							/ 86400000;
					if (downTime > 0) {
						koeff = downTime * 24;
						hourToUpdate = DateUtils.truncate(JdbcUtils.getTimestamp(rsLoop.getTimestamp("END_DATE")),
								Calendar.HOUR_OF_DAY);
						insert_downtime_stat_cashin(connection, rsLoop.getString("PID"), hourToUpdate, koeff);
					}
				}
			}

		} finally {
			JdbcUtils.close(rsLoop);
			JdbcUtils.close(pstmtLoop);
		}

		_logger.debug("Method prepare_downtimes executed for atms: " + atmList.toString());

	}

	protected static int checkEncashmentCashAdd(Connection connection, int pEncId) throws SQLException {
		PreparedStatement pstmtCheck = null;
		ResultSet rs = null;

		String sqlCheck = "SELECT CASH_ADD_ENCASHMENT " + "FROM T_CM_ENC_CASHOUT_STAT " + "WHERE ENCASHMENT_ID = ?";
		int checkEncashmentCashAdd = 0;
		try {
			pstmtCheck = connection.prepareStatement(sqlCheck);
			pstmtCheck.setInt(1, pEncId);
			rs = pstmtCheck.executeQuery();

			if (rs.next()) {
				checkEncashmentCashAdd = rs.getInt("CASH_ADD_ENCASHMENT");
			}

		} finally {
			JdbcUtils.close(pstmtCheck);
		}
		return checkEncashmentCashAdd;
	}

	protected static int checkEncashmentCashInPreload(ISessionHolder sessionHolder, int pEncId) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());

		int checkEncashmentCashInPreload = 0;
		try {
			AggregationMapper mapper = session.getMapper(getMapperClass());
			checkEncashmentCashInPreload = ORMUtils
					.getSingleValue(mapper.checkEncashmentCashInPreload(pEncId, CO_ENC_DET_LOADED), 0);
		} finally {
			session.close();
		}
		return checkEncashmentCashInPreload;
	}

	public static void insertEncashments(ISessionHolder sessionHolder, String pPid, AtomicBoolean table1,
			AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4, AtomicBoolean table5,
			AtomicBoolean table6) throws SQLException {
		_logger.debug("Executing insertEncashments method for PID: " + pPid);
		Boolean method_status = false;
		Date dateIncass = null;
		Date currentDate;
		int cashAdd;
		Integer encId = null;

		SqlSession session = sessionHolder.getSession(getMapperClass());

		String selectSeqSQL = "select " + ORMUtils.getNextSequence(session, "s_cm_enc_stat_id") + " "
				+ ORMUtils.getFromDummyExpression(session);

		while (method_status == false) {
			if (table1.get() == false & table3.get() == false & table4.get() == false & table5.get() == false
					& table6.get() == false) {
				try {
					AggregationMapper mapper = session.getMapper(getMapperClass());
					table2.set(true);
					table3.set(true);
					table4.set(true);
					table5.set(true);
					table6.set(true);
					List<MultiObject<Integer, Integer, Integer, Integer, Integer, Timestamp, ?, ?, ?, ?>> result = mapper
							.insertEncashments_check(pPid);
					for (MultiObject<Integer, Integer, Integer, Integer, Integer, Timestamp, ?, ?, ?, ?> item : result) {
						currentDate = JdbcUtils.getTimestamp(item.getSixth());
						cashAdd = item.getFirst();
						ObjectPair<Integer, Timestamp> pairs = ORMUtils.getSingleValue(
								mapper.insertEncashments_select(pPid, JdbcUtils.getSqlTimestamp(currentDate)));

						if (dateIncass == null) {
							if (pairs != null) {
								dateIncass = JdbcUtils.getTimestamp(pairs.getValue());
								encId = pairs.getKey();
							} else {
								dateIncass = currentDate;

								encId = ORMUtils.getSingleValue(mapper.getIntegerValues(selectSeqSQL), 0);

								_logger.debug("Inserting encashment:");
								_logger.debug("pPid:" + pPid);
								_logger.debug("encId:" + encId);
								if (dateIncass != null) {
									_logger.debug("dateIncass:" + dateIncass.toString());
								} else {
									_logger.debug("dateIncass: null");
								}

								mapper.insertEncashments_insertStat(pPid, encId, JdbcUtils.getSqlTimestamp(dateIncass));
							}
						}

						if (Math.abs(JdbcUtils.getSqlTimestamp(dateIncass).getTime()
								- JdbcUtils.getSqlTimestamp(currentDate).getTime()) > 15 * 60000) {
							dateIncass = currentDate;

							encId = ORMUtils.getSingleValue(mapper.getIntegerValues(selectSeqSQL), 0);

							_logger.debug("Inserting encashment:");
							_logger.debug("pPid:" + pPid);
							_logger.debug("encId:" + encId);
							if (dateIncass != null) {
								_logger.debug("dateIncass:" + dateIncass.toString());
							} else {
								_logger.debug("dateIncass: null");
							}
							_logger.debug("cashAdd: " + cashAdd);
							mapper.insertEncashments_insertStatFull(pPid, encId, JdbcUtils.getSqlTimestamp(dateIncass),
									cashAdd);
						}

						try {
							mapper.insertEncashments_insertDetails(encId, item.getFourth(), item.getThird(),
									item.getSecond(), CO_ENC_DET_LOADED, item.getFifth());
						} catch (Exception ex) {
							mapper.insertEncashments_updateDetails(encId, item.getFourth(), item.getThird(),
									item.getSecond(), CO_ENC_DET_LOADED, item.getFifth());
						}
					}
				} finally {
					table2.set(false);
					table3.set(false);
					table4.set(false);
					table5.set(false);
					table6.set(false);
					session.close();
				}
				method_status = true;
				_logger.debug("Method insertEncashments executed for PID: " + pPid);
			} else {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertEncashmentsMd(Connection connection, String pPid, AtomicBoolean table1,
			AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4) throws SQLException {
		_logger.debug("Executing insertEncashments method for PID: " + pPid);
		Boolean method_status = false;
		Date dateIncass = null;
		Integer encId = null;
		PreparedStatement pstmtCheck = null;
		PreparedStatement pstmtSeq = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtDetailsInsert = null;
		ResultSet rsCheck = null;
		ResultSet rs = null;
		int recordsCount = 0;
		String CheckSQL = "select sum(bills) as bills,truncToMinute(d) as d , curr,denom, min(d) as enc_date,cass_num, "
				+ " CASE " + " WHEN trans_type_ind = " + CO_719_ENC_TRANSACTION_TYPE + " THEN 0 "
				+ " WHEN trans_type_ind = " + CO_743_ENC_TRANSACTION_TYPE + " THEN 0 " + " WHEN trans_type_ind = "
				+ CO_CA_ENC_TRANSACTION_TYPE + " THEN 1 " + " ELSE 0 END as cash_add_encashment " + " FROM( "
				+ " select itmd.note_dispensed as BILLS,DATETIME as d,itmd.face as denom, itmd.currency as CURR,itmd.disp_number as CASS_NUM,oper_type as trans_type_ind "
				+ " from t_cm_intgr_trans_md itm "
				+ " join t_cm_intgr_trans_md_disp itmd on (itm.oper_id = itmd.oper_id) " + " where oper_type in( "
				+ CO_719_ENC_TRANSACTION_TYPE + ", " + CO_743_ENC_TRANSACTION_TYPE + ", " + CO_CA_ENC_TRANSACTION_TYPE
				+ ") " + " and itm.terminal_id = ? " + " and itmd.note_dispensed > 0 " + " order by d "
				+ " )GROUP BY truncToMinute(d),curr,denom,cass_num " +

				" ORDER BY ENC_DATE,CASS_NUM ";

		// getIntegerValueByQuery() (as result TODO)
		String selectSeqSQL = "select " + JdbcUtils.getNextSequence(connection, "s_cm_enc_stat_id") + " "
				+ JdbcUtils.getFromDummyExpression(connection);

		String sqlStatFullInsert = "INSERT INTO "
				+ "T_CM_ENC_CASHOUT_STAT(ATM_ID,ENCASHMENT_ID,ENC_DATE,CASH_ADD_ENCASHMENT) " + "VALUES(?, ?, ?, ?)";

		String sqlDetailsInsert = "INSERT INTO "
				+ "t_cm_enc_cashout_stat_details(ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) "
				+ "VALUES(?, ?, ?, ?, ?, ?)";

		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false & table4.get() == false) {
				try {
					table1.set(true);
					table2.set(true);
					table3.set(true);
					table4.set(true);

					pstmtCheck = connection.prepareStatement(CheckSQL);
					pstmtCheck.setString(1, pPid);
					rsCheck = pstmtCheck.executeQuery();

					pstmtInsert = connection.prepareStatement(sqlStatFullInsert);
					pstmtDetailsInsert = connection.prepareStatement(sqlDetailsInsert);
					while (rsCheck.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtDetailsInsert.executeBatch();
							pstmtDetailsInsert.clearBatch();
							recordsCount = 0;

						}
						if (dateIncass == null) {
							dateIncass = JdbcUtils.getTimestamp(rsCheck.getTimestamp("ENC_DATE"));

							pstmtSeq = connection.prepareStatement(selectSeqSQL);
							rs = pstmtSeq.executeQuery();
							rs.next();
							encId = rs.getInt(1);
							JdbcUtils.close(rs);
							JdbcUtils.close(pstmtSeq);

							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setInt(2, encId);
							pstmtInsert.setTimestamp(3, JdbcUtils.getSqlTimestamp(dateIncass));
							pstmtInsert.setInt(4, rsCheck.getInt("cash_add_encashment"));

							pstmtInsert.addBatch();
						}
						if (dateIncass.getTime() - rsCheck.getTimestamp("ENC_DATE").getTime() > 5 * 60000) {
							dateIncass = JdbcUtils.getTimestamp(rsCheck.getTimestamp("ENC_DATE"));

							pstmtSeq = connection.prepareStatement(selectSeqSQL);
							rs = pstmtSeq.executeQuery();
							rs.next();
							encId = rs.getInt(1);
							JdbcUtils.close(rs);
							JdbcUtils.close(pstmtSeq);

							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setInt(2, encId);
							pstmtInsert.setTimestamp(3, JdbcUtils.getSqlTimestamp(dateIncass));
							pstmtInsert.setNull(4, java.sql.Types.INTEGER);

							pstmtInsert.addBatch();
						}
						pstmtDetailsInsert.clearParameters();
						pstmtDetailsInsert.setInt(1, encId);
						pstmtDetailsInsert.setInt(2, rsCheck.getInt("denom"));
						pstmtDetailsInsert.setInt(3, rsCheck.getInt("curr"));
						pstmtDetailsInsert.setInt(4, rsCheck.getInt("bills"));
						pstmtDetailsInsert.setInt(5, CO_ENC_DET_LOADED);
						pstmtDetailsInsert.setInt(6, rsCheck.getInt("cass_num"));

						pstmtDetailsInsert.addBatch();
						recordsCount++;
					}

					if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
						pstmtInsert.executeBatch();
						pstmtDetailsInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtDetailsInsert);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtCheck);
				}
				method_status = true;
				_logger.debug("Method insertEncashmentsMD executed for PID: " + pPid);
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCassStat(Connection connection, String pPid, int pEncId, Date pEncDate, Date pNextEncDate,
			AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4)
			throws SQLException {
		_logger.debug("Executing insertCassStat method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		Boolean method_status = false;
		PreparedStatement pstmtCheck = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsCheck = null;
		String CheckSQL = "select  cs.PID as PID, cs.BILLS,cs.trans_count,cs.trans_date , cs.denom, cs.CURR,cs.CASS_NUM, "
				+ " COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF " + " FROM ( "
				+ " select ? as PID,sum(BILLS) as BILLS,count(1) as trans_count,truncToHour(d) as trans_date , denom, CURR, CASS_NUM "
				+ " FROM( "
				+ " select BILL_CASS1 as BILLS,DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR,1 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS1 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS1 = " + CASS_TYPE_CASH_OUT + " union all "
				+ " select BILL_CASS2 as BILLS,DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR,2 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS2 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS2 = " + CASS_TYPE_CASH_OUT + " union all "
				+ " select BILL_CASS3 as BILLS,DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR,3 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS3 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS3 = " + CASS_TYPE_CASH_OUT + " union all "
				+ " select BILL_CASS4 as BILLS,DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR,4 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS4 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS4 = " + CASS_TYPE_CASH_OUT + " union all "
				+ " select BILL_CASS5 as BILLS,DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR,5 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS5 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS5 = " + CASS_TYPE_CASH_OUT + " union all "
				+ " select BILL_CASS6 as BILLS,DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR,6 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS6 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS6 = " + CASS_TYPE_CASH_OUT + " union all "
				+ " select BILL_CASS7 as BILLS,DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR,7 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS7 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS7 = " + CASS_TYPE_CASH_OUT + " union all "
				+ " select BILL_CASS8 as BILLS,DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR,8 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE + " ,"
				+ DEBIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS8 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS8 = " + CASS_TYPE_CASH_OUT
				+ " )GROUP BY truncToHour(d),denom, CURR,CASS_NUM " + " ORDER BY trans_date,CASS_NUM) cs "
				+ " left outer join t_cm_intgr_downtime_cashout ds on (cs.PID = ds.PID and cs.trans_date = ds.stat_date)";

		String sqlInsert = "INSERT INTO "
				+ "T_CM_CASHOUT_CASS_STAT(ATM_ID,STAT_DATE,ENCASHMENT_ID,CASS_VALUE,CASS_COUNT,CASS_TRANS_COUNT,CASS_CURR,CASS_NUMBER,AVAIL_COEFF) "
				+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String sqlUpdate = "UPDATE T_CM_CASHOUT_CASS_STAT " + " SET CASS_COUNT = CASS_COUNT + ?, "
				+ " CASS_TRANS_COUNT = CASS_TRANS_COUNT + ?, " + " AVAIL_COEFF = ? " + "WHERE " + "ATM_ID = ? " + "AND "
				+ "STAT_DATE = ? " + "AND " + "ENCASHMENT_ID = ? " + "AND " + "CASS_NUMBER = ? " + "AND "
				+ "CASS_VALUE =  ? " + "AND " + "CASS_CURR = ?";

		while (method_status == false) {
			if (table1.get() == false & table3.get() == false & table4.get() == false) {
				try {
					table2.set(true);
					table3.set(true);
					table4.set(true);
					pstmtCheck = connection.prepareStatement(CheckSQL);
					pstmtCheck.setString(1, pPid);
					pstmtCheck.setString(2, pPid);
					pstmtCheck.setTimestamp(3, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(4, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(5, pPid);
					pstmtCheck.setTimestamp(6, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(7, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(8, pPid);
					pstmtCheck.setTimestamp(9, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(10, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(11, pPid);
					pstmtCheck.setTimestamp(12, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(13, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(14, pPid);
					pstmtCheck.setTimestamp(15, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(16, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(17, pPid);
					pstmtCheck.setTimestamp(18, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(19, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(20, pPid);
					pstmtCheck.setTimestamp(21, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(22, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(23, pPid);
					pstmtCheck.setTimestamp(24, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(25, JdbcUtils.getSqlTimestamp(pNextEncDate));
					rsCheck = pstmtCheck.executeQuery();
					while (rsCheck.next()) {

						pstmtInsert = connection.prepareStatement(sqlInsert);

						pstmtInsert.setString(1, pPid);
						pstmtInsert.setTimestamp(2, rsCheck.getTimestamp("trans_date"));
						pstmtInsert.setInt(3, pEncId);
						pstmtInsert.setInt(4, rsCheck.getInt("denom"));
						pstmtInsert.setInt(5, rsCheck.getInt("bills"));
						pstmtInsert.setInt(6, rsCheck.getInt("TRANS_COUNT"));
						pstmtInsert.setInt(7, rsCheck.getInt("curr"));
						pstmtInsert.setInt(8, rsCheck.getInt("cass_num"));
						pstmtInsert.setInt(9, rsCheck.getInt("AVAIL_COEFF"));

						try {
							pstmtInsert.executeUpdate();
						} catch (SQLException ex) {

							if (ex.getErrorCode() == JdbcUtils.getDuplicateValueErrorCode(connection)) {
								JdbcUtils.close(pstmtInsert);
								pstmtUpdate = connection.prepareStatement(sqlUpdate);

								pstmtUpdate.setInt(1, rsCheck.getInt("bills"));
								pstmtUpdate.setInt(2, rsCheck.getInt("TRANS_COUNT"));
								pstmtUpdate.setInt(3, rsCheck.getInt("AVAIL_COEFF"));
								pstmtUpdate.setString(4, pPid);
								pstmtUpdate.setTimestamp(5, rsCheck.getTimestamp("TRANS_DATE"));
								pstmtUpdate.setInt(6, pEncId);
								pstmtUpdate.setInt(7, rsCheck.getInt("cass_num"));
								pstmtUpdate.setInt(8, rsCheck.getInt("denom"));
								pstmtUpdate.setInt(9, rsCheck.getInt("curr"));

								pstmtUpdate.executeUpdate();
							} else {
								throw ex;
							}
						} finally {
							JdbcUtils.close(pstmtInsert);
							JdbcUtils.close(pstmtUpdate);
						}
					}
				} finally {
					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
				}
				method_status = true;
				_logger.debug("Method insertCassStat  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCassStatMd(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3) throws SQLException {
		_logger.debug("Executing insertCassStatMd method for PID: " + pPid);
		Boolean method_status = false;
		PreparedStatement pstmtCheck = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsCheck = null;
		String CheckSQL = "select  cs.PID as PID, cs.BILLS,cs.trans_count,cs.trans_date , cs.denom, cs.CURR,cs.CASS_NUM, "
				+ " COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF, cs.BILLS_REMAINING " + " FROM ( "
				+ " select ? as PID,sum(itmd.note_dispensed) as BILLS, min(itmd.note_remained) as BILLS_REMAINING,count(1) as trans_count,truncToHour(itm.datetime) as trans_date , "
				+ " itmd.face as denom, itmd.currency as CURR,itmd.disp_number as CASS_NUM "
				+ " FROM t_cm_intgr_trans_md itm "
				+ " join t_cm_intgr_trans_md_disp itmd on (itm.oper_id = itmd.oper_id)" + " where oper_type in ("
				+ EXCHANGE_TRANSACTION_TYPE + "," + DEBIT_TRANSACTION_TYPE + ") " + "and itm.terminal_id = ? "
				+ "and itmd.note_dispensed > 0 " + "and itm.datetime between ? and ? "
				+ "GROUP BY truncToHour(itm.datetime),itmd.face, itmd.currency, itmd.disp_number "
				+ "ORDER BY trans_date,CASS_NUM) cs "
				+ "left outer join t_cm_intgr_downtime_cashout ds on (cs.PID = ds.PID and cs.trans_date = ds.stat_date) ";

		String sqlInsert = "INSERT INTO "
				+ "T_CM_CASHOUT_CASS_STAT(ATM_ID,STAT_DATE,ENCASHMENT_ID,CASS_VALUE,CASS_COUNT,CASS_TRANS_COUNT,CASS_CURR,CASS_NUMBER,AVAIL_COEFF,CASS_REMAINING) "
				+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String sqlUpdate = "UPDATE T_CM_CASHOUT_CASS_STAT " + " SET CASS_COUNT = CASS_COUNT + ?, "
				+ " CASS_TRANS_COUNT = CASS_TRANS_COUNT + ?, " + " AVAIL_COEFF = ?, " + " CASS_REMAINING = ? "
				+ "WHERE " + "ATM_ID = ? " + "AND " + "STAT_DATE = ? " + "AND " + "ENCASHMENT_ID = ? " + "AND "
				+ "CASS_NUMBER = ? " + "AND " + "CASS_VALUE =  ? " + "AND " + "CASS_CURR = ?";

		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {
				try {
					table1.set(true);
					table2.set(true);
					table3.set(true);
					pstmtCheck = connection.prepareStatement(CheckSQL);
					pstmtCheck.setString(1, pPid);
					pstmtCheck.setString(2, pPid);
					pstmtCheck.setTimestamp(3, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(4, JdbcUtils.getSqlTimestamp(pNextEncDate));
					rsCheck = pstmtCheck.executeQuery();
					while (rsCheck.next()) {

						pstmtInsert = connection.prepareStatement(sqlInsert);

						pstmtInsert.setString(1, pPid);
						pstmtInsert.setTimestamp(2, rsCheck.getTimestamp("trans_date"));
						pstmtInsert.setInt(3, pEncId);
						pstmtInsert.setInt(4, rsCheck.getInt("denom"));
						pstmtInsert.setInt(5, rsCheck.getInt("bills"));
						pstmtInsert.setInt(6, rsCheck.getInt("TRANS_COUNT"));
						pstmtInsert.setInt(7, rsCheck.getInt("curr"));
						pstmtInsert.setInt(8, rsCheck.getInt("cass_num"));
						pstmtInsert.setInt(9, rsCheck.getInt("AVAIL_COEFF"));
						pstmtInsert.setInt(10, rsCheck.getInt("BILLS_REMAINING"));

						try {
							pstmtInsert.executeUpdate();
						} catch (SQLException ex) {

							if (ex.getErrorCode() == JdbcUtils.getDuplicateValueErrorCode(connection)) {
								JdbcUtils.close(pstmtInsert);
								pstmtUpdate = connection.prepareStatement(sqlUpdate);

								pstmtUpdate.setInt(1, rsCheck.getInt("bills"));
								pstmtUpdate.setInt(2, rsCheck.getInt("TRANS_COUNT"));
								pstmtUpdate.setInt(3, rsCheck.getInt("AVAIL_COEFF"));
								pstmtUpdate.setInt(4, rsCheck.getInt("BILLS_REMAINING"));
								pstmtUpdate.setString(5, pPid);
								pstmtUpdate.setTimestamp(6, rsCheck.getTimestamp("TRANS_DATE"));
								pstmtUpdate.setInt(7, pEncId);
								pstmtUpdate.setInt(8, rsCheck.getInt("cass_num"));
								pstmtUpdate.setInt(9, rsCheck.getInt("denom"));
								pstmtUpdate.setInt(10, rsCheck.getInt("curr"));

								pstmtUpdate.executeUpdate();

								JdbcUtils.close(pstmtUpdate);
							} else {
								throw ex;
							}
						} finally {
							JdbcUtils.close(pstmtInsert);

						}
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
				}
				method_status = true;
				_logger.debug("Method insertEncashments executed for PID: " + pPid);
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCurrStat(ISessionHolder sessionHolder, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4)
			throws SQLException {
		_logger.debug("Executing insertCurrStat method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());

		Boolean method_status = false;
		SqlSession session = sessionHolder.getSession(getMapperClass());

		while (method_status == false) {

			if (table1.get() == false & table3.get() == false & table4.get() == false) {
				try {
					AggregationMapper mapper = session.getMapper(getMapperClass());
					table2.set(true);
					table3.set(true);
					table4.set(true);

					List<MultiObject<Timestamp, Integer, Integer, Integer, ?, ?, ?, ?, ?, ?>> result = mapper
							.insertCurrStat_check(pPid, JdbcUtils.getSqlTimestamp(pEncDate),
									JdbcUtils.getSqlTimestamp(pNextEncDate));

					for (MultiObject<Timestamp, Integer, Integer, Integer, ?, ?, ?, ?, ?, ?> item : result) {

						try {
							mapper.insertCurrStat_insert(pPid, item.getFirst(), pEncId, item.getSecond(),
									item.getThird(), item.getFourth());
						} catch (Exception ex) {
							mapper.insertCurrStat_update(pPid, item.getFirst(), pEncId, item.getSecond(),
									item.getThird(), item.getFourth());
						}
					}
				} finally {
					table2.set(false);
					table3.set(false);
					table4.set(false);
					session.close();
				}
				method_status = true;
				_logger.debug("Method insertCurrStat  for PID: " + pPid + " executed");
			} else {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void insertCurrStatMd(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3) throws SQLException {
		_logger.debug("Executing insertCurrStatMD method for PID: " + pPid + " with parameters:");
		Boolean method_status = false;
		PreparedStatement pstmtCheck = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsCheck = null;
		String CheckSQL = "SELECT sum(itmd.note_dispensed*face) as summ, truncToHour(itm.datetime) as trans_date, itmd.currency as CURR, "
				+ " count(distinct itm.oper_id) as curr_trans_count " + " from t_cm_intgr_trans_md itm "
				+ " join t_cm_intgr_trans_md_disp itmd on (itm.oper_id = itmd.oper_id) " + " where oper_type in ("
				+ EXCHANGE_TRANSACTION_TYPE + ", " + DEBIT_TRANSACTION_TYPE + ") " + " and itm.terminal_id = ? "
				+ " and itmd.note_dispensed > 0 " + " and itm.datetime between ? and ? "
				+ " GROUP BY truncToHour(itm.datetime), itmd.currency " + " ORDER BY trans_date";
		// insertCurrStat_insert()
		String sqlInsert = "INSERT INTO "
				+ "T_CM_CASHOUT_CURR_STAT(ATM_ID,STAT_DATE,ENCASHMENT_ID,CURR_CODE,CURR_SUMM,CURR_TRANS_COUNT) "
				+ "VALUES(?, ?, ?, ?, ?, ?)";
		// insertCurrStat_update()
		String sqlUpdate = "UPDATE T_CM_CASHOUT_CURR_STAT " + " SET CURR_SUMM = CURR_SUMM + ?, "
				+ " CURR_TRANS_COUNT = CURR_TRANS_COUNT + ? " + "WHERE " + "ATM_ID = ? " + "AND " + "STAT_DATE = ? "
				+ "AND " + "ENCASHMENT_ID = ? " + "AND " + "CURR_CODE = ? ";
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {
				try {
					table1.set(true);
					table2.set(true);
					table3.set(true);
					pstmtCheck = connection.prepareStatement(CheckSQL);
					pstmtCheck.setString(1, pPid);
					pstmtCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(3, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(4, pPid);
					pstmtCheck.setTimestamp(5, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(6, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(7, pPid);
					pstmtCheck.setTimestamp(8, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(9, JdbcUtils.getSqlTimestamp(pNextEncDate));
					pstmtCheck.setString(10, pPid);
					pstmtCheck.setTimestamp(11, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtCheck.setTimestamp(12, JdbcUtils.getSqlTimestamp(pNextEncDate));
					rsCheck = pstmtCheck.executeQuery();
					while (rsCheck.next()) {

						pstmtInsert = connection.prepareStatement(sqlInsert);

						pstmtInsert.setString(1, pPid);
						pstmtInsert.setTimestamp(2, rsCheck.getTimestamp("trans_date"));
						pstmtInsert.setInt(3, pEncId);
						pstmtInsert.setInt(4, rsCheck.getInt("CURR"));
						pstmtInsert.setInt(5, rsCheck.getInt("SUMM"));
						pstmtInsert.setInt(6, rsCheck.getInt("CURR_TRANS_COUNT"));

						try {
							pstmtInsert.executeUpdate();
						} catch (SQLException ex) {

							if (ex.getErrorCode() == JdbcUtils.getDuplicateValueErrorCode(connection)) {
								JdbcUtils.close(pstmtInsert);
								pstmtUpdate = connection.prepareStatement(sqlUpdate);

								pstmtUpdate.setInt(1, rsCheck.getInt("SUMM"));
								pstmtUpdate.setInt(2, rsCheck.getInt("CURR_TRANS_COUNT"));
								pstmtUpdate.setString(3, pPid);
								pstmtUpdate.setTimestamp(4, rsCheck.getTimestamp("trans_date"));
								pstmtUpdate.setInt(5, pEncId);
								pstmtUpdate.setInt(6, rsCheck.getInt("CURR"));

								pstmtUpdate.executeUpdate();

								JdbcUtils.close(pstmtUpdate);
							} else {
								throw ex;
							}
						} finally {
							JdbcUtils.close(pstmtInsert);

						}
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
				}
				method_status = true;
				_logger.debug("Method insertCurrStatMD for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertEncashmentsPartAndOut(Connection connection, String pPid, int pEncId, Date pEncDate,
			AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4,
			AtomicBoolean table5, AtomicBoolean table6) throws SQLException {
		_logger.debug("Executing insertEncashmentsPartAndOut method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());

		Boolean method_status = false;
		int prevEncId = 0;
		Date prevEncLastStat = null;
		int cassValue = 0;
		int cassCurr = 0;
		int remaining = 0;
		int unloadType = 0;

		// int recordsCount;

		// insertEncashmentsPartAndOut_getPrevEnc()
		String prevEncSQL = "SELECT COALESCE(MAX(ENCASHMENT_ID),0) " + " FROM T_CM_ENC_CASHOUT_STAT "
				+ " WHERE ATM_ID = ? " + " AND ENCASHMENT_ID < ?";
		String prevEncLastStatSQL = "SELECT COALESCE(MAX(STAT_DATE),?) " + " FROM T_CM_CASHOUT_CASS_STAT "
				+ " WHERE ATM_ID = ? " + " AND ENCASHMENT_ID < ?";
		String unloadTypeSQL = "SELECT " + " CASE " + " WHEN CASH_ADD_ENCASHMENT > 0 THEN " + CO_ENC_DET_NOT_UNLOADED_CA
				+ " " + " ELSE " + CO_ENC_DET_UNLOADED + " " + " END " + " FROM T_CM_ENC_CASHOUT_STAT "
				+ " WHERE ENCASHMENT_ID = ?";

		String detailsLoopSQL = "select CASS_NUMBER from t_cm_enc_cashout_stat_Details " + " where encashment_id = ? "
				+ " and not exists " + " (select CASS_NUMBER from t_cm_enc_cashout_stat_Details "
				+ " where encashment_id = ?) ";

		String prevEncDetailsLoopSQL = "select CASS_NUMBER from t_cm_enc_cashout_stat_Details "
				+ " where encashment_id = ? ";

		String cassStatSQL = "SELECT CASS_REMAINING,CASS_VALUE,CASS_CURR " + " FROM T_CM_CASHOUT_CASS_STAT "
				+ " WHERE ATM_ID = ? " + " AND ENCASHMENT_ID = ? " + " AND STAT_DATE = ? " + " AND CASS_NUMBER = ? ";

		String insertSQL = " INSERT INTO T_CM_ENC_CASHOUT_STAT_details "
				+ " (ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) " + " VALUES "
				+ " (?, ?, ?, ?, ?, ?) ";

		String checkInsertSQL = " select ENCASHMENT_ID from T_CM_ENC_CASHOUT_STAT_details "
				+ " where ENCASHMENT_ID=? and CASS_NUMBER=? and ACTION_TYPE=?";

		String deleteSQL = "delete from t_cm_enc_cashout_stat_details ecs " + " where ecs.encashment_id = ? and "
				+ " ecs.ACTION_TYPE = " + CO_ENC_DET_NOT_UNLOADED + " and exists  "
				+ " (select null from t_cm_enc_cashout_stat_details ecsd  "
				+ " where ecsd.ENCASHMENT_ID = ecs.ENCASHMENT_ID  " + " and ecsd.CASS_NUMBER = ecs.CASS_NUMBER  "
				+ " and ecsd.ACTION_TYPE = " + CO_ENC_DET_LOADED + ")";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtDelete = null;
		ResultSet rsCheck = null;
		ResultSet rs = null;
		ResultSet rsInsertCheck = null;

		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false & table5.get() == false) {
				try {
					table1.set(true);
					table2.set(true);
					table4.set(true);
					table6.set(true);
					pstmtSelect = connection.prepareStatement(prevEncSQL);
					pstmtSelect.setString(1, pPid);
					pstmtSelect.setInt(2, pEncId);
					rs = pstmtSelect.executeQuery();
					if (rs.next()) {
						prevEncId = rs.getInt(1);
					}
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtSelect);

					pstmtSelect = connection.prepareStatement(prevEncLastStatSQL);
					pstmtSelect.setTimestamp(1, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtSelect.setString(2, pPid);
					pstmtSelect.setInt(3, pEncId);
					rs = pstmtSelect.executeQuery();
					if (rs.next()) {
						prevEncLastStat = JdbcUtils.getTimestamp(rs.getTimestamp(1));
					}
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtSelect);

					pstmtSelect = connection.prepareStatement(unloadTypeSQL);
					pstmtSelect.setInt(1, pEncId);
					rs = pstmtSelect.executeQuery();
					if (rs.next()) {
						unloadType = rs.getInt(1);
					}
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtSelect);

					pstmtLoop = connection.prepareStatement(detailsLoopSQL);
					pstmtLoop.setInt(1, prevEncId);
					pstmtLoop.setInt(2, pEncId);

					rs = pstmtLoop.executeQuery();

					// recordsCount = 0;

					while (rs.next()) {

						pstmtSelect = connection.prepareStatement(cassStatSQL);
						pstmtSelect.setString(1, pPid);
						pstmtSelect.setInt(2, prevEncId);
						pstmtSelect.setTimestamp(3, JdbcUtils.getSqlTimestamp(prevEncLastStat));
						pstmtSelect.setInt(4, rs.getInt("CASS_NUMBER"));
						rsCheck = pstmtSelect.executeQuery();
						if (rsCheck.next()) {
							remaining = rsCheck.getInt("CASS_REMAINING");
							cassValue = rsCheck.getInt("CASS_VALUE");
							cassCurr = rsCheck.getInt("CASS_CURR");
						}
						JdbcUtils.close(rsCheck);
						JdbcUtils.close(pstmtSelect);

						pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);
						pstmtInsertCheck.setInt(1, pEncId);
						pstmtInsertCheck.setInt(2, rs.getInt("CASS_NUMBER"));
						// pstmtInsertCheck.setInt(3, cassCurr);
						pstmtInsertCheck.setInt(3, CO_ENC_DET_NOT_UNLOADED);
						rsInsertCheck = pstmtInsertCheck.executeQuery();
						if (rsInsertCheck.next()) {

						} else {
							pstmtInsert = connection.prepareStatement(insertSQL);
							pstmtInsert.setInt(1, pEncId);
							pstmtInsert.setInt(2, cassValue);
							pstmtInsert.setInt(3, cassCurr);
							pstmtInsert.setInt(4, remaining);
							pstmtInsert.setInt(5, CO_ENC_DET_NOT_UNLOADED);
							pstmtInsert.setInt(6, rs.getInt("CASS_NUMBER"));

							pstmtInsert.executeUpdate();

						}
						JdbcUtils.close(pstmtInsert);
						JdbcUtils.close(rsInsertCheck);
						JdbcUtils.close(pstmtInsertCheck);

					}

					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtLoop);

					pstmtLoop = connection.prepareStatement(prevEncDetailsLoopSQL);
					pstmtLoop.setInt(1, prevEncId);
					rs = pstmtLoop.executeQuery();
					// recordsCount=0;

					while (rs.next()) {

						pstmtSelect = connection.prepareStatement(cassStatSQL);
						pstmtSelect.setString(1, pPid);
						pstmtSelect.setInt(2, prevEncId);
						pstmtSelect.setTimestamp(3, JdbcUtils.getSqlTimestamp(prevEncLastStat));
						pstmtSelect.setInt(4, rs.getInt("CASS_NUMBER"));
						rsCheck = pstmtSelect.executeQuery();
						if (rsCheck.next()) {
							remaining = rsCheck.getInt("CASS_REMAINING");
							cassValue = rsCheck.getInt("CASS_VALUE");
							cassCurr = rsCheck.getInt("CASS_CURR");
						}
						JdbcUtils.close(rsCheck);
						JdbcUtils.close(pstmtSelect);

						pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);
						pstmtInsertCheck.setInt(1, pEncId);
						pstmtInsertCheck.setInt(2, rs.getInt("CASS_NUMBER"));
						// pstmtInsertCheck.setInt(3, cassCurr);
						pstmtInsertCheck.setInt(3, unloadType);
						rsInsertCheck = pstmtInsertCheck.executeQuery();
						if (rsInsertCheck.next()) {

						} else {
							pstmtInsert = connection.prepareStatement(insertSQL);
							pstmtInsert.setInt(1, pEncId);
							pstmtInsert.setInt(2, cassValue);
							pstmtInsert.setInt(3, cassCurr);
							pstmtInsert.setInt(4, remaining);
							pstmtInsert.setInt(5, unloadType);
							pstmtInsert.setInt(6, rs.getInt("CASS_NUMBER"));

							pstmtInsert.executeUpdate();

						}

						JdbcUtils.close(pstmtInsert);
						JdbcUtils.close(rsInsertCheck);
						JdbcUtils.close(pstmtInsertCheck);

					}

					// JdbcUtils.close(pstmtInsert);

					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtLoop);

					pstmtDelete = connection.prepareStatement(deleteSQL);
					pstmtDelete.setInt(1, pEncId);
					pstmtDelete.executeUpdate();
					JdbcUtils.close(pstmtDelete);
				} finally {

					table1.set(false);
					table2.set(false);
					table4.set(false);
					table6.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsInsertCheck);
					JdbcUtils.close(pstmtSelect);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtLoop);
					JdbcUtils.close(pstmtDelete);
				}

				method_status = true;
				_logger.debug("Method insertEncashmentsPartAndOut  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void insertRemainingsForCass(Connection connection, String pPid, int pEncId, Date pEncDate,
			AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4)
			throws SQLException {
		_logger.debug("Executing insertRemainingsForCass method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());

		Boolean method_status = false;
		int remaining = 0;
		Date maxStatDate = null;

		int recordsCount = 0;
		// int maxEncID;
		int cashAddEnc = checkEncashmentCashAdd(connection, pEncId);

		String cassStatLoopSQL = "SELECT DISTINCT CASS_NUMBER " + " FROM T_CM_CASHOUT_CASS_STAT "
				+ " WHERE ENCASHMENT_ID = ?";

		String cassStatLoop2SQL = "SELECT STAT_DATE, CASS_COUNT " + " FROM T_CM_CASHOUT_CASS_STAT "
				+ " WHERE ATM_ID = ? " + " AND ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " ORDER BY STAT_DATE";

		String remainingCashAddSelectSQL = "SELECT CASS_COUNT " + " FROM T_CM_ENC_CASHOUT_STAT_details "
				+ " WHERE ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " AND ACTION_TYPE in (" + CO_ENC_DET_LOADED
				+ ") ";

		String remainingCashAddNotUnloadedSelectSQL = "SELECT sum(CASS_COUNT) " + " FROM T_CM_ENC_CASHOUT_STAT_details "
				+ " WHERE ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " AND ACTION_TYPE in (" + CO_ENC_DET_LOADED
				+ ", " + CO_ENC_DET_NOT_UNLOADED_CA + ")";

		String maxStatDateSQL = "SELECT MAX(STAT_DATE) " + " FROM T_CM_CASHOUT_CASS_STAT " + " WHERE  ATM_ID = ? "
				+ " AND CASS_NUMBER = ? " + " AND ENCASHMENT_ID < ?";
		String remainingmaxStatDateSQL = "SELECT CASS_REMAINING " + " FROM T_CM_CASHOUT_CASS_STAT "
				+ " WHERE STAT_DATE = ? " + " AND ATM_ID = ? " + " AND CASS_NUMBER = ? ";

		String updateSQL = "UPDATE T_CM_CASHOUT_CASS_STAT " + "SET CASS_REMAINING = ? " + "WHERE ATM_ID = ? "
				+ "AND ENCASHMENT_ID = ? " + "AND CASS_NUMBER = ? " + "AND STAT_DATE = ?";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtLoop2 = null;
		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rsLoop2 = null;
		ResultSet rs = null;

		pstmtLoop = connection.prepareStatement(cassStatLoopSQL);
		pstmtLoop.setInt(1, pEncId);

		rsLoop = pstmtLoop.executeQuery();

		while (method_status == false) {
			if (table1.get() == false & table3.get() == false & table4.get() == false) {
				try {

					table2.set(true);
					table3.set(true);
					table4.set(true);
					while (rsLoop.next()) {
						try {
							if (cashAddEnc == 0) {
								pstmtSelect = connection.prepareStatement(remainingCashAddSelectSQL);
								pstmtSelect.setInt(1, pEncId);
								pstmtSelect.setInt(2, rsLoop.getInt("CASS_NUMBER"));

							} else {
								pstmtSelect = connection.prepareStatement(remainingCashAddNotUnloadedSelectSQL);
								pstmtSelect.setInt(1, pEncId);
								pstmtSelect.setInt(2, rsLoop.getInt("CASS_NUMBER"));

							}
							rs = pstmtSelect.executeQuery();
							if (rs.next()) {
								remaining = rs.getInt(1);
							} else {
								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);
								pstmtSelect = connection.prepareStatement(maxStatDateSQL);
								pstmtSelect.setString(1, pPid);
								pstmtSelect.setInt(2, rsLoop.getInt("CASS_NUMBER"));
								pstmtSelect.setInt(3, pEncId);

								rs = pstmtSelect.executeQuery();

								if (rs.next()) {
									maxStatDate = JdbcUtils.getTimestamp(rs.getTimestamp(1));
								}

								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);
								pstmtSelect = connection.prepareStatement(remainingmaxStatDateSQL);
								pstmtSelect.setTimestamp(1, JdbcUtils.getSqlTimestamp(maxStatDate));
								pstmtSelect.setString(2, pPid);
								pstmtSelect.setInt(3, rsLoop.getInt("CASS_NUMBER"));

								rs = pstmtSelect.executeQuery();

								if (rs.next()) {
									remaining = rs.getInt(1);
								}
							}

						} finally {
							JdbcUtils.close(rs);
							JdbcUtils.close(pstmtSelect);
						}
						pstmtLoop2 = connection.prepareStatement(cassStatLoop2SQL);
						pstmtLoop2.setString(1, pPid);
						pstmtLoop2.setInt(2, pEncId);
						pstmtLoop2.setInt(3, rsLoop.getInt("CASS_NUMBER"));

						rsLoop2 = pstmtLoop2.executeQuery();
						pstmtUpdate = connection.prepareStatement(updateSQL);
						recordsCount = 0;
						while (rsLoop2.next()) {
							if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

							} else {
								pstmtUpdate.executeBatch();
								pstmtUpdate.clearBatch();
								recordsCount = 0;
							}
							remaining -= rsLoop2.getInt("CASS_COUNT");
							pstmtUpdate.clearParameters();
							pstmtUpdate.setInt(1, remaining);
							pstmtUpdate.setString(2, pPid);
							pstmtUpdate.setInt(3, pEncId);
							pstmtUpdate.setInt(4, rsLoop.getInt("CASS_NUMBER"));
							pstmtUpdate.setTimestamp(5, JdbcUtils.getSqlTimestamp(rsLoop2.getTimestamp("STAT_DATE")));

							pstmtUpdate.addBatch();
							recordsCount++;
						}
						if (recordsCount > 0) {// && recordsCount <=
												// BATCH_MAX_SIZE
							pstmtUpdate.executeBatch();
						}

						JdbcUtils.close(rsLoop2);
						JdbcUtils.close(pstmtLoop2);
						JdbcUtils.close(pstmtUpdate);

					}

				} finally {

					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(rsLoop2);
					JdbcUtils.close(pstmtLoop2);
					JdbcUtils.close(pstmtUpdate);

					JdbcUtils.close(rsLoop);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertRemainingsForCass  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertRemainingsForCurr(Connection connection, String pPid, int pEncId, AtomicBoolean table1,
			AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4) throws SQLException {
		_logger.debug("Executing insertRemainingsForCurr method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);

		Boolean method_status = false;
		int recordsCount = 0;

		String cassStatLoopSQL = "select stat_date,sum(cass_remaining*cass_value) as CURR_REMAINING,CASS_CURR "
				+ " from T_CM_CASHOUT_CASS_STAT ds " + " where " + " ATM_ID = ? " + " AND ENCASHMENT_ID = ? "
				+ " group by stat_date,cass_curr";

		String updateSQL = "UPDATE T_CM_CASHOUT_CURR_STAT " + "SET CURR_REMAINING = ? " + "WHERE ATM_ID = ? "
				+ "AND ENCASHMENT_ID = ? " + "AND CURR_CODE = ? " + "AND STAT_DATE = ?";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;

		pstmtLoop = connection.prepareStatement(cassStatLoopSQL);
		pstmtLoop.setString(1, pPid);
		pstmtLoop.setInt(2, pEncId);

		rsLoop = pstmtLoop.executeQuery();

		pstmtUpdate = connection.prepareStatement(updateSQL);
		recordsCount = 0;
		while (method_status == false) {
			if (table1.get() == false & table3.get() == false & table4.get() == false) {
				try {

					table2.set(true);
					table3.set(true);
					table4.set(true);
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}
						pstmtUpdate.clearParameters();
						pstmtUpdate.setInt(1, rsLoop.getInt("CURR_REMAINING"));
						pstmtUpdate.setString(2, pPid);
						pstmtUpdate.setInt(3, pEncId);
						pstmtUpdate.setInt(4, rsLoop.getInt("CASS_CURR"));
						pstmtUpdate.setTimestamp(5, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));

						pstmtUpdate.addBatch();
						recordsCount++;
					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
					}

				} finally {

					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(pstmtUpdate);

					JdbcUtils.close(rsLoop);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertRemainingsForCurr  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertZeroTakeOffsForCass(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4,
			AtomicBoolean table5, AtomicBoolean table6) throws SQLException {
		_logger.debug("Executing insertZeroTakeOffsForCass method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());

		Boolean method_status = false;
		int fillStatDays = 0;
		// Date fillStatDate;
		int remaining = 0;
		int cashAddEnc = checkEncashmentCashAdd(connection, pEncId);

		int denomRemaining = 0;
		Date statDate = null;
		double availCoeff;

		// int recordsCount = 0;

		String coStatDetailsLoopSQL = "SELECT distinct CASS_NUMBER,CASS_CURR,CASS_VALUE "
				+ " FROM T_CM_ENC_CASHOUT_STAT_details " + " WHERE ENCASHMENT_ID = ? " + " AND ACTION_TYPE = "
				+ CO_ENC_DET_LOADED;

		String fillStatDateSelectSQL = "SELECT STAT_DATE " + " FROM T_CM_CASHOUT_CASS_STAT " + " WHERE ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " AND STAT_DATE = ?";

		String statDetailsLoadSelectSQL = "SELECT CASS_COUNT " + " FROM T_CM_ENC_CASHOUT_STAT_details "
				+ " WHERE ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " AND ACTION_TYPE in (" + CO_ENC_DET_LOADED
				+ ")";

		String statDetailsLoadUnloadSelectSQL = "SELECT sum(CASS_COUNT) as CASS_COUNT "
				+ " FROM T_CM_ENC_CASHOUT_STAT_details " + " WHERE ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? "
				+ " AND ACTION_TYPE in (" + CO_ENC_DET_LOADED + ", " + CO_ENC_DET_NOT_UNLOADED_CA + ")";

		String downtimeCoSelectSQL = "SELECT COALESCE(ds.AVAIL_COEFF,1) " + "FROM t_cm_intgr_downtime_cashout ds "
				+ "WHERE ds.PID = ? " + "AND ds.STAT_DATE = ?";

		String cassStatSelectSQL = "SELECT CASS_REMAINING,cs.STAT_DATE " + " FROM T_CM_CASHOUT_CASS_STAT cs "
				+ " WHERE ATM_ID = ? " + " AND ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " AND cs.STAT_DATE = ? ";

		String cassStatInsertSQL = "INSERT INTO T_CM_CASHOUT_CASS_STAT "
				+ " (ATM_ID,STAT_DATE,ENCASHMENT_ID,CASS_VALUE,CASS_COUNT,CASS_TRANS_COUNT,CASS_CURR,CASS_REMAINING,CASS_NUMBER,AVAIL_COEFF) "
				+ " VALUES " + " (? ,?, ? , ?, ?, ?, ?, ?, ?, ?)";

		String cassStatInsertCheckSQL = "SELECT * FROM T_CM_CASHOUT_CASS_STAT " + " where " + " ATM_ID = ? "
				+ " AND STAT_DATE = ? " + " AND ENCASHMENT_ID = ? " + " AND CASS_VALUE = ? " + " AND CASS_CURR = ? "
				+ " AND CASS_NUMBER = ?";

		String cassStatUpdateSQL = "UPDATE T_CM_CASHOUT_CASS_STAT " + " SET CASS_COUNT = ?, "
				+ " CASS_TRANS_COUNT = ?, " + " CASS_REMAINING = ?, " + " AVAIL_COEFF = ? " + " where " + " ATM_ID = ? "
				+ " AND STAT_DATE = ? " + " AND ENCASHMENT_ID = ? " + " AND CASS_VALUE = ? " + " AND CASS_CURR = ? "
				+ " AND CASS_NUMBER = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;

		pstmtLoop = connection.prepareStatement(coStatDetailsLoopSQL);
		pstmtLoop.setInt(1, pEncId);
		rsLoop = pstmtLoop.executeQuery();

		pstmtInsert = connection.prepareStatement(cassStatInsertSQL);
		pstmtInsertCheck = connection.prepareStatement(cassStatInsertCheckSQL);
		pstmtUpdate = connection.prepareStatement(cassStatUpdateSQL);
		while (method_status == false) {
			if (table1.get() == false & table3.get() == false & table4.get() == false & table5.get() == false) {
				try {

					table2.set(true);
					table3.set(true);
					table4.set(true);
					table6.set(true);

					while (rsLoop.next()) {
						fillStatDays = 0;
						while (DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)
								.compareTo(DateUtils.truncate(pNextEncDate, Calendar.HOUR_OF_DAY)) <= 0) {
							try {

								pstmtSelect = connection.prepareStatement(fillStatDateSelectSQL);
								pstmtSelect.setString(1, pPid);
								pstmtSelect.setInt(2, pEncId);
								pstmtSelect.setInt(3, rsLoop.getInt("CASS_NUMBER"));
								pstmtSelect.setTimestamp(4, JdbcUtils.getSqlTimestamp(DateUtils
										.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));

								rs = pstmtSelect.executeQuery();

								if (rs.next()) {
									// fillStatDate =
									// JdbcUtils.getTimestamp(rs.getTimestamp("STAT_DATE"));
									if (rs.next()) {
										throw new SQLException("TOO MANY ROWS " + "encID=" + pEncId + " " + pPid + " "
												+ rsLoop.getInt("CASS_NUMBER") + " "
												+ DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY) + " "
												+ fillStatDays);
									}
								} else {
									JdbcUtils.close(rs);
									JdbcUtils.close(pstmtSelect);
									if (fillStatDays == 0) {
										if (cashAddEnc == 0) {
											pstmtSelect = connection.prepareStatement(statDetailsLoadSelectSQL);
											pstmtSelect.setInt(1, pEncId);
											pstmtSelect.setInt(2, rsLoop.getInt("CASS_NUMBER"));
											rs = pstmtSelect.executeQuery();
											if (rs.next()) {
												remaining = rs.getInt("CASS_COUNT");
											}

										} else {
											pstmtSelect = connection.prepareStatement(statDetailsLoadUnloadSelectSQL);
											pstmtSelect.setInt(1, pEncId);
											pstmtSelect.setInt(2, rsLoop.getInt("CASS_NUMBER"));
											rs = pstmtSelect.executeQuery();
											if (rs.next()) {
												remaining = rs.getInt("CASS_COUNT");
											}
										}

										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);

										pstmtInsertCheck.clearParameters();
										pstmtInsertCheck.setString(1, pPid);
										pstmtInsertCheck.setTimestamp(2, JdbcUtils
												.getSqlTimestamp(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY)));
										pstmtInsertCheck.setInt(3, pEncId);
										pstmtInsertCheck.setInt(4, rsLoop.getInt("CASS_VALUE"));
										pstmtInsertCheck.setInt(5, rsLoop.getInt("CASS_CURR"));
										pstmtInsertCheck.setInt(6, rsLoop.getInt("CASS_NUMBER"));
										rsCheck = pstmtInsertCheck.executeQuery();
										if (rsCheck.next()) {
											JdbcUtils.close(rsCheck);
											pstmtUpdate.clearParameters();
											pstmtUpdate.setInt(1, 0);
											pstmtUpdate.setInt(2, 0);
											pstmtUpdate.setInt(3, remaining);
											pstmtUpdate.setDouble(4, 1);
											pstmtUpdate.setString(5, pPid);
											pstmtUpdate.setTimestamp(6, JdbcUtils.getSqlTimestamp(
													DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY)));
											pstmtUpdate.setInt(7, pEncId);
											pstmtUpdate.setInt(8, rsLoop.getInt("CASS_VALUE"));
											pstmtUpdate.setInt(9, rsLoop.getInt("CASS_CURR"));
											pstmtUpdate.setInt(10, rsLoop.getInt("CASS_NUMBER"));
											pstmtUpdate.executeUpdate();
										} else {
											pstmtInsert.clearParameters();
											pstmtInsert.setString(1, pPid);
											pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(
													DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY)));
											pstmtInsert.setInt(3, pEncId);
											pstmtInsert.setInt(4, rsLoop.getInt("CASS_VALUE"));
											pstmtInsert.setInt(5, 0);
											pstmtInsert.setInt(6, 0);
											pstmtInsert.setInt(7, rsLoop.getInt("CASS_CURR"));
											pstmtInsert.setInt(8, remaining);
											pstmtInsert.setInt(9, rsLoop.getInt("CASS_NUMBER"));
											pstmtInsert.setDouble(10, 1);
											pstmtInsert.executeUpdate();
											// recordsCount++;
										}
									}

									if (fillStatDays > 0) {
										denomRemaining = 0;
										pstmtSelect = connection.prepareStatement(downtimeCoSelectSQL);
										pstmtSelect.setString(1, pPid);
										pstmtSelect.setTimestamp(2,
												JdbcUtils.getSqlTimestamp(DateUtils.addHours(
														DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY),
														fillStatDays)));
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											availCoeff = rs.getDouble(1);
										} else {
											availCoeff = 1;
										}
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);

										pstmtSelect = connection.prepareStatement(cassStatSelectSQL);
										pstmtSelect.setString(1, pPid);
										pstmtSelect.setInt(2, pEncId);
										pstmtSelect.setInt(3, rsLoop.getInt("CASS_NUMBER"));
										pstmtSelect.setTimestamp(4,
												JdbcUtils.getSqlTimestamp(DateUtils.addHours(
														DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY),
														fillStatDays - 1)));
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											denomRemaining = rs.getInt("CASS_REMAINING");
											statDate = JdbcUtils.getTimestamp(rs.getTimestamp("STAT_DATE"));
										}
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);

										pstmtInsertCheck.clearParameters();
										pstmtInsertCheck.setString(1, pPid);
										pstmtInsertCheck.setTimestamp(2,
												JdbcUtils.getSqlTimestamp(DateUtils.addHours(statDate, 1)));
										pstmtInsertCheck.setInt(3, pEncId);
										pstmtInsertCheck.setInt(4, rsLoop.getInt("CASS_VALUE"));
										pstmtInsertCheck.setInt(5, rsLoop.getInt("CASS_CURR"));
										pstmtInsertCheck.setInt(6, rsLoop.getInt("CASS_NUMBER"));
										rsCheck = pstmtInsertCheck.executeQuery();
										if (rsCheck.next()) {
											JdbcUtils.close(rsCheck);
											pstmtUpdate.clearParameters();
											pstmtUpdate.setInt(1, 0);
											pstmtUpdate.setInt(2, 0);
											pstmtUpdate.setInt(3, denomRemaining);
											pstmtUpdate.setDouble(4, availCoeff);
											pstmtUpdate.setString(5, pPid);
											pstmtUpdate.setTimestamp(6,
													JdbcUtils.getSqlTimestamp(DateUtils.addHours(statDate, 1)));
											pstmtUpdate.setInt(7, pEncId);
											pstmtUpdate.setInt(8, rsLoop.getInt("CASS_VALUE"));
											pstmtUpdate.setInt(9, rsLoop.getInt("CASS_CURR"));
											pstmtUpdate.setInt(10, rsLoop.getInt("CASS_NUMBER"));
											pstmtUpdate.executeUpdate();
										} else {
											pstmtInsert.clearParameters();
											pstmtInsert.setString(1, pPid);
											pstmtInsert.setTimestamp(2,
													JdbcUtils.getSqlTimestamp(DateUtils.addHours(statDate, 1)));
											pstmtInsert.setInt(3, pEncId);
											pstmtInsert.setInt(4, rsLoop.getInt("CASS_VALUE"));
											pstmtInsert.setInt(5, 0);
											pstmtInsert.setInt(6, 0);
											pstmtInsert.setInt(7, rsLoop.getInt("CASS_CURR"));
											pstmtInsert.setInt(8, denomRemaining);
											pstmtInsert.setInt(9, rsLoop.getInt("CASS_NUMBER"));
											pstmtInsert.setDouble(10, availCoeff);
											pstmtInsert.executeUpdate();
										}
									}

								}

							} finally {
								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);
							}

							fillStatDays++;
						}
					}

				} finally {

					table2.set(false);
					table3.set(false);
					table4.set(false);
					table6.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtSelect);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertZeroTakeOffsForCass  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertZeroTakeOffsForCurr(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4)
			throws SQLException {
		_logger.debug("Executing insertZeroTakeOffsForCurr method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());

		Boolean method_status = false;
		int fillStatDays = 0;

		// int recordsCount = 0;

		String coStatDetailsLoopSQL = "SELECT distinct CASS_CURR as CURR_CODE " + " FROM T_CM_ENC_CASHOUT_STAT_details "
				+ " WHERE ENCASHMENT_ID = ? " + " AND ACTION_TYPE = " + CO_ENC_DET_LOADED;

		String statDateSelectSQL = "SELECT STAT_DATE " + " FROM T_CM_CASHOUT_CURR_STAT " + " WHERE ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " AND CURR_CODE = ? " + " AND STAT_DATE = ?";

		String insertSQL = "INSERT INTO T_CM_CASHOUT_CURR_STAT "
				+ " (ATM_ID,STAT_DATE,ENCASHMENT_ID,CURR_CODE,CURR_SUMM,CURR_TRANS_COUNT) " + " VALUES "
				+ " (? , ?, ? , ?, ?, ?)";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		while (method_status == false) {
			if (table1.get() == false & table3.get() == false & table4.get() == false) {
				try {

					table2.set(true);
					table3.set(true);
					table4.set(true);

					pstmtLoop = connection.prepareStatement(coStatDetailsLoopSQL);
					pstmtLoop.setInt(1, pEncId);
					rsLoop = pstmtLoop.executeQuery();

					pstmtInsert = connection.prepareStatement(insertSQL);

					while (rsLoop.next()) {
						fillStatDays = 0;
						while (DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)
								.compareTo(DateUtils.truncate(pNextEncDate, Calendar.HOUR_OF_DAY)) <= 0) {
							try {

								pstmtSelect = connection.prepareStatement(statDateSelectSQL);
								pstmtSelect.setString(1, pPid);
								pstmtSelect.setInt(2, pEncId);
								pstmtSelect.setInt(3, rsLoop.getInt("CURR_CODE"));
								pstmtSelect.setTimestamp(4, JdbcUtils.getSqlTimestamp(DateUtils
										.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));

								rs = pstmtSelect.executeQuery();

								if (!rs.next()) {
									pstmtInsert.clearParameters();
									pstmtInsert.setString(1, pPid);
									pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(DateUtils.addHours(
											DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
									pstmtInsert.setInt(3, pEncId);
									pstmtInsert.setInt(4, rsLoop.getInt("CURR_CODE"));
									pstmtInsert.setInt(5, 0);
									pstmtInsert.setInt(6, 0);
									pstmtInsert.executeUpdate();
									// recordsCount++;
								}

							} finally {
								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);
							}

							fillStatDays++;
						}
					}

				} finally {

					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(pstmtSelect);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertZeroTakeOffsForCurr  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertRejects(Connection connection, String pPid, int pEncId, Date pEncDate, Date pNextEncDate,
			AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4)
			throws SQLException {
		_logger.debug("Executing insertRejects method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());

		Boolean method_status = false;
		int recordsCount = 0;

		String intgrTransLoopSQL = "select sum(COALESCE(bill_reject,0))+sum(COALESCE(bill_retract,0)) as BILLS_COUNT,truncToHour(datetime) as stat_Date "
				+ " from t_cm_intgr_trans " + " WHERE atm_id = ? " + " and datetime between ? and ? "
				+ " group by truncToHour(datetime) " + " order by stat_Date";

		String insertSQL = "INSERT INTO T_CM_REJECT_STAT " + " (ATM_ID,STAT_DATE,ENCASHMENT_ID,BILLS_COUNT) "
				+ " VALUES " + " (? , ?, ?, ?)";

		String checkInsertSQL = "select ATM_ID from T_CM_REJECT_STAT "
				+ " where ATM_ID=? and STAT_DATE=? and ENCASHMENT_ID=?";

		String updateSQL = "UPDATE T_CM_REJECT_STAT " + " SET BILLS_COUNT = BILLS_COUNT + ? " + " WHERE "
				+ " ATM_ID = ? " + " AND " + " STAT_DATE = ? " + " AND " + " ENCASHMENT_ID = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtUpdate = null;
		PreparedStatement pstmtInsertCheck = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;
		while (method_status == false) {
			if (table1.get() == false & table3.get() == false & table4.get() == false) {
				try {

					table2.set(true);
					table3.set(true);
					table4.set(true);
					pstmtLoop = connection.prepareStatement(intgrTransLoopSQL);
					pstmtLoop.setInt(1, pEncId);
					pstmtLoop.setTimestamp(2, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtLoop.setTimestamp(3, JdbcUtils.getSqlTimestamp(pNextEncDate));
					rsLoop = pstmtLoop.executeQuery();

					pstmtInsert = connection.prepareStatement(insertSQL);
					pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);
					pstmtUpdate = connection.prepareStatement(updateSQL);

					recordsCount = 0;

					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}

						pstmtInsertCheck.clearParameters();
						pstmtInsertCheck.setString(1, pPid);
						pstmtInsertCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
						pstmtInsertCheck.setInt(3, pEncId);
						rsCheck = pstmtInsertCheck.executeQuery();
						if (rsCheck.next()) {
							pstmtUpdate.clearParameters();
							pstmtUpdate.setInt(1, rsLoop.getInt("BILLS_COUNT"));
							pstmtUpdate.setString(2, pPid);
							pstmtUpdate.setTimestamp(3, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("stat_date")));
							pstmtUpdate.setInt(4, pEncId);
							pstmtUpdate.addBatch();
							recordsCount++;
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, rsLoop.getInt("BILLS_COUNT"));
							pstmtInsert.addBatch();
							recordsCount++;
						}

					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
						pstmtInsert.executeBatch();
					}
				} finally {

					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertRejects  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertRejectsMd(Connection connection, String pPid, int pEncId, Date pEncDate, Date pNextEncDate,
			AtomicBoolean table1, AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertRejectsMd method for PID: " + pPid);
		Boolean method_status = false;
		int recordsCount = 0;

		String transLoopSQL = "select sum(note_rejected)+sum(note_retracted) as BILLS_COUNT,truncToHour(datetime) as stat_date "
				+ " from t_cm_intgr_trans_md " + " WHERE terminal_id = ? " + " and datetime between ? and ? "
				+ " group by trunc(datetime,'HH24') " + " order by stat_Date";
		// insertRejects_insert()
		String insertSQL = "INSERT INTO T_CM_REJECT_STAT " + " (ATM_ID,STAT_DATE,ENCASHMENT_ID,BILLS_COUNT) "
				+ " VALUES " + " (? , ?, ?, ?)";
		// insertRejects_checkInsert()
		String checkInsertSQL = "select ATM_ID from T_CM_REJECT_STAT "
				+ " where ATM_ID=? and STAT_DATE=? and ENCASHMENT_ID=?";
		// insertRejects_update()
		String updateSQL = "UPDATE T_CM_REJECT_STAT " + " SET BILLS_COUNT = BILLS_COUNT + ? " + " WHERE "
				+ " ATM_ID = ? " + " AND " + " STAT_DATE = ? " + " AND " + " ENCASHMENT_ID = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				try {
					table1.set(true);
					table2.set(true);
					pstmtLoop = connection.prepareStatement(transLoopSQL);
					pstmtLoop.setString(1, pPid);
					pstmtLoop.setTimestamp(2, JdbcUtils.getSqlTimestamp(pEncDate));
					pstmtLoop.setTimestamp(3, JdbcUtils.getSqlTimestamp(pNextEncDate));
					rsLoop = pstmtLoop.executeQuery();

					pstmtInsert = connection.prepareStatement(insertSQL);
					pstmtUpdate = connection.prepareStatement(updateSQL);
					pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);

					recordsCount = 0;

					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}

						pstmtInsertCheck.clearParameters();
						pstmtInsertCheck.setString(1, pPid);
						pstmtInsertCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
						pstmtInsertCheck.setInt(3, pEncId);
						rsCheck = pstmtInsertCheck.executeQuery();
						if (rsCheck.next()) {
							pstmtUpdate.clearParameters();

							pstmtUpdate.setInt(1, rsLoop.getInt("BILLS_COUNT"));
							pstmtUpdate.setString(2, pPid);
							pstmtUpdate.setTimestamp(3, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("stat_date")));
							pstmtUpdate.setInt(4, pEncId);
							pstmtUpdate.addBatch();
							recordsCount++;
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtUpdate.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, rsLoop.getInt("BILLS_COUNT"));
							pstmtInsert.addBatch();
							recordsCount++;
						}

					}
					if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
						pstmtUpdate.executeBatch();
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertRejectsMD  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertZeroDaysForRejects(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertZeroDaysForRejects method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());

		Boolean method_status = false;
		int fillStatDays = 0;
		// Date fillStatDate;

		// int recordsCount = 0;

		String selectSQL = "SELECT DISTINCT STAT_DATE " + " FROM T_CM_REJECT_STAT " + " WHERE ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " AND STAT_DATE = ?";
		// insertRejects_insert()
		String insertSQL = "INSERT INTO T_CM_REJECT_STAT " + " (ATM_ID,STAT_DATE,ENCASHMENT_ID,BILLS_COUNT) "
				+ " VALUES " + " (? ,? , ?, ?)";

		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rs = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				try {

					table1.set(true);
					table2.set(true);

					pstmtSelect = connection.prepareStatement(selectSQL);

					pstmtInsert = connection.prepareStatement(insertSQL);

					// recordsCount = 0;

					while (DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)
							.compareTo(DateUtils.truncate(pNextEncDate, Calendar.HOUR_OF_DAY)) <= 0) {

						pstmtSelect.setString(1, pPid);
						pstmtSelect.setInt(2, pEncId);
						pstmtSelect.setTimestamp(3, JdbcUtils.getSqlTimestamp(
								DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
						rs = pstmtSelect.executeQuery();

						if (rs.next()) {
							// fillStatDate =
							// JdbcUtils.getTimestamp(rs.getTimestamp("STAT_DATE"));
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(DateUtils
									.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, 0);
							pstmtInsert.executeUpdate();
							// recordsCount++;
						}
						JdbcUtils.close(rs);

						fillStatDays += 1;

					}

				} finally {

					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtSelect);
				}
				method_status = true;
				_logger.debug("Method insertZeroDaysForRejects  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertRemainingsForRejects(Connection connection, String pPid, int pEncId, AtomicBoolean table1,
			AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertRemainingsForRejects method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);

		Boolean method_status = false;
		int remaining = 0;

		int recordsCount = 0;

		String selectLoopSQL = "SELECT STAT_DATE, BILLS_COUNT " + " FROM T_CM_REJECT_STAT " + " WHERE ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " ORDER BY STAT_DATE";

		String insertSQL = "UPDATE T_CM_REJECT_STAT " + " SET BILLS_REMAINING = ? " + " WHERE ATM_ID = ? "
				+ " AND ENCASHMENT_ID = ? " + " AND STAT_DATE = ?";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rs = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				try {

					table1.set(true);
					table2.set(true);

					pstmtLoop = connection.prepareStatement(selectLoopSQL);
					pstmtLoop.setString(1, pPid);
					pstmtLoop.setInt(2, pEncId);
					rs = pstmtLoop.executeQuery();

					pstmtInsert = connection.prepareStatement(insertSQL);

					recordsCount = 0;

					while (rs.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							recordsCount = 0;
						}
						remaining += rs.getInt("BILLS_COUNT");

						pstmtInsert.clearParameters();
						pstmtInsert.setInt(1, remaining);
						pstmtInsert.setString(2, pPid);
						pstmtInsert.setInt(3, pEncId);
						pstmtInsert.setTimestamp(4, JdbcUtils.getSqlTimestamp(rs.getTimestamp("STAT_DATE")));
						pstmtInsert.addBatch();
						recordsCount++;

					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtInsert.executeBatch();
					}
				} finally {

					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertRemainingsForRejects  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertEcnashmentsCashIn(Connection connection, String pPid, AtomicBoolean table1,
			AtomicBoolean table2, AtomicBoolean table3) throws SQLException {
		_logger.debug("Executing insertEcnashmentsCashIn method for PID: " + pPid);
		// int recordsCount = 0;
		Boolean method_status = false;
		Date dateIncass = null;
		Date datePrevIncass;
		int encId = 0;
		String selectLoopSQL = "select bills as bills, curr,denom, d as enc_date,cass_num " + "FROM( "
				+ "select 0 as BILLS, datetime as d, 0 as denom, -999 as CURR, -1 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where " + "trans_type_ind = " + CI_ENC_TRANSACTION_TYPE + " "
				+ "and atm_id = ? " + "union all "
				+ "select BILL_CASS1 as BILLS,DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR,1 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_909_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS1 > 0 " + "and CURRENCY_CASS1 > 0 " + "union all "
				+ "select BILL_CASS2 as BILLS,DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR,2 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_909_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS2 > 0 " + "and CURRENCY_CASS2 > 0 " + "union all "
				+ "select BILL_CASS3 as BILLS,DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR,3 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_910_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS3 > 0 " + "and CURRENCY_CASS3 > 0 " + "union all "
				+ "select BILL_CASS4 as BILLS,DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR,4 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_910_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS4 > 0 " + "and CURRENCY_CASS4 > 0 " + "union all "
				+ "select BILL_CASS5 as BILLS,DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR,5 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_909_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS5 > 0 " + "and CURRENCY_CASS5 > 0 " + "union all "
				+ "select BILL_CASS6 as BILLS,DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR,6 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_909_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS6 > 0 " + "and CURRENCY_CASS6 > 0 " + "union all "
				+ "select BILL_CASS7 as BILLS,DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR,7 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_910_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS7 > 0 " + "and CURRENCY_CASS7 > 0 " + "union all "
				+ "select BILL_CASS8 as BILLS,DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR,8 as CASS_NUM,trans_type_ind "
				+ "from t_cm_intgr_trans " + "where trans_type_ind in(" + CR_910_ENC_TRANSACTION_TYPE + ") "
				+ "and atm_id = ? " + "and DENOM_CASS8 > 0 " + "and CURRENCY_CASS8 > 0 " + "order by d "
				+ ") ORDER BY ENC_DATE,CASS_NUM,trans_type_ind";

		String encIdAndDateSelectSQL = "select st.CASH_IN_ENCASHMENT_ID,st.CASH_IN_ENC_DATE "
				+ "from t_cm_enc_cashin_stat st " + "where " + "st.atm_id = ? " + "and "
				+ "abs(dateDiffMin(st.CASH_IN_ENC_DATE, ?)) < 15";

		/*
		 * String insertWithSeqSQL = "INSERT INTO T_CM_ENC_CASHIN_STAT "+
		 * " (ATM_ID,CASH_IN_ENCASHMENT_ID,CASH_IN_ENC_DATE) "+ " VALUES "+
		 * " (?, "+JdbcUtils.getNextSequence(connection,
		 * "s_cm_enc_stat_id")+",?)";
		 */

		String insertWithoutSeqSQL = "INSERT INTO T_CM_ENC_CASHIN_STAT "
				+ " (ATM_ID,CASH_IN_ENCASHMENT_ID,CASH_IN_ENC_DATE) " + " VALUES " + " (?, ?, ?)";
		// as result | getIntegerValueByQuery()
		String nextSeqSQL = "SELECT " + JdbcUtils.getNextSequence(connection, "s_cm_enc_stat_id") + " as SQ "
				+ JdbcUtils.getFromDummyExpression(connection);

		String insertDetailsSQL = "INSERT INTO t_cm_enc_cashin_stat_details "
				+ "(CASH_IN_ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) " + "VALUES "
				+ "(?, ? , ?, ?, " + CO_ENC_DET_LOADED + ", ?);";

		String updateDetailsSQL = "UPDATE t_cm_enc_cashin_stat_details " + "SET CASS_CURR = ?, " + "CASS_VALUE = ? , "
				+ "CASS_COUNT = ? " + "WHERE " + "CASS_NUMBER = ? " + "AND " + "CASH_IN_ENCASHMENT_ID = ? " + "AND "
				+ "ACTION_TYPE = " + CO_ENC_DET_LOADED;

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtSelect = null;
		// PreparedStatement pstmtInsertWithSeq = null;
		PreparedStatement pstmtInsertWithoutSeq = null;
		PreparedStatement pstmtDetailsInsert = null;
		PreparedStatement pstmtDetailsUpdate = null;
		PreparedStatement pstmtSeq = null;
		ResultSet rs = null;
		ResultSet rsSelect = null;
		ResultSet rsSeq = null;

		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {
				try {
					table1.set(true);
					table2.set(true);
					table3.set(true);
					pstmtLoop = connection.prepareStatement(selectLoopSQL);
					pstmtLoop.setString(1, pPid);
					pstmtLoop.setString(2, pPid);
					pstmtLoop.setString(3, pPid);
					pstmtLoop.setString(4, pPid);
					pstmtLoop.setString(5, pPid);
					pstmtLoop.setString(6, pPid);
					pstmtLoop.setString(7, pPid);
					pstmtLoop.setString(8, pPid);
					pstmtLoop.setString(9, pPid);
					rs = pstmtLoop.executeQuery();

					// pstmtInsertWithSeq =
					// connection.prepareStatement(insertWithSeqSQL);
					pstmtInsertWithoutSeq = connection.prepareStatement(insertWithoutSeqSQL);

					// recordsCount = 0;

					while (rs.next()) {
						Date currentDate = new Date();
						if (CmUtils.getNVLValue(dateIncass, currentDate).equals(currentDate)) {
							pstmtSelect = connection.prepareStatement(encIdAndDateSelectSQL);
							pstmtSelect.setString(1, pPid);
							pstmtSelect.setTimestamp(2, rs.getTimestamp("ENC_DATE"));
							rsSelect = pstmtSelect.executeQuery();
							if (rsSelect.next()) {
								encId = rsSelect.getInt("CASH_IN_ENCASHMENT_ID");
								dateIncass = JdbcUtils.getTimestamp(rsSelect.getTimestamp("CASH_IN_ENC_DATE"));
								pstmtInsertWithoutSeq.clearParameters();
								pstmtInsertWithoutSeq.setString(1, pPid);
								pstmtInsertWithoutSeq.setInt(2, encId);
								pstmtInsertWithoutSeq.setTimestamp(3, JdbcUtils.getSqlTimestamp(dateIncass));
								pstmtInsertWithoutSeq.executeUpdate();
								// pstmtInsertWithoutSeq.addBatch();
								// recordsCount++;
							} else {
								dateIncass = JdbcUtils.getTimestamp(rs.getTimestamp("ENC_DATE"));
								pstmtSeq = connection.prepareStatement(nextSeqSQL);
								rsSeq = pstmtSeq.executeQuery();
								rsSeq.next();
								encId = rsSeq.getInt("SQ");
								JdbcUtils.close(rsSeq);
								JdbcUtils.close(pstmtSeq);
								pstmtInsertWithoutSeq.clearParameters();
								pstmtInsertWithoutSeq.setString(1, pPid);
								pstmtInsertWithoutSeq.setInt(2, encId);
								pstmtInsertWithoutSeq.setTimestamp(3, JdbcUtils.getSqlTimestamp(dateIncass));
								pstmtInsertWithoutSeq.executeUpdate();
								/*
								 * pstmtInsertWithSeq.setString(1, pPid);
								 * pstmtInsertWithSeq.setTimestamp(2,
								 * JdbcUtils.getSqlTimestamp(dateIncass));
								 * pstmtInsertWithSeq.executeUpdate();
								 */
								// pstmtInsertWithSeq.addBatch();
								// recordsCount++;
							}
							JdbcUtils.close(rsSelect);
							JdbcUtils.close(pstmtSelect);

						}
						if (Math.abs(dateIncass.getTime() - currentDate.getTime()) / 60000 > 15) {
							dateIncass = JdbcUtils.getTimestamp(rs.getTimestamp("ENC_DATE"));
							pstmtSeq = connection.prepareStatement(nextSeqSQL);
							rsSeq = pstmtSeq.executeQuery();
							rsSeq.next();
							encId = rsSeq.getInt("SQ");
							JdbcUtils.close(rsSeq);
							JdbcUtils.close(pstmtSeq);
							pstmtInsertWithoutSeq.clearParameters();
							pstmtInsertWithoutSeq.setString(1, pPid);
							pstmtInsertWithoutSeq.setInt(2, encId);
							pstmtInsertWithoutSeq.setTimestamp(3, JdbcUtils.getSqlTimestamp(dateIncass));
							pstmtInsertWithoutSeq.executeUpdate();
							// pstmtInsertWithSeq.addBatch();
							// recordsCount++;
						}
						try {
							if (rs.getInt("cass_num") > 0) {
								pstmtDetailsInsert = connection.prepareStatement(insertDetailsSQL);
								pstmtDetailsInsert.setInt(1, encId);
								pstmtDetailsInsert.setInt(2, rs.getInt("denom"));
								pstmtDetailsInsert.setInt(3, rs.getInt("curr"));
								pstmtDetailsInsert.setInt(4, rs.getInt("bills"));
								pstmtDetailsInsert.setInt(5, rs.getInt("cass_num"));
								pstmtDetailsInsert.executeUpdate();
								JdbcUtils.close(pstmtDetailsInsert);
							}
						} catch (SQLException ex) {
							if (ex.getErrorCode() == JdbcUtils.getDuplicateValueErrorCode(connection)) {
								pstmtDetailsInsert = connection.prepareStatement(updateDetailsSQL);
								pstmtDetailsInsert.setInt(1, rs.getInt("curr"));
								pstmtDetailsInsert.setInt(2, rs.getInt("denom"));
								pstmtDetailsInsert.setInt(3, rs.getInt("bills"));
								pstmtDetailsInsert.setInt(4, rs.getInt("cass_num"));
								pstmtDetailsInsert.setInt(5, encId);
								pstmtDetailsInsert.executeUpdate();
								JdbcUtils.close(pstmtDetailsInsert);
							}
						}

						/*
						 * if(recordsCount>0 && recordsCount < BATCH_MAX_SIZE){
						 * 
						 * } else{ pstmtInsert.executeBatch();
						 * pstmtInsert.clearBatch(); recordsCount = 0; }
						 * 
						 * pstmtInsert.clearParameters();
						 * pstmtInsert.setString(1, pPid);
						 * pstmtInsert.setTimestamp(2,
						 * JdbcUtils.getSqlTimestamp(rs.getTimestamp("ENC_DATE")
						 * )); pstmtInsert.addBatch(); recordsCount++;
						 * 
						 * } if(recordsCount > 0 && recordsCount <=
						 * BATCH_MAX_SIZE){ pstmtInsert.executeBatch(); }
						 */
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtInsertWithoutSeq);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertEcnashmentsCashIn executed for PID: " + pPid);

			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertEcnashmentsCashInMd(Connection connection, String pPid, AtomicBoolean table1,
			AtomicBoolean table2) throws SQLException {
		Boolean method_status = false;
		int recordsCount = 0;

		String selectLoopSQL = "select datetime as ENC_DATE " + " from t_cm_intgr_trans_md " + " where "
				+ " oper_type = " + CI_ENC_TRANSACTION_TYPE + " and terminal_id = ? " + " order by datetime";

		String insertSQL = "INSERT INTO T_CM_ENC_CASHIN_STAT " + " (ATM_ID,CASH_IN_ENCASHMENT_ID,CASH_IN_ENC_DATE) "
				+ " VALUES " + " (?, " + JdbcUtils.getNextSequence(connection, "s_cm_enc_stat_id") + ",?)";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rs = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {

				table1.set(true);
				table2.set(true);

				pstmtLoop = connection.prepareStatement(selectLoopSQL);
				pstmtLoop.setString(1, pPid);
				rs = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(insertSQL);

				recordsCount = 0;
				try {
					while (rs.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							recordsCount = 0;
						}

						pstmtInsert.clearParameters();
						pstmtInsert.setString(1, pPid);
						pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(rs.getTimestamp("ENC_DATE")));
						pstmtInsert.addBatch();
						recordsCount++;

					}
					if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtLoop);
				}

				method_status = true;
				_logger.debug("Method insertEncashmentsCashInMD executed for PID: " + pPid);
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCiEncashmentsPartAndOut(Connection connection, String pPid, int pEncId, Date pEncDate,
			AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3) throws SQLException {
		_logger.debug("Executing insertCiEncashmentsPartAndOut method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		Boolean method_status = false;
		int prevEncId = 0;
		Date prevEncLastStat = null;
		int cassValue = 0;
		int cassCurr = 0;
		int remaining = 0;
		int unloadType = 0;

		String prevEncIdSelectSQL = "SELECT COALESCE(MAX(st.CASH_IN_ENCASHMENT_ID),0) "
				+ "FROM T_CM_ENC_CASHIN_STAT st, t_cm_enc_cashin_stat_details dt "
				+ "WHERE st.CASH_IN_ENCASHMENT_ID = dt.CASH_IN_ENCASHMENT_ID " + "and ATM_ID = ? "
				+ "AND st.CASH_IN_ENCASHMENT_ID < ?";

		String prevEncLastStatSelectSQL = "SELECT COALESCE(MAX(STAT_DATE),pEncDate) " + "FROM T_CM_CASHIN_R_CASS_STAT "
				+ "WHERE ATM_ID = ? " + "AND CASH_IN_ENCASHMENT_ID < ?";

		String unloadTypeSelectSQL = "SELECT " + CO_ENC_DET_UNLOADED + " " + "FROM T_CM_ENC_CASHIN_STAT "
				+ "WHERE CASH_IN_ENCASHMENT_ID = ?";

		String ciEncStatDetailsLoopSQL = "select CASS_NUMBER from t_cm_enc_cashin_stat_Details "
				+ "where cash_in_encashment_id = ? " + "and CASS_NUMBER not in "
				+ "(select CASS_NUMBER from t_cm_enc_cashin_stat_Details " + "where cash_in_encashment_id = ?)";

		String ciEncStatDetailsSelectSQL = "SELECT CASS_REMAINING,CASS_VALUE,CASS_CURR "
				+ "FROM T_CM_CASHIN_R_CASS_STAT " + "WHERE ATM_ID = ? " + "AND CASH_IN_ENCASHMENT_ID = ? "
				+ "AND STAT_DATE = ? " + "AND CASS_NUMBER = ?";

		String ciEncStatDetailsInsertSQL = "INSERT INTO T_CM_ENC_CASHIN_STAT_DETAILS "
				+ "(CASH_IN_ENCASHMENT_ID,CASS_VALUE,CASS_CURR,CASS_COUNT,ACTION_TYPE,CASS_NUMBER) " + "VALUES "
				+ "(?, ?, ?, ?, ?, ?)";

		String ciEncStatDetailsLoop2SQL = "select CASS_NUMBER from t_cm_enc_cashin_stat_details "
				+ "where cash_in_encashment_id = ?";

		String ciEncStatDetailsDeleteSQL = "delete from t_cm_enc_cashin_stat_details ecs "
				+ "where ecs.cash_in_encashment_id = ? and " + "ecs.ACTION_TYPE = " + CO_ENC_DET_NOT_UNLOADED
				+ " and exists " + "(select null from t_cm_enc_cashin_stat_details ecsd "
				+ "where ecsd.CASH_IN_ENCASHMENT_ID = ecs.CASH_IN_ENCASHMENT_ID "
				+ "and ecsd.CASS_NUMBER = ecs.CASS_NUMBER " + "and ecsd.ACTION_TYPE = " + CO_ENC_DET_LOADED + ")";

		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtDelete = null;
		ResultSet rs = null;
		ResultSet rsSelect = null;

		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {
				try {
					table1.set(true);
					table2.set(true);
					table3.set(true);
					// ----------Dealing with partial encashments--------
					pstmtSelect = connection.prepareStatement(prevEncIdSelectSQL);
					pstmtSelect.setString(1, pPid);
					pstmtSelect.setInt(2, pEncId);
					rsSelect = pstmtSelect.executeQuery();
					if (rsSelect.next()) {
						prevEncId = rsSelect.getInt(1);
					}
					JdbcUtils.close(rsSelect);
					JdbcUtils.close(pstmtSelect);

					pstmtSelect = connection.prepareStatement(prevEncLastStatSelectSQL);
					pstmtSelect.setString(1, pPid);
					pstmtSelect.setInt(2, pEncId);
					rsSelect = pstmtSelect.executeQuery();
					if (rsSelect.next()) {
						prevEncLastStat = JdbcUtils.getTimestamp(rsSelect.getTimestamp(1));
					}
					JdbcUtils.close(rsSelect);
					JdbcUtils.close(pstmtSelect);

					if (prevEncId > 0) {
						pstmtInsert = connection.prepareStatement(ciEncStatDetailsInsertSQL);

						pstmtSelect = connection.prepareStatement(unloadTypeSelectSQL);
						pstmtSelect.setInt(1, pEncId);
						rsSelect = pstmtSelect.executeQuery();
						if (rsSelect.next()) {
							unloadType = rsSelect.getInt(1);
						}
						JdbcUtils.close(rsSelect);
						JdbcUtils.close(pstmtSelect);

						pstmtLoop = connection.prepareStatement(ciEncStatDetailsLoopSQL);
						pstmtLoop.setInt(1, prevEncId);
						pstmtLoop.setInt(2, pEncId);
						rs = pstmtLoop.executeQuery();
						while (rs.next()) {
							pstmtSelect = connection.prepareStatement(ciEncStatDetailsSelectSQL);
							pstmtSelect.setString(1, pPid);
							pstmtSelect.setInt(2, prevEncId);
							pstmtSelect.setTimestamp(3, JdbcUtils.getSqlTimestamp(prevEncLastStat));
							pstmtSelect.setInt(4, rs.getInt("CASS_NUMBER"));
							rsSelect = pstmtSelect.executeQuery();
							if (rsSelect.next()) {
								remaining = rsSelect.getInt("CASS_REMAINING");
								cassValue = rsSelect.getInt("CASS_VALUE");
								cassCurr = rsSelect.getInt("CASS_CURR");
							}
							JdbcUtils.close(rsSelect);
							JdbcUtils.close(pstmtSelect);

							pstmtInsert.clearParameters();
							pstmtInsert.setInt(1, pEncId);
							pstmtInsert.setInt(2, cassValue);
							pstmtInsert.setInt(3, cassCurr);
							pstmtInsert.setInt(4, remaining);
							pstmtInsert.setInt(5, CO_ENC_DET_NOT_UNLOADED);
							pstmtInsert.setInt(6, rs.getInt("CASS_NUMBER"));
							pstmtInsert.executeUpdate();
						}
						JdbcUtils.close(rs);
						JdbcUtils.close(pstmtLoop);
						// --inserting tooken out from atm
						pstmtLoop = connection.prepareStatement(ciEncStatDetailsLoop2SQL);
						pstmtLoop.setInt(1, prevEncId);
						rs = pstmtLoop.executeQuery();
						while (rs.next()) {
							pstmtSelect = connection.prepareStatement(ciEncStatDetailsSelectSQL);
							pstmtSelect.setString(1, pPid);
							pstmtSelect.setInt(2, prevEncId);
							pstmtSelect.setTimestamp(3, JdbcUtils.getSqlTimestamp(prevEncLastStat));
							pstmtSelect.setInt(4, rs.getInt("CASS_NUMBER"));
							rsSelect = pstmtSelect.executeQuery();
							if (rsSelect.next()) {
								remaining = rsSelect.getInt("CASS_REMAINING");
								cassValue = rsSelect.getInt("CASS_VALUE");
								cassCurr = rsSelect.getInt("CASS_CURR");
							}
							JdbcUtils.close(rsSelect);
							JdbcUtils.close(pstmtSelect);

							pstmtInsert.clearParameters();
							pstmtInsert.setInt(1, pEncId);
							pstmtInsert.setInt(2, cassValue);
							pstmtInsert.setInt(3, cassCurr);
							pstmtInsert.setInt(4, remaining);
							pstmtInsert.setInt(5, unloadType);
							pstmtInsert.setInt(6, rs.getInt("CASS_NUMBER"));
							pstmtInsert.executeUpdate();
						}
						JdbcUtils.close(rs);
						JdbcUtils.close(pstmtLoop);

						pstmtDelete = connection.prepareStatement(ciEncStatDetailsDeleteSQL);
						pstmtDelete.setInt(1, pEncId);
						pstmtDelete.executeUpdate();
						JdbcUtils.close(pstmtDelete);
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsSelect);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtSelect);
					JdbcUtils.close(pstmtLoop);
					JdbcUtils.close(pstmtDelete);
				}
				method_status = true;
				_logger.debug("Method insertCiEncashmentsPartAndOut executed for PID: " + pPid);
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCashInStat(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4)
			throws SQLException {
		_logger.debug("Executing insertCashInStat method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		int recordsCount = 0;
		Boolean method_status = false;

		String transLoopSQL = " SELECT cs.BILLS_COUNT,cs.STAT_DATE,COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF "
				+ " FROM " + " (select ? as PID,sum(BILLS_COUNT) as BILLS_COUNT,stat_date from "
				+ " (select ? as PID,sum(bill_num) as BILLS_COUNT,truncToHour(datetime) as stat_Date "
				+ " from t_cm_intgr_trans_cash_in itci " + " WHERE trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE
				+ " ," + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and datetime between ? and ? "
				+ " group by truncToHour(datetime) " +

				" union all " +

				" select ? as PID, "
				+ " -sum(COALESCE(BILL_CASS1,0)+COALESCE(BILL_CASS2,0)+COALESCE(BILL_CASS3,0)+COALESCE(BILL_CASS4,0)) as BILLS_COUNT, "
				+ " truncToHour(datetime) as stat_Date " + " from t_cm_intgr_trans " + " where trans_type_ind in ("
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and datetime between ? and ? "
				+ " and (TYPE_CASS1 = " + CASS_TYPE_RECYCLING + " or TYPE_CASS2 = " + CASS_TYPE_RECYCLING
				+ " or TYPE_CASS3 = " + CASS_TYPE_RECYCLING + " or TYPE_CASS4 = " + CASS_TYPE_RECYCLING
				+ " or TYPE_CASS5 = " + CASS_TYPE_RECYCLING + " or TYPE_CASS6 = " + CASS_TYPE_RECYCLING
				+ " or TYPE_CASS7 = " + CASS_TYPE_RECYCLING + " or TYPE_CASS8 = " + CASS_TYPE_RECYCLING + ") "
				+ " group by truncToHour(datetime) " + " ) " + " group by stat_date " + " order by stat_date) cs "
				+ " left outer join t_cm_intgr_downtime_cashin ds on (cs.PID = ds.PID and cs.stat_Date = ds.stat_date)";

		String insertSQL = "INSERT INTO T_CM_CASHIN_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,BILLS_COUNT,AVAIL_COEFF) " + " VALUES "
				+ " (? , ?, ?, ?, ?)";

		String checkInsertSQL = "select ATM_ID from T_CM_CASHIN_STAT "
				+ " where ATM_ID=? and STAT_DATE=? and CASH_IN_ENCASHMENT_ID=?";

		String updateSQL = "UPDATE T_CM_CASHIN_STAT " + " SET BILLS_COUNT = BILLS_COUNT + ?, " + "AVAIL_COEFF = ? "
				+ " WHERE " + " ATM_ID = ? " + " AND " + " STAT_DATE = ? " + " AND " + " CASH_IN_ENCASHMENT_ID = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {

				table1.set(true);
				table2.set(true);
				table3.set(true);
				table4.set(true);
				pstmtLoop = connection.prepareStatement(transLoopSQL);
				pstmtLoop.setString(1, pPid);
				pstmtLoop.setString(2, pPid);
				pstmtLoop.setString(3, pPid);
				pstmtLoop.setTimestamp(4, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(5, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(6, pPid);
				pstmtLoop.setString(7, pPid);
				pstmtLoop.setTimestamp(8, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(9, JdbcUtils.getSqlTimestamp(pNextEncDate));

				rsLoop = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtUpdate = connection.prepareStatement(updateSQL);
				pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}

						pstmtInsertCheck.clearParameters();
						pstmtInsertCheck.setString(1, pPid);
						pstmtInsertCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
						pstmtInsertCheck.setInt(3, pEncId);
						rsCheck = pstmtInsertCheck.executeQuery();
						if (rsCheck.next()) {
							pstmtUpdate.clearParameters();

							pstmtUpdate.setInt(1, rsLoop.getInt("BILLS_COUNT"));
							pstmtUpdate.setDouble(2, rsLoop.getDouble("AVAIL_COEFF"));
							pstmtUpdate.setString(3, pPid);
							pstmtUpdate.setTimestamp(4, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("stat_date")));
							pstmtUpdate.setInt(5, pEncId);
							pstmtUpdate.addBatch();
							recordsCount++;
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, rsLoop.getInt("BILLS_COUNT"));
							pstmtInsert.setDouble(5, rsLoop.getDouble("AVAIL_COEFF"));
							pstmtInsert.addBatch();
							recordsCount++;
						}
					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertCashInStat  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCashInDenomStat(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3, AtomicBoolean table4)
			throws SQLException {
		_logger.debug("Executing insertCashInDenomStat method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		int recordsCount = 0;
		Boolean method_status = false;
		String transLoopSQL = " SELECT cs.STAT_DATE,DENOM_COUNT, DENOM_CURR,DENOM_VALUE " + " FROM "
				+ " (select ? as PID,sum(DENOM_COUNT) as DENOM_COUNT,DENOM_CURR,DENOM_VALUE, stat_date " + " FROM "
				+ " ( "
				+ " select ? as PID,sum(bill_num) as DENOM_COUNT,BILL_CURR as DENOM_CURR,BILL_DENOM as DENOM_VALUE,truncToHour(datetime) as stat_Date "
				+ " from t_cm_intgr_trans_cash_in itci " + " WHERE trans_type_ind in (" + EXCHANGE_TRANSACTION_TYPE
				+ ", " + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and datetime between ? and ? "
				+ " group by truncToHour(datetime), BILL_CURR, BILL_DENOM " +

				" union all " +

				" select ? as PID,-sum(bill_num) as DENOM_COUNT,CURR as DENOM_CURR,DENOM as DENOM_VALUE,truncToHour(d) as stat_Date "
				+ " from ( " + " select " + " BILL_CASS1 as bill_num, "
				+ " DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS1 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS1 = " + CASS_TYPE_RECYCLING
				+ " union all " + " select " + " BILL_CASS2 as bill_num, "
				+ " DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS2 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS2 = " + CASS_TYPE_RECYCLING
				+ " union all " + " select " + " BILL_CASS3 as bill_num, "
				+ " DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS3 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS3 = " + CASS_TYPE_RECYCLING
				+ " union all " + " select " + " BILL_CASS4 as bill_num, "
				+ " DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS4 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS4 = " + CASS_TYPE_RECYCLING
				+ " union all " + " select " + " BILL_CASS5 as bill_num, "
				+ " DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS5 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS5 = " + CASS_TYPE_RECYCLING
				+ " union all " + " select " + " BILL_CASS6 as bill_num, "
				+ " DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS6 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS6 = " + CASS_TYPE_RECYCLING
				+ " union all " + " select " + " BILL_CASS7 as bill_num, "
				+ " DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS7 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS7 = " + CASS_TYPE_RECYCLING
				+ " union all " + " select " + " BILL_CASS8 as bill_num, "
				+ " DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? "
				+ " and BILL_CASS8 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS8 = " + CASS_TYPE_RECYCLING
				+ " )  group by truncToHour(d), CURR, DENOM " + " ) " + " group by DENOM_CURR,DENOM_VALUE, stat_date "
				+ " order by stat_date) cs "
				+ " left outer join t_cm_intgr_downtime_cashin ds on (cs.PID = ds.PID and cs.stat_Date = ds.stat_date)";

		String insertSQL = "INSERT INTO T_CM_CASHIN_DENOM_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,DENOM_CURR,DENOM_COUNT,DENOM_VALUE) " + " VALUES "
				+ " ( ?, ?, ?, ?, ?, ?)";

		String checkInsertSQL = "select ATM_ID from T_CM_CASHIN_DENOM_STAT "
				+ " where ATM_ID=? and STAT_DATE=? and CASH_IN_ENCASHMENT_ID=? and DENOM_CURR=?";

		String updateSQL = "UPDATE T_CM_CASHIN_DENOM_STAT " + " SET DENOM_COUNT = DENOM_COUNT + ? " + " WHERE "
				+ " ATM_ID = ? " + " AND " + " STAT_DATE = ? " + " AND " + " CASH_IN_ENCASHMENT_ID = ? " + " AND "
				+ " DENOM_VALUE = ? " + " AND " + " DENOM_CURR = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false & table4.get() == false) {
				table1.set(true);
				table2.set(true);
				table3.set(true);
				table4.set(true);
				pstmtLoop = connection.prepareStatement(transLoopSQL);
				pstmtLoop.setString(1, pPid);
				pstmtLoop.setString(2, pPid);
				pstmtLoop.setString(3, pPid);
				pstmtLoop.setTimestamp(4, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(5, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(6, pPid);
				pstmtLoop.setString(7, pPid);
				pstmtLoop.setTimestamp(8, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(9, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(10, pPid);
				pstmtLoop.setTimestamp(11, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(12, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(13, pPid);
				pstmtLoop.setTimestamp(14, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(15, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(16, pPid);
				pstmtLoop.setTimestamp(17, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(18, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(19, pPid);
				pstmtLoop.setTimestamp(20, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(21, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(22, pPid);
				pstmtLoop.setTimestamp(23, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(24, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(25, pPid);
				pstmtLoop.setTimestamp(26, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(27, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(28, pPid);
				pstmtLoop.setTimestamp(29, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(30, JdbcUtils.getSqlTimestamp(pNextEncDate));

				rsLoop = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtUpdate = connection.prepareStatement(updateSQL);
				pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}

						pstmtInsertCheck.clearParameters();
						pstmtInsertCheck.setString(1, pPid);
						pstmtInsertCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
						pstmtInsertCheck.setInt(3, pEncId);
						pstmtInsertCheck.setInt(4, rsLoop.getInt("DENOM_CURR"));
						rsCheck = pstmtInsertCheck.executeQuery();
						if (rsCheck.next()) {
							pstmtUpdate.clearParameters();

							pstmtUpdate.setInt(1, rsLoop.getInt("DENOM_COUNT"));
							pstmtUpdate.setString(2, pPid);
							pstmtUpdate.setTimestamp(3, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("stat_date")));
							pstmtUpdate.setInt(4, pEncId);
							pstmtUpdate.setInt(5, rsLoop.getInt("DENOM_VALUE"));
							pstmtUpdate.setInt(6, rsLoop.getInt("DENOM_CURR"));

							pstmtUpdate.addBatch();
							recordsCount++;
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, rsLoop.getInt("DENOM_CURR"));
							pstmtInsert.setInt(5, rsLoop.getInt("DENOM_COUNT"));
							pstmtInsert.setInt(6, rsLoop.getInt("DENOM_VALUE"));
							pstmtInsert.addBatch();
							recordsCount++;
						}
					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					table4.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertCashInDenomStat  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCashInCassStat(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3) throws SQLException {
		_logger.debug("Executing insertCashInCassStat method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		Boolean method_status = false;
		int recordsCount = 0;

		String transLoopSQL = "select  cs.PID as PID, cs.BILLS_IN, cs.BILLS_OUT,cs.trans_count_in,cs.trans_count_out,cs.trans_date , cs.denom, cs.CURR,cs.CASS_NUM, "
				+ " COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF " + " FROM ( "
				+ " select ? as PID,sum(BILLS_IN) as BILLS_IN,sum(BILLS_OUT) as BILLS_OUT, "
				+ " sum(TRANS_COUNT_IN) as trans_count_in,sum(TRANS_COUNT_OUT) as trans_count_out, "
				+ " truncToHour(d) as trans_date , denom, CURR, CASS_NUM " + " FROM( " + " select "
				+ " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS1 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS1 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then 1 " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR,1 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS1 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS1 = " + CASS_TYPE_RECYCLING + " union all "
				+ " select " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS2 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS2 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR,2 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS2 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS2 = " + CASS_TYPE_RECYCLING + " " + " union all "
				+ " select " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS3 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS3 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then 1 " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR,3 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS3 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS3 = " + CASS_TYPE_RECYCLING + " " + " union all "
				+ " select " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS4 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS4 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then 1 " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR,4 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS4 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS4 = " + CASS_TYPE_RECYCLING + " " + " union all "
				+ " select " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS5 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS5 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR,5 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS5 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS5 = " + CASS_TYPE_RECYCLING + " " + " union all "
				+ " select " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS6 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS6 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then 1 " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR,6 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS6 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS6 = " + CASS_TYPE_RECYCLING + " " + " union all "
				+ " select " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS7 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS7 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then 1 " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR,7 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS7 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS7 = " + CASS_TYPE_RECYCLING + " " + " union all "
				+ " select " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS8 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS8 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then 1 " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then 1 "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR,8 as CASS_NUM "
				+ " from t_cm_intgr_trans " + " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and atm_id = ? " + " and BILL_CASS8 > 0 "
				+ " and datetime between ? and ? " + " and TYPE_CASS8 = " + CASS_TYPE_RECYCLING + " "
				+ " )GROUP BY truncToHour(d),denom, CURR,CASS_NUM " + " ORDER BY trans_date,CASS_NUM) cs "
				+ " left outer join t_cm_intgr_downtime_cashin ds on (cs.PID = ds.PID and cs.trans_date = ds.stat_date)";

		String insertSQL = " INSERT INTO T_CM_CASHIN_R_CASS_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,CASS_VALUE,CASS_COUNT_IN,CASS_COUNT_OUT,CASS_TRANS_COUNT_IN,CASS_TRANS_COUNT_OUT,CASS_CURR,CASS_NUMBER,AVAIL_COEFF) "
				+ " VALUES " + " (? , ?, ? , ?, ?, ?, ?, ?, ?, ?)";

		String checkInsertSQL = "select ATM_ID from T_CM_CASHIN_R_CASS_STAT "
				+ " where ATM_ID=? and STAT_DATE=? and CASH_IN_ENCASHMENT_ID=? and CASS_CURR=? and CASS_NUMBER=?";

		String updateSQL = " UPDATE " + " T_CM_CASHIN_R_CASS_STAT " + " SET " + " CASS_COUNT_IN = CASS_COUNT_IN + ?, "
				+ " CASS_COUNT_OUT = CASS_COUNT_OUT + ?, " + " CASS_TRANS_COUNT_IN = CASS_TRANS_COUNT_IN + ?, "
				+ " CASS_TRANS_COUNT_OUT = CASS_TRANS_COUNT_OUT + ?, " + " AVAIL_COEFF = ? " + " WHERE "
				+ " ATM_ID = ? " + " AND " + " STAT_DATE = ? " + " AND " + " CASH_IN_ENCASHMENT_ID = ? " + " AND "
				+ " CASS_NUMBER = ? " + " AND " + " CASS_VALUE =  ? " + " AND " + " CASS_CURR = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {
				table1.set(true);
				table2.set(true);
				table3.set(true);
				pstmtLoop = connection.prepareStatement(transLoopSQL);
				pstmtLoop.setString(1, pPid);
				pstmtLoop.setString(2, pPid);
				pstmtLoop.setTimestamp(3, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(4, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(5, pPid);
				pstmtLoop.setTimestamp(6, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(7, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(8, pPid);
				pstmtLoop.setTimestamp(9, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(10, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(11, pPid);
				pstmtLoop.setTimestamp(12, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(13, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(14, pPid);
				pstmtLoop.setTimestamp(15, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(16, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(17, pPid);
				pstmtLoop.setTimestamp(18, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(19, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(20, pPid);
				pstmtLoop.setTimestamp(21, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(22, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(23, pPid);
				pstmtLoop.setTimestamp(24, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(25, JdbcUtils.getSqlTimestamp(pNextEncDate));

				rsLoop = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtUpdate = connection.prepareStatement(updateSQL);
				pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}

						pstmtInsertCheck.clearParameters();
						pstmtInsertCheck.setString(1, pPid);
						pstmtInsertCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("trans_date")));
						pstmtInsertCheck.setInt(3, pEncId);
						pstmtInsertCheck.setInt(4, rsLoop.getInt("CURR"));
						pstmtInsertCheck.setInt(5, rsLoop.getInt("CASS_NUM"));
						rsCheck = pstmtInsertCheck.executeQuery();
						if (rsCheck.next()) {
							pstmtUpdate.clearParameters();

							pstmtUpdate.setInt(1, rsLoop.getInt("BILLS_IN"));
							pstmtUpdate.setInt(2, rsLoop.getInt("BILLS_OUT"));
							pstmtUpdate.setInt(3, rsLoop.getInt("TRANS_COUNT_IN"));
							pstmtUpdate.setInt(4, rsLoop.getInt("TRANS_COUNT_OUT"));
							pstmtUpdate.setDouble(5, rsLoop.getDouble("AVAIL_COEFF"));
							pstmtUpdate.setString(6, pPid);
							pstmtUpdate.setTimestamp(7, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("trans_date")));
							pstmtUpdate.setInt(8, pEncId);
							pstmtUpdate.setInt(9, rsLoop.getInt("CASS_NUM"));
							pstmtUpdate.setInt(10, rsLoop.getInt("denom"));
							pstmtUpdate.setInt(11, rsLoop.getInt("CURR"));

							pstmtUpdate.addBatch();
							recordsCount++;
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("trans_date")));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, rsLoop.getInt("denom"));
							pstmtInsert.setInt(5, rsLoop.getInt("BILLS_IN"));
							pstmtInsert.setInt(6, rsLoop.getInt("BILLS_OUT"));
							pstmtInsert.setInt(7, rsLoop.getInt("TRANS_COUNT_IN"));
							pstmtInsert.setInt(8, rsLoop.getInt("TRANS_COUNT_OUT"));
							pstmtInsert.setInt(9, rsLoop.getInt("CURR"));
							pstmtInsert.setInt(10, rsLoop.getInt("CASS_NUM"));
							pstmtInsert.setDouble(11, rsLoop.getDouble("AVAIL_COEFF"));
							pstmtInsert.addBatch();
							recordsCount++;
						}
					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertCashInCassStat  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCashInCurrStat(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertCashInCurrStat method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		Boolean method_status = false;
		int recordsCount = 0;

		String transLoopSQL = "SELECT sum(bills_in*denom) as summ_in, sum(bills_out*denom) as summ_out, "
				+ " truncToHour(d) as trans_date, CURR, " + " count(distinct TRANS_COUNT_IN) as curr_trans_count_in, "
				+ " count(distinct TRANS_COUNT_OUT) as curr_trans_count_out " + " FROM( " + " select "
				+ " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS1 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS1 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then utrnno "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass1 as denom, currency_cass1 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + "," + CREDIT_TRANSACTION_TYPE + ") "
				+ " and atm_id = ? " + " and BILL_CASS1 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS1 = "
				+ CASS_TYPE_RECYCLING + " union all " + " select " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then BILL_CASS2 " + " else 0 end as BILLS_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then BILL_CASS2 "
				+ " else 0 end as BILLS_IN, " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE
				+ " then utrnno " + " else 0 end as TRANS_COUNT_OUT, " + " case when trans_type_ind = "
				+ CREDIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass2 as denom, currency_cass2 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + "," + CREDIT_TRANSACTION_TYPE + ") "
				+ " and atm_id = ? " + " and BILL_CASS2 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS2 = "
				+ CASS_TYPE_RECYCLING + " union all " + " select " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then BILL_CASS3 " + " else 0 end as BILLS_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then BILL_CASS3 "
				+ " else 0 end as BILLS_IN, " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE
				+ " then utrnno " + " else 0 end as TRANS_COUNT_OUT, " + " case when trans_type_ind = "
				+ CREDIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass3 as denom, currency_cass3 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + "," + CREDIT_TRANSACTION_TYPE + ") "
				+ " and atm_id = ? " + " and BILL_CASS3 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS3 = "
				+ CASS_TYPE_RECYCLING + " union all " + " select " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then BILL_CASS4 " + " else 0 end as BILLS_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then BILL_CASS4 "
				+ " else 0 end as BILLS_IN, " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE
				+ " then utrnno " + " else 0 end as TRANS_COUNT_OUT, " + " case when trans_type_ind = "
				+ CREDIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass4 as denom, currency_cass4 as CURR " + " union all " + " select "
				+ " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE + " then BILL_CASS5 "
				+ " else 0 end as BILLS_OUT, " + " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE
				+ " then BILL_CASS5 " + " else 0 end as BILLS_IN, " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then utrnno "
				+ " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass5 as denom, currency_cass5 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + "," + CREDIT_TRANSACTION_TYPE + ") "
				+ " and atm_id = ? " + " and BILL_CASS5 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS5 = "
				+ CASS_TYPE_RECYCLING + " union all " + " select " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then BILL_CASS6 " + " else 0 end as BILLS_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then BILL_CASS6 "
				+ " else 0 end as BILLS_IN, " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE
				+ " then utrnno " + " else 0 end as TRANS_COUNT_OUT, " + " case when trans_type_ind = "
				+ CREDIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass6 as denom, currency_cass6 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + "," + CREDIT_TRANSACTION_TYPE + ") "
				+ " and atm_id = ? " + " and BILL_CASS6 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS6 = "
				+ CASS_TYPE_RECYCLING + " union all " + " select " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then BILL_CASS7 " + " else 0 end as BILLS_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then BILL_CASS7 "
				+ " else 0 end as BILLS_IN, " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE
				+ " then utrnno " + " else 0 end as TRANS_COUNT_OUT, " + " case when trans_type_ind = "
				+ CREDIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass7 as denom, currency_cass7 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + "," + CREDIT_TRANSACTION_TYPE + ") "
				+ " and atm_id = ? " + " and BILL_CASS7 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS7 = "
				+ CASS_TYPE_RECYCLING + " union all " + " select " + " case when trans_type_ind = "
				+ DEBIT_TRANSACTION_TYPE + " then BILL_CASS8 " + " else 0 end as BILLS_OUT, "
				+ " case when trans_type_ind = " + CREDIT_TRANSACTION_TYPE + " then BILL_CASS8 "
				+ " else 0 end as BILLS_IN, " + " case when trans_type_ind = " + DEBIT_TRANSACTION_TYPE
				+ " then utrnno " + " else 0 end as TRANS_COUNT_OUT, " + " case when trans_type_ind = "
				+ CREDIT_TRANSACTION_TYPE + " then utrnno " + " else 0 end as TRANS_COUNT_IN, "
				+ " utrnno,DATETIME as d,denom_cass8 as denom, currency_cass8 as CURR " + " from t_cm_intgr_trans "
				+ " where trans_type_ind in (" + DEBIT_TRANSACTION_TYPE + "," + CREDIT_TRANSACTION_TYPE + ") "
				+ " and atm_id = ? " + " and BILL_CASS8 > 0 " + " and datetime between ? and ? " + " and TYPE_CASS8 = "
				+ CASS_TYPE_RECYCLING + " )GROUP BY truncToHour(d), CURR " + " ORDER BY trans_date";

		String insertSQL = " INSERT INTO T_CM_CASHIN_R_CURR_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,CURR_CODE,CURR_SUMM_IN,CURR_SUMM_OUT,CURR_TRANS_COUNT_IN,CURR_TRANS_COUNT_OUT) "
				+ " VALUES " + " (? , ?, ? , ?, ?, ?, ?, ?)";

		String checkInsertSQL = "select ATM_ID from T_CM_CASHIN_R_CURR_STAT "
				+ " where ATM_ID=? and STAT_DATE=? and CASH_IN_ENCASHMENT_ID=? and CURR_CODE=?";

		String updateSQL = " UPDATE " + " T_CM_CASHIN_R_CURR_STAT " + " SET " + " CURR_SUMM_IN = CURR_SUMM_IN + ?, "
				+ " CURR_SUMM_OUT = CURR_SUMM_OUT + ?, " + " CURR_TRANS_COUNT_IN = CURR_TRANS_COUNT_IN + ?, "
				+ " CURR_TRANS_COUNT_OUT = CURR_TRANS_COUNT_OUT + ? " + " WHERE " + " ATM_ID = ? " + " AND "
				+ " STAT_DATE = ? " + " AND " + " CASH_IN_ENCASHMENT_ID =  ? " + " AND " + " CURR_CODE = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;

		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				table1.set(true);
				table2.set(true);
				pstmtLoop = connection.prepareStatement(transLoopSQL);
				pstmtLoop.setString(1, pPid);
				pstmtLoop.setTimestamp(2, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(3, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(4, pPid);
				pstmtLoop.setTimestamp(5, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(6, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(8, pPid);
				pstmtLoop.setTimestamp(9, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(10, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(11, pPid);
				pstmtLoop.setTimestamp(12, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(13, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(14, pPid);
				pstmtLoop.setTimestamp(15, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(16, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(17, pPid);
				pstmtLoop.setTimestamp(18, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(19, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(20, pPid);
				pstmtLoop.setTimestamp(21, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(22, JdbcUtils.getSqlTimestamp(pNextEncDate));
				pstmtLoop.setString(23, pPid);
				pstmtLoop.setTimestamp(24, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(25, JdbcUtils.getSqlTimestamp(pNextEncDate));

				rsLoop = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtUpdate = connection.prepareStatement(updateSQL);
				pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}

						pstmtInsertCheck.clearParameters();
						pstmtInsertCheck.setString(1, pPid);
						pstmtInsertCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("trans_date")));
						pstmtInsertCheck.setInt(3, pEncId);
						pstmtInsertCheck.setInt(4, rsLoop.getInt("CURR"));
						rsCheck = pstmtInsertCheck.executeQuery();
						if (rsCheck.next()) {
							pstmtUpdate.clearParameters();

							pstmtUpdate.setInt(1, rsLoop.getInt("SUMM_IN"));
							pstmtUpdate.setInt(2, rsLoop.getInt("SUMM_OUT"));
							pstmtUpdate.setInt(3, rsLoop.getInt("CURR_TRANS_COUNT_IN"));
							pstmtUpdate.setInt(4, rsLoop.getInt("CURR_TRANS_COUNT_OUT"));
							pstmtUpdate.setString(5, pPid);
							pstmtUpdate.setTimestamp(6, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("trans_date")));
							pstmtUpdate.setInt(7, pEncId);
							pstmtUpdate.setInt(8, rsLoop.getInt("CURR"));

							pstmtUpdate.addBatch();
							recordsCount++;
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("trans_date")));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, rsLoop.getInt("CURR"));
							pstmtInsert.setInt(5, rsLoop.getInt("SUMM_IN"));
							pstmtInsert.setInt(6, rsLoop.getInt("SUMM_OUT"));
							pstmtInsert.setInt(7, rsLoop.getInt("CURR_TRANS_COUNT_IN"));
							pstmtInsert.setInt(8, rsLoop.getInt("CURR_TRANS_COUNT_OUT"));
							pstmtInsert.addBatch();
							recordsCount++;
						}
					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertCashInCurrStat  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertRemainingsForCashInCass(ISessionHolder sessionHolder, String pPid, int pEncId,
			AtomicBoolean table1, AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertRemainingsForCashInCass method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		Boolean method_status = false;
		int remaining = 0;

		Date maxStatDate;

		int recordsCount = 0;

		int preload = checkEncashmentCashInPreload(sessionHolder, pEncId);

		String remainingCassCountSelectSQL = "SELECT CASS_COUNT " + "FROM T_CM_ENC_CASHIN_STAT_DETAILS "
				+ "WHERE CASH_IN_ENCASHMENT_ID = ? " + "AND CASS_NUMBER = ? " + "AND ACTION_TYPE in ("
				+ CO_ENC_DET_LOADED + ")";

		String maxStatDateSelectSQL = "SELECT MAX(STAT_DATE) " + "FROM T_CM_CASHIN_R_CASS_STAT " + "WHERE  ATM_ID = ? "
				+ "AND CASS_NUMBER = ? " + "AND CASH_IN_ENCASHMENT_ID < ?";
		String remainingSelectSQL = "SELECT CASS_REMAINING " + "FROM T_CM_CASHIN_R_CASS_STAT " + "WHERE STAT_DATE = ? "
				+ "AND ATM_ID = ? " + "AND CASS_NUMBER = ?";
		String cassStatLoopSQL = "SELECT DISTINCT CASS_NUMBER " + " FROM T_CM_CASHIN_R_CASS_STAT "
				+ " WHERE CASH_IN_ENCASHMENT_ID = ?";

		String cassStatloop2SQL = " SELECT STAT_DATE, CASS_COUNT_IN-CASS_COUNT_OUT as CASS_COUNT "
				+ " FROM T_CM_CASHIN_R_CASS_STAT " + " WHERE ATM_ID = ? " + " AND CASH_IN_ENCASHMENT_ID = ? "
				+ " AND CASS_NUMBER = ? " + " ORDER BY STAT_DATE";

		String updateSQL = " UPDATE T_CM_CASHIN_R_CASS_STAT " + " SET CASS_REMAINING = ? " + " WHERE ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " AND STAT_DATE = ?";

		SqlSession session = sessionHolder.getSession(getMapperClass());
		Connection connection = session.getConnection();
		
		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtLoop2 = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsSelect = null;
		ResultSet rsLoop = null;
		ResultSet rsLoop2 = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				table1.set(true);
				table2.set(true);
				pstmtLoop = connection.prepareStatement(cassStatLoopSQL);
				pstmtLoop.setInt(1, pEncId);

				rsLoop = pstmtLoop.executeQuery();

				pstmtUpdate = connection.prepareStatement(updateSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						remaining = 0;

						if (preload > 0) {
							pstmtSelect = connection.prepareStatement(remainingCassCountSelectSQL);
							pstmtSelect.setInt(1, pEncId);
							pstmtSelect.setInt(2, rsLoop.getInt("CASS_NUMBER"));
							rsSelect = pstmtSelect.executeQuery();
							if (rsSelect.next()) {
								remaining = rsSelect.getInt("CASS_COUNT");
							} else {
								JdbcUtils.close(rsSelect);
								JdbcUtils.close(pstmtSelect);

								pstmtSelect = connection.prepareStatement(maxStatDateSelectSQL);
								pstmtSelect.setString(1, pPid);
								pstmtSelect.setInt(2, rsLoop.getInt("CASS_NUMBER"));
								pstmtSelect.setInt(3, pEncId);
								rsSelect = pstmtSelect.executeQuery();
								if (rsSelect.next()) {
									maxStatDate = JdbcUtils.getTimestamp(rsSelect.getTimestamp("CASS_COUNT"));

									JdbcUtils.close(rsSelect);
									JdbcUtils.close(pstmtSelect);

									pstmtSelect = connection.prepareStatement(remainingSelectSQL);
									pstmtSelect.setTimestamp(1, JdbcUtils.getSqlTimestamp(maxStatDate));
									pstmtSelect.setString(2, pPid);
									pstmtSelect.setInt(3, rsLoop.getInt("CASS_NUMBER"));
									rsSelect = pstmtSelect.executeQuery();
									if (rsSelect.next()) {
										remaining = rsSelect.getInt("CASS_REMAINING");
									} else {
										remaining = 0;
									}
								} else {
									remaining = 0;
								}
							}
						}

						pstmtLoop2 = connection.prepareStatement(cassStatloop2SQL);
						pstmtLoop2.clearParameters();
						pstmtLoop2.setString(1, pPid);
						pstmtLoop2.setInt(2, pEncId);
						pstmtLoop2.setInt(3, rsLoop.getInt("CASS_NUMBER"));
						rsLoop2 = pstmtLoop2.executeQuery();
						while (rsLoop2.next()) {
							if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

							} else {
								pstmtUpdate.executeBatch();
								pstmtUpdate.clearBatch();
								recordsCount = 0;
							}
							remaining += rsLoop2.getInt("CASS_COUNT");
							pstmtUpdate.clearParameters();
							pstmtUpdate.setInt(1, remaining);
							pstmtUpdate.setString(2, pPid);
							pstmtUpdate.setInt(3, pEncId);
							pstmtUpdate.setInt(4, rsLoop.getInt("CASS_NUMBER"));
							pstmtUpdate.setTimestamp(5, JdbcUtils.getSqlTimestamp(rsLoop2.getTimestamp("STAT_DATE")));
							pstmtUpdate.addBatch();
							recordsCount++;
						}
						JdbcUtils.close(rsLoop2);
						JdbcUtils.close(pstmtLoop2);

					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsLoop2);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtLoop2);
					JdbcUtils.close(pstmtLoop);
					connection.close();
					session.close();
				}
				method_status = true;
				_logger.debug("Method insertRemainingsForCashInCass  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertRemainingsForCashInCurr(Connection connection, String pPid, int pEncId,
			AtomicBoolean table1, AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertRemainingsForCashInCurr method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		Boolean method_status = false;
		int recordsCount;

		String cassStatLoopSQL = "select stat_date,sum(cass_remaining*cass_value) as CURR_REMAINING,CASS_CURR "
				+ " from T_CM_CASHIN_R_CASS_STAT ds " + " where " + " ATM_ID = ? " + " AND CASH_IN_ENCASHMENT_ID = ? "
				+ " group by stat_date,cass_curr";

		String updateSQL = " UPDATE T_CM_CASHIN_R_CURR_STAT " + " SET CURR_REMAINING = ? " + " WHERE ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND CURR_CODE = ? " + " AND STAT_DATE = ?";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				table1.set(true);
				table2.set(true);
				pstmtLoop = connection.prepareStatement(cassStatLoopSQL);
				pstmtLoop.setString(1, pPid);
				pstmtLoop.setInt(2, pEncId);

				rsLoop = pstmtLoop.executeQuery();

				pstmtUpdate = connection.prepareStatement(updateSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}
						pstmtUpdate.clearParameters();
						pstmtUpdate.setInt(1, rsLoop.getInt("CURR_REMAINING"));
						pstmtUpdate.setString(2, pPid);
						pstmtUpdate.setInt(3, pEncId);
						pstmtUpdate.setInt(4, rsLoop.getInt("CASS_CURR"));
						pstmtUpdate.setTimestamp(5, rsLoop.getTimestamp("STAT_DATE"));
						pstmtUpdate.addBatch();
						recordsCount++;

					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertRemainingsForCashInCurr  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertZeroTakeOffsForCICass(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertZeroTakeOffsForCICass method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		Boolean method_status = false;
		int fillStatDays = 0;
		// Date statDate = null;
		// Date fillStatDate;
		int remaining = 0;
		// int cashAddEnc = checkEncashmentCashAdd(connection, pEncId);

		int denomRemaining = 0;
		double availCoeff;

		int recordsCount = 0;

		String ciCassStatLoopSQL = "SELECT distinct CASS_NUMBER,CASS_CURR,CASS_VALUE "
				+ " FROM T_CM_CASHIN_R_CASS_STAT " + " WHERE CASH_IN_ENCASHMENT_ID = ?";

		String fillStatDateSelectSQL = "SELECT STAT_DATE " + " FROM T_CM_CASHIN_R_CASS_STAT " + " WHERE ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? " + " AND STAT_DATE = ?";

		String downtimeCoSelectSQL = "SELECT COALESCE(ds.AVAIL_COEFF,1) " + " FROM t_cm_intgr_downtime_cashout ds "
				+ " WHERE ds.PID = ? " + " AND ds.STAT_DATE = ?";

		String cassStatSelectSQL = "SELECT CASS_REMAINING,cs.STAT_DATE " + " FROM T_CM_CASHIN_R_CASS_STAT cs "
				+ " WHERE ATM_ID = ? " + " AND CASH_IN_ENCASHMENT_ID = ? " + " AND CASS_NUMBER = ? "
				+ " AND cs.STAT_DATE = ? ";

		String cassStatInsertSQL = "INSERT INTO T_CM_CASHIN_R_CASS_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,CASS_VALUE,CASS_COUNT_IN,CASS_COUNT_OUT, "
				+ " CASS_TRANS_COUNT_IN,CASS_TRANS_COUNT_OUT,CASS_CURR,CASS_REMAINING,CASS_NUMBER,AVAIL_COEFF) "
				+ " VALUES " + " (? , ?, ? , ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				table1.set(true);
				table2.set(true);
				pstmtLoop = connection.prepareStatement(ciCassStatLoopSQL);
				pstmtLoop.setInt(1, pEncId);
				rsLoop = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(cassStatInsertSQL);
				try {
					while (rsLoop.next()) {
						fillStatDays = 0;
						while (DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)
								.compareTo(DateUtils.truncate(pNextEncDate, Calendar.HOUR_OF_DAY)) <= 0) {
							try {
								if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

								} else {
									pstmtInsert.executeBatch();
									pstmtInsert.clearBatch();
									recordsCount = 0;
								}

								pstmtSelect = connection.prepareStatement(fillStatDateSelectSQL);
								pstmtSelect.setString(1, pPid);
								pstmtSelect.setInt(2, pEncId);
								pstmtSelect.setInt(3, rsLoop.getInt("CASS_NUMBER"));
								pstmtSelect.setTimestamp(4, JdbcUtils.getSqlTimestamp(DateUtils
										.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));

								rs = pstmtSelect.executeQuery();

								if (rs.next()) {
									// fillStatDate =
									// JdbcUtils.getTimestamp(rs.getTimestamp("STAT_DATE"));
									if (rs.next()) {
										throw new SQLException("TOO MANY ROWS " + "encID=" + pEncId + " " + pPid + " "
												+ rsLoop.getInt("CASS_NUMBER") + " "
												+ DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY) + " "
												+ fillStatDays);
									}
								} else {
									JdbcUtils.close(rs);
									JdbcUtils.close(pstmtSelect);
									if (fillStatDays == 0) {

										pstmtInsert.clearParameters();
										pstmtInsert.setString(1, pPid);
										pstmtInsert.setTimestamp(2, JdbcUtils
												.getSqlTimestamp(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY)));
										pstmtInsert.setInt(3, pEncId);
										pstmtInsert.setInt(4, rsLoop.getInt("CASS_VALUE"));
										pstmtInsert.setInt(5, 0);
										pstmtInsert.setInt(6, 0);
										pstmtInsert.setInt(7, 0);
										pstmtInsert.setInt(8, 0);
										pstmtInsert.setInt(9, rsLoop.getInt("CASS_CURR"));
										pstmtInsert.setInt(10, remaining);
										pstmtInsert.setInt(11, rsLoop.getInt("CASS_NUMBER"));
										pstmtInsert.setDouble(12, 1);
										pstmtInsert.addBatch();
										recordsCount++;
									}

									if (fillStatDays > 0) {
										pstmtSelect = connection.prepareStatement(downtimeCoSelectSQL);
										pstmtSelect.setString(1, pPid);
										pstmtSelect.setTimestamp(2,
												JdbcUtils.getSqlTimestamp(DateUtils.addHours(
														DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY),
														fillStatDays)));
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											availCoeff = rs.getDouble(1);
										} else {
											availCoeff = 1;
										}
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);

										pstmtSelect = connection.prepareStatement(cassStatSelectSQL);
										pstmtSelect.setString(1, pPid);
										pstmtSelect.setInt(2, pEncId);
										pstmtSelect.setInt(3, rsLoop.getInt("CASS_NUMBER"));
										pstmtSelect.setTimestamp(4,
												JdbcUtils.getSqlTimestamp(DateUtils.addHours(
														DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY),
														fillStatDays - 1)));
										rs = pstmtSelect.executeQuery();
										if (rs.next()) {
											denomRemaining = rs.getInt("CASS_REMAINING");
											// statDate =
											// JdbcUtils.getTimestamp(rs.getTimestamp("STAT_DATE"));
										}
										JdbcUtils.close(rs);
										JdbcUtils.close(pstmtSelect);

										pstmtInsert.clearParameters();
										pstmtInsert.setString(1, pPid);
										pstmtInsert.setTimestamp(2,
												JdbcUtils.getSqlTimestamp(DateUtils.addHours(pEncDate, 1)));
										pstmtInsert.setInt(3, pEncId);
										pstmtInsert.setInt(4, rsLoop.getInt("CASS_VALUE"));
										pstmtInsert.setInt(5, 0);
										pstmtInsert.setInt(6, 0);
										pstmtInsert.setInt(7, 0);
										pstmtInsert.setInt(8, 0);
										pstmtInsert.setInt(9, rsLoop.getInt("CASS_CURR"));
										pstmtInsert.setInt(10, denomRemaining);
										pstmtInsert.setInt(11, rsLoop.getInt("CASS_NUMBER"));
										pstmtInsert.setDouble(12, availCoeff);
										pstmtInsert.addBatch();
										recordsCount++;
									}
								}

							} finally {
								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);
							}

							fillStatDays++;
						}
					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(pstmtSelect);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertZeroTakeOffsForCICass  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertZeroTakeOffsForCICurr(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2) throws SQLException {
		_logger.debug("Executing insertZeroTakeOffsForCICurr method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		Boolean method_status = false;
		int fillStatDays = 0;
		// Date fillStatDate;

		int recordsCount = 0;

		String ciStatDetailsLoopSQL = "SELECT distinct CASS_CURR as CURR_CODE " + " FROM T_CM_CASHIN_R_CASS_STAT "
				+ " WHERE CASH_IN_ENCASHMENT_ID = ?";

		String fillstatDateSelectSQL = "SELECT STAT_DATE " + " FROM t_cm_cashin_r_curr_stat " + " WHERE ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND CURR_CODE = ? " + " AND STAT_DATE = ? ";

		String insertSQL = "INSERT INTO T_CM_CASHIN_R_CURR_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,CURR_CODE,CURR_SUMM_IN,CURR_SUMM_OUT,CURR_TRANS_COUNT_IN, CURR_TRANS_COUNT_OUT) "
				+ " VALUES " + " (? , ?, ? , ?, ?, ?, ?, ?)";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false) {
				table1.set(true);
				table2.set(true);

				pstmtLoop = connection.prepareStatement(ciStatDetailsLoopSQL);
				pstmtLoop.setInt(1, pEncId);
				rsLoop = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(insertSQL);
				try {
					while (rsLoop.next()) {
						fillStatDays = 0;
						while (DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)
								.compareTo(DateUtils.truncate(pNextEncDate, Calendar.HOUR_OF_DAY)) <= 0) {
							try {
								if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

								} else {
									pstmtInsert.executeBatch();
									pstmtInsert.clearBatch();
									recordsCount = 0;
								}

								pstmtSelect = connection.prepareStatement(fillstatDateSelectSQL);
								pstmtSelect.setString(1, pPid);
								pstmtSelect.setInt(2, pEncId);
								pstmtSelect.setInt(3, rsLoop.getInt("CURR_CODE"));
								pstmtSelect.setTimestamp(4, JdbcUtils.getSqlTimestamp(DateUtils
										.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));

								rs = pstmtSelect.executeQuery();

								if (rs.next()) {
									// fillStatDate =
									// rs.getTimestamp("STAT_DATE");
								} else {

									pstmtInsert.clearParameters();
									pstmtInsert.setString(1, pPid);
									pstmtInsert.setTimestamp(2,
											JdbcUtils.getSqlTimestamp(DateUtils.addHours(pEncDate, fillStatDays)));
									pstmtInsert.setInt(3, pEncId);
									pstmtInsert.setInt(4, rsLoop.getInt("CURR_CODE"));
									pstmtInsert.setInt(5, 0);
									pstmtInsert.setInt(6, 0);
									pstmtInsert.setInt(6, 0);
									pstmtInsert.setInt(7, 0);
									pstmtInsert.addBatch();
									recordsCount++;
								}
							} finally {
								JdbcUtils.close(rs);
								JdbcUtils.close(pstmtSelect);
							}

							fillStatDays++;
						}
					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(pstmtSelect);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertZeroTakeOffsForCICurr  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertCashInStatMd(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3) throws SQLException {
		Boolean method_status = false;
		int recordsCount = 0;

		String transLoopSQL = " SELECT cs.BILLS_COUNT,cs.STAT_DATE,COALESCE(ds.AVAIL_COEFF,1) as AVAIL_COEFF "
				+ " FROM " + " (select ? as PID,sum(note_cash_in) as BILLS_COUNT,truncToHour(datetime) as stat_Date "
				+ " from t_cm_intgr_trans_md " + " WHERE oper_type in (" + EXCHANGE_TRANSACTION_TYPE + ","
				+ CREDIT_TRANSACTION_TYPE + ") " + " and terminal_id = ? " + " and datetime between ? and ? "
				+ " group by truncToHour(datetime) " + " order by stat_date) cs "
				+ " left outer join t_cm_intgr_downtime_cashin ds on (cs.PID = ds.PID and cs.stat_Date = ds.stat_date)";

		String insertSQL = "INSERT INTO T_CM_CASHIN_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,BILLS_COUNT,AVAIL_COEFF) " + " VALUES "
				+ " (? , ?, ?, ?, ?)";

		String checkInsertSQL = "select ATM_ID from T_CM_CASHIN_STAT "
				+ " where ATM_ID=? and STAT_DATE=? and CASH_IN_ENCASHMENT_ID=?";

		String updateSQL = "UPDATE T_CM_CASHIN_STAT " + " SET BILLS_COUNT = BILLS_COUNT + ?, " + " AVAIL_COEFF = ? "
				+ " WHERE " + " ATM_ID = ? " + " AND " + " STAT_DATE = ? " + " AND " + " CASH_IN_ENCASHMENT_ID = ?";
		// String cassStatFillInsertSQL

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtInsertCheck = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		ResultSet rs = null;
		ResultSet rsCheck = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {
				table1.set(true);
				table2.set(true);
				table3.set(true);
				pstmtLoop = connection.prepareStatement(transLoopSQL);
				pstmtLoop.setString(1, pPid);
				pstmtLoop.setString(2, pPid);
				pstmtLoop.setTimestamp(3, JdbcUtils.getSqlTimestamp(pEncDate));
				pstmtLoop.setTimestamp(4, JdbcUtils.getSqlTimestamp(pNextEncDate));

				rsLoop = pstmtLoop.executeQuery();

				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtUpdate = connection.prepareStatement(updateSQL);
				pstmtInsertCheck = connection.prepareStatement(checkInsertSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}

						pstmtInsertCheck.clearParameters();
						pstmtInsertCheck.setString(1, pPid);
						pstmtInsertCheck.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
						pstmtInsertCheck.setInt(3, pEncId);
						rsCheck = pstmtInsertCheck.executeQuery();
						if (rsCheck.next()) {
							pstmtUpdate.clearParameters();

							pstmtUpdate.setInt(1, rsLoop.getInt("BILLS_COUNT"));
							pstmtUpdate.setDouble(2, rsLoop.getDouble("AVAIL_COEFF"));
							pstmtUpdate.setString(3, pPid);
							pstmtUpdate.setTimestamp(4, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("stat_date")));
							pstmtUpdate.setInt(5, pEncId);
							pstmtUpdate.addBatch();
							recordsCount++;
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(rsLoop.getTimestamp("STAT_DATE")));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, rsLoop.getInt("BILLS_COUNT"));
							pstmtInsert.setDouble(5, rsLoop.getDouble("AVAIL_COEFF"));
							pstmtInsert.addBatch();
							recordsCount++;
						}

					}
					if (recordsCount > 0 && recordsCount <= BATCH_MAX_SIZE) {
						pstmtUpdate.executeBatch();
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(rsCheck);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtInsertCheck);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertCashINStatMD executed for PID: " + pPid);
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertZeroDaysForCashIn(Connection connection, String pPid, int pEncId, Date pEncDate,
			Date pNextEncDate, AtomicBoolean table1, AtomicBoolean table2, AtomicBoolean table3) throws SQLException {
		_logger.debug("Executing insertZeroDaysForCashIn method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		_logger.debug("pEncDate: " + pEncDate.toString());
		_logger.debug("pNextEncDate: " + pNextEncDate.toString());
		Boolean method_status = false;
		int fillStatDays = 0;
		double availCoeff = 0.0;
		// Date fillStatDate;

		int recordsCount = 0;

		ArrayList<Integer> currList = new ArrayList<Integer>();
		// getIntegerValueByQuery() as result
		String currListSelectSQL = "SELECT DISTINCT DENOM_CURR " + " FROM t_cm_cashin_denom_stat";

		String availCoeffSelectSQL = "SELECT COALESCE(ds.AVAIL_COEFF,1) " + " FROM t_cm_intgr_downtime_cashin ds "
				+ " WHERE ds.PID = ? " + " AND ds.STAT_DATE = ?";

		String fillStatDateSelectSQL = "SELECT DISTINCT STAT_DATE " + " FROM T_CM_CASHIN_STAT " + " WHERE ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE = ?";

		String fillStatDateForCurrSelectSQL = "SELECT DISTINCT STAT_DATE " + " FROM T_CM_CASHIN_DENOM_STAT "
				+ " WHERE ATM_ID = ? " + " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE = ? "
				+ " AND DENOM_CURR = ?";

		String insertSQL = "INSERT INTO T_CM_CASHIN_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,BILLS_COUNT,AVAIL_COEFF) " + " VALUES "
				+ " (? , ?, ?, ?, ?)";

		String insertDenomSQL = "INSERT INTO T_CM_CASHIN_DENOM_STAT "
				+ " (ATM_ID,STAT_DATE,CASH_IN_ENCASHMENT_ID,DENOM_CURR,DENOM_COUNT,DENOM_VALUE) " + " VALUES "
				+ " (? , ?, ?, ?, ?, ?)";

		PreparedStatement pstmtSelect = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtDenomInsert = null;
		PreparedStatement pstmtCurrListSelect = null;
		ResultSet rs = null;
		while (method_status == false) {
			if (table1.get() == false & table2.get() == false & table3.get() == false) {
				table1.set(true);
				table2.set(true);
				table3.set(true);
				try {
					pstmtCurrListSelect = connection.prepareStatement(currListSelectSQL);
					rs = pstmtCurrListSelect.executeQuery();
					while (rs.next()) {
						currList.add(rs.getInt("DENOM_CURR"));
					}
				} finally {
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtCurrListSelect);
				}

				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtDenomInsert = connection.prepareStatement(insertDenomSQL);

				recordsCount = 0;
				try {
					while (DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)
							.compareTo(DateUtils.truncate(pNextEncDate, Calendar.HOUR_OF_DAY)) <= 0) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtInsert.executeBatch();
							pstmtInsert.clearBatch();
							pstmtDenomInsert.executeBatch();
							pstmtDenomInsert.clearBatch();
							recordsCount = 0;
						}

						pstmtSelect = connection.prepareStatement(availCoeffSelectSQL);
						pstmtSelect.setString(1, pPid);
						pstmtSelect.setTimestamp(2, JdbcUtils.getSqlTimestamp(
								DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
						rs = pstmtSelect.executeQuery();

						if (rs.next()) {
							availCoeff = rs.getDouble(1);
						} else {
							availCoeff = 1;
						}
						JdbcUtils.close(rs);
						JdbcUtils.close(pstmtSelect);

						pstmtSelect = connection.prepareStatement(fillStatDateSelectSQL);
						pstmtSelect.setString(1, pPid);
						pstmtSelect.setInt(2, pEncId);
						pstmtSelect.setTimestamp(3, JdbcUtils.getSqlTimestamp(
								DateUtils.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
						rs = pstmtSelect.executeQuery();

						if (rs.next()) {
							// fillStatDate =
							// JdbcUtils.getTimestamp(rs.getTimestamp("STAT_DATE"));
						} else {
							pstmtInsert.clearParameters();
							pstmtInsert.setString(1, pPid);
							pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(DateUtils
									.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
							pstmtInsert.setInt(3, pEncId);
							pstmtInsert.setInt(4, 0);
							pstmtInsert.setDouble(5, availCoeff);
							pstmtInsert.addBatch();
							recordsCount++;
						}

						for (Integer curr : currList) {
							pstmtSelect = connection.prepareStatement(fillStatDateForCurrSelectSQL);
							pstmtSelect.setString(1, pPid);
							pstmtSelect.setInt(2, pEncId);
							pstmtSelect.setTimestamp(3, JdbcUtils.getSqlTimestamp(DateUtils
									.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
							pstmtSelect.setInt(4, curr);

							rs = pstmtSelect.executeQuery();

							if (rs.next()) {
								// fillStatDate =
								// JdbcUtils.getTimestamp(rs.getTimestamp("STAT_DATE"));
							} else {
								pstmtDenomInsert.clearParameters();
								pstmtDenomInsert.setString(1, pPid);
								pstmtDenomInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(DateUtils
										.addHours(DateUtils.truncate(pEncDate, Calendar.HOUR_OF_DAY), fillStatDays)));
								pstmtDenomInsert.setInt(3, pEncId);
								pstmtDenomInsert.setInt(4, curr);
								pstmtDenomInsert.setInt(5, 0);
								pstmtDenomInsert.setInt(6, 0);
								pstmtDenomInsert.addBatch();
								recordsCount++;
							}
						}

						fillStatDays += 1;

					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtInsert.executeBatch();
					}
				} finally {
					table1.set(false);
					table2.set(false);
					table3.set(false);
					JdbcUtils.close(rs);
					JdbcUtils.close(pstmtDenomInsert);
					JdbcUtils.close(pstmtInsert);
					JdbcUtils.close(pstmtSelect);
				}
				method_status = true;
				_logger.debug("Method insertZeroDaysForCashIn  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insertRemainingsForCashIn(Connection connection, String pPid, int pEncId, AtomicBoolean table1)
			throws SQLException {
		_logger.debug("Executing insertRemainingsForCashIn method for PID: " + pPid + " with parameters:");
		_logger.debug("pEncId: " + pEncId);
		Boolean method_status = false;
		int remaining = 0;

		int recordsCount;

		String ciStatLoopSQL = "SELECT STAT_DATE, BILLS_COUNT " + " FROM T_CM_CASHIN_STAT " + " WHERE ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " ORDER BY STAT_DATE";

		String updateSQL = " UPDATE T_CM_CASHIN_STAT " + " SET BILLS_REMAINING = ? " + " WHERE ATM_ID = ? "
				+ " AND CASH_IN_ENCASHMENT_ID = ? " + " AND STAT_DATE = ?";

		PreparedStatement pstmtLoop = null;
		PreparedStatement pstmtUpdate = null;
		ResultSet rsLoop = null;
		while (method_status == false) {
			if (table1.get() == false) {
				table1.set(true);
				pstmtLoop = connection.prepareStatement(ciStatLoopSQL);
				pstmtLoop.setString(1, pPid);
				pstmtLoop.setInt(2, pEncId);

				rsLoop = pstmtLoop.executeQuery();

				pstmtUpdate = connection.prepareStatement(updateSQL);

				recordsCount = 0;
				try {
					while (rsLoop.next()) {
						if (recordsCount > 0 && recordsCount < BATCH_MAX_SIZE) {

						} else {
							pstmtUpdate.executeBatch();
							pstmtUpdate.clearBatch();
							recordsCount = 0;
						}
						remaining += rsLoop.getInt("BILLS_COUNT");
						pstmtUpdate.clearParameters();
						pstmtUpdate.setInt(1, remaining);
						pstmtUpdate.setString(2, pPid);
						pstmtUpdate.setInt(3, pEncId);
						pstmtUpdate.setTimestamp(4, rsLoop.getTimestamp("STAT_DATE"));
						pstmtUpdate.addBatch();
						recordsCount++;

					}
					if (recordsCount > 0) {// && recordsCount <= BATCH_MAX_SIZE
						pstmtUpdate.executeBatch();
					}
				} finally {
					table1.set(false);
					JdbcUtils.close(rsLoop);
					JdbcUtils.close(pstmtUpdate);
					JdbcUtils.close(pstmtLoop);
				}
				method_status = true;
				_logger.debug("Method insertRemainingsForCashIn  for PID: " + pPid + " executed");
			} else {
				try {
					// _logger.debug("Waiting, while another thread will be
					// completed");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void insert_downtime_stat_cashout(Connection connection, String pPid, Date pStatDate, Double pKoeff)
			throws SQLException {
		/*
		 * _logger.
		 * debug("Executing insert_downtime_stat_cashout method for PID: "
		 * +pPid+" with parameters:");
		 * _logger.debug("pStatDate: "+pStatDate.toString());
		 * _logger.debug("pKoeff: "+pKoeff);
		 */
		double availCoeff = 0.0;

		String selectSQL = "SELECT COALESCE(AVAIL_COEFF,1) " + " FROM t_cm_intgr_downtime_cashout " + " WHERE PID = ? "
				+ " AND STAT_DATE = ?";

		String updateSQL = " UPDATE t_cm_intgr_downtime_cashout " + "SET AVAIL_COEFF = ? " + "WHERE " + "PID = ? "
				+ "AND " + "STAT_DATE = ?";

		String insertSQL = "INSERT INTO t_cm_intgr_downtime_cashout " + " (PID,STAT_DATE,AVAIL_COEFF) " + " VALUES "
				+ " (?,?,?)";

		PreparedStatement pstmt = null;
		PreparedStatement pstmtUpdate = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rs = null;

		try {
			pstmt = connection.prepareStatement(selectSQL);
			pstmt.setString(1, pPid);
			pstmt.setTimestamp(2, JdbcUtils.getSqlTimestamp(pStatDate));

			rs = pstmt.executeQuery();

			if (rs.next()) {
				availCoeff = rs.getDouble(1);
				pstmtUpdate = connection.prepareStatement(updateSQL);
				pstmtUpdate.setDouble(1, Math.max(availCoeff - pKoeff, 0));
				pstmtUpdate.setString(2, pPid);
				pstmtUpdate.setTimestamp(3, JdbcUtils.getSqlTimestamp(pStatDate));
				pstmtUpdate.executeUpdate();
			} else {
				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtInsert.setString(1, pPid);
				pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(pStatDate));
				pstmtInsert.setDouble(3, Math.max(1 - pKoeff, 0));
				pstmtInsert.executeUpdate();
			}

		} finally {
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmtUpdate);
			JdbcUtils.close(pstmtInsert);
			JdbcUtils.close(pstmt);
		}
		// _logger.debug("Method insert_downtime_stat_cashout for PID: "+pPid+"
		// executed");
	}

	public static void insert_downtime_stat_cashin(Connection connection, String pPid, Date pStatDate, Double pKoeff)
			throws SQLException {
		/*
		 * _logger.
		 * debug("Executing insert_downtime_stat_cashin method for PID: "
		 * +pPid+" with parameters:");
		 * _logger.debug("pStatDate: "+pStatDate.toString());
		 * _logger.debug("pKoeff: "+pKoeff);
		 */
		double availCoeff = 0.0;
		// insertZeroDaysForCashIn_getAvailCoeff()
		String selectSQL = "SELECT COALESCE(AVAIL_COEFF,1) " + " FROM t_cm_intgr_downtime_cashin " + " WHERE PID = ? "
				+ " AND STAT_DATE = ?";

		String updateSQL = " UPDATE t_cm_intgr_downtime_cashin " + " SET AVAIL_COEFF = ? " + " WHERE " + " PID = ? "
				+ " AND " + " STAT_DATE = ?";

		String insertSQL = "INSERT INTO t_cm_intgr_downtime_cashin " + " (PID,STAT_DATE,AVAIL_COEFF) " + " VALUES "
				+ " (?,?,?)";

		PreparedStatement pstmt = null;
		PreparedStatement pstmtUpdate = null;
		PreparedStatement pstmtInsert = null;
		ResultSet rs = null;

		try {
			pstmt = connection.prepareStatement(selectSQL);
			pstmt.setString(1, pPid);
			pstmt.setTimestamp(2, JdbcUtils.getSqlTimestamp(pStatDate));

			rs = pstmt.executeQuery();

			if (rs.next()) {
				availCoeff = rs.getDouble(1);
				pstmtUpdate = connection.prepareStatement(updateSQL);
				pstmtUpdate.setDouble(1, Math.max(availCoeff - pKoeff, 0));
				pstmtUpdate.setString(2, pPid);
				pstmtUpdate.setTimestamp(3, JdbcUtils.getSqlTimestamp(pStatDate));
				pstmtUpdate.executeUpdate();
			} else {
				pstmtInsert = connection.prepareStatement(insertSQL);
				pstmtInsert.setString(1, pPid);
				pstmtInsert.setTimestamp(2, JdbcUtils.getSqlTimestamp(pStatDate));
				pstmtInsert.setDouble(3, Math.max(1 - pKoeff, 0));
				pstmtInsert.executeUpdate();
			}

		} finally {
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmtUpdate);
			JdbcUtils.close(pstmtInsert);
			JdbcUtils.close(pstmt);
		}
		// _logger.debug("Method insert_downtime_stat_cashin for PID: "+pPid+"
		// executed");
	}

}
