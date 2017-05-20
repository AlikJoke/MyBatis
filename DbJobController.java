package ru.bpc.cm.integration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSession;

import ejbs.cm.svcm.CmDbJobBean;
import ejbs.cm.svcm.CmWebTask;
import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.exception.ExtendedSQLException;
import ru.bpc.cm.integration.db.ICashManagementAPI;
import ru.bpc.cm.integration.orm.DbJobMapper;
import ru.bpc.cm.integration.ws.ICashManagementWSAPI;
import ru.bpc.cm.orm.common.ORMUtils;

public class DbJobController {

	private static Class<DbJobMapper> getMapperClass() {
		return DbJobMapper.class;
	}

	private static void actualizeStats(ISessionHolder sessionHolder) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int shift = 0;

		try {
			DbJobMapper mapper = session.getMapper(getMapperClass());
			shift = ORMUtils.getNotNullValue(mapper.actualizeStats_getShift(), 0);

			mapper.actualizeStats_update(shift, "T_CM_CASHOUT_CASS_STAT", "STAT_DATE");
			mapper.actualizeStats_update(shift, "T_CM_CASHOUT_CURR_STAT", "STAT_DATE");
			mapper.actualizeStats_update(shift, "T_CM_REJECT_STAT", "STAT_DATE");
			mapper.actualizeStats_update(shift, "T_CM_ENC_CASHOUT_STAT", "ENC_DATE");

			mapper.actualizeStats_delete("T_CM_CASHOUT_CASS_STAT", "STAT_DATE");
			mapper.actualizeStats_delete("T_CM_CASHOUT_CURR_STAT", "STAT_DATE");
			mapper.actualizeStats_delete("T_CM_REJECT_STAT", "STAT_DATE");
			mapper.actualizeStats_deleteFromDetails();
			mapper.actualizeStats_delete("T_CM_ENC_CASHOUT_STAT", "ENC_DATE");

			mapper.actualizeStats_update(shift, "T_CM_CASHIN_R_CASS_STAT", "STAT_DATE");
			mapper.actualizeStats_update(shift, "T_CM_CASHIN_R_CURR_STAT", "STAT_DATE");
			mapper.actualizeStats_update(shift, "T_CM_CASHIN_STAT", "STAT_DATE");
			mapper.actualizeStats_update(shift, "T_CM_CASHIN_DENOM_STAT", "STAT_DATE");
			mapper.actualizeStats_update(shift, "T_CM_ENC_CASHIN_STAT", "CASH_IN_ENC_DATE");

			mapper.actualizeStats_delete("T_CM_CASHIN_R_CASS_STAT", "STAT_DATE");
			mapper.actualizeStats_delete("T_CM_CASHIN_R_CURR_STAT", "STAT_DATE");
			mapper.actualizeStats_delete("T_CM_CASHIN_DENOM_STAT", "STAT_DATE");
			mapper.actualizeStats_delete("T_CM_CASHIN_STAT", "STAT_DATE");
			mapper.actualizeStats_delete("T_CM_ENC_CASHIN_STAT", "CASH_IN_ENC_DATE");

			mapper.actualizeStats_update(shift, "t_cm_atm_calendar_days", "CL_DATE");
			mapper.actualizeStats_updateParams(shift);
		} finally {
			session.close();
		}
	}

	public static void executeLoadDictionariesJob(AtomicInteger interruptFlag, ISessionHolder sessionHolder,
			ICashManagementAPI api) throws SQLException {
		IntegrationController.load_inst_list(sessionHolder, api);
		IntegrationController.load_atm_list(sessionHolder, api);
		IntegrationController.load_currency_convert_rates(sessionHolder, api);
	}

	public static void executeLoadDictionariesRestJob(Connection connection, ICashManagementWSAPI restEjb)
			throws SQLException {
		IntegrationController.load_inst_list(connection, restEjb);
		IntegrationController.load_atm_list(connection, restEjb);
		IntegrationController.load_currency_convert_rates(connection, restEjb);
	}

	public static void executeLoadStatJob(AtomicInteger interruptFlag, ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api)
			throws SQLException {
		load_stat(interruptFlag, sessionHolder, connection, api);
	}

	public static void executeLoadErrorStatJob(AtomicInteger interruptFlag, ISessionHolder sessionHolder,
			ICashManagementAPI api) throws SQLException {
		load_error_stat(interruptFlag, sessionHolder, api);
	}

	public static void executeLoadStatRestJob(AtomicInteger interruptFlag, ISessionHolder sessionHolder, Connection connection,
			ICashManagementWSAPI restEjb) throws SQLException {
		load_stat(interruptFlag, sessionHolder, connection, restEjb);
	}

	public static void executeLoadStatJob(AtomicInteger interruptFlag, Connection connection, ICashManagementAPI api,
			ISessionHolder sessionHolder, Date dateFrom, List<Integer> atmList, Long lastUtrnno) throws SQLException {
		load_stat(interruptFlag, connection, sessionHolder, api, dateFrom, atmList, lastUtrnno);
	}

	public static void executeLoadStatRestJob(AtomicInteger interruptFlag, Connection connection,
			ISessionHolder sessionHolder, ICashManagementWSAPI restEjb, Date dateFrom, List<Integer> atmList, Long lastUtrnno) throws SQLException {
		load_stat(interruptFlag, connection, restEjb, sessionHolder, dateFrom, atmList, lastUtrnno);
	}

	public static void executeLoadReversalsRestJob(Connection connection, ICashManagementWSAPI restEjb,
			List<Integer> atmList, Long lastUtrnno, long transTo, int checkTime) throws SQLException {
		load_reversals(connection, restEjb, atmList, lastUtrnno, transTo, checkTime);
	}

	public static void executeLoadReversalsRestJob(Connection connection, ICashManagementWSAPI restEjb, Long lastUtrnno,
			long transTo, int checkTime) throws SQLException {
		load_reversals(connection, restEjb, lastUtrnno, transTo, checkTime);
	}

	public static void executeAggregateStatJob(DataSource dataSource, AtomicInteger interruptFlag,
			ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api, CmDbJobBean bean)
			throws SQLException, ExtendedSQLException, InterruptedException {
		// actualizeStats(connection);
		aggregate_stat(dataSource, interruptFlag, sessionHolder, connection, api, bean);
	}

	public static void executeAggregateStatErrAtmsJob(DataSource dataSource, AtomicInteger interruptFlag,
			ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api, CmDbJobBean bean)
			throws SQLException, ExtendedSQLException, InterruptedException {
		// actualizeStats(connection);
		aggregate_stat_err_atms(dataSource, interruptFlag, sessionHolder, connection, api, bean);
	}

	public static void executeAggregateStatJob(DataSource dataSource, ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api,
			Date dateFrom, List<Integer> atmList, Long lastUtrnno, int taskId, CmWebTask cmWebTaskEJB,
			CmDbJobBean jobBean) throws SQLException, InterruptedException {
		// actualizeStats(connection);
		aggregate_stat(dataSource, sessionHolder, connection, api, dateFrom, atmList, lastUtrnno, taskId, cmWebTaskEJB, jobBean);
	}

	public static void executeLoadAndAggregateStatJob(DataSource dataSource, AtomicInteger interruptFlag,
			ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api, Date dateFrom, Date dateTo, CmDbJobBean jobBean)
			throws SQLException, InterruptedException {
		load_and_aggregate_stat(dataSource, interruptFlag, sessionHolder, connection, api, dateFrom, dateTo, jobBean);
	}

	public static void executeClearIntgrTransJob(ISessionHolder sessionHolder, ICashManagementAPI api, List<Integer> atmList)
			throws SQLException {
		clearTrans(sessionHolder, api, atmList);
	}

	public static void executeClearIntgrTransJob(Connection connection, ICashManagementAPI api) throws SQLException {
		clearTrans(connection, api);
	}

	private static void clearTrans(ISessionHolder sessionHolder, ICashManagementAPI api, List<Integer> atmList)
			throws SQLException {
		DataLoadController.truncateTrans(sessionHolder, atmList);
	}

	private static void clearTrans(Connection connection, ICashManagementAPI api) throws SQLException {
		DataLoadController.truncateAllTrans(connection);
	}

	public static void executeLoadAtmCassettesBalancesJob(ISessionHolder sessionHolder, ICashManagementAPI api)
			throws SQLException {
		IntegrationController.load_atm_cassettes_balances(sessionHolder, api);
	}

	public static void executeLoadAtmCassettesBalancesRestJob(Connection connection, ICashManagementWSAPI restEjb)
			throws SQLException {
		IntegrationController.load_atm_cassettes_balances(connection, restEjb);
	}

	public static void executeLoadAtmCassettesBalancesJobForAtms(ISessionHolder sessionHolder, ICashManagementAPI api,
			List<Integer> atms) throws SQLException {
		IntegrationController.load_atm_cassettes_balances(sessionHolder, api, atms);
	}

	public static void executeLoadAtmCassettesBalancesRestJobForAtms(Connection connection,
			ICashManagementWSAPI restEjb, List<Integer> atms) throws SQLException {
		IntegrationController.load_atm_cassettes_balances(connection, restEjb, atms);
	}

	public static void executeLoadAtmCassettesStatusesJob(AtomicInteger interruptFlag, ISessionHolder sessionHolder,
			ICashManagementAPI api) throws SQLException {
		IntegrationController.load_atm_cassettes_statuses(interruptFlag, sessionHolder, api);
	}

	public static void executeLoadAtmCassettesStatusesRestJob(Connection connection, ICashManagementWSAPI restEjb)
			throws SQLException {
		IntegrationController.load_atm_cassettes_statuses(connection, restEjb);
	}

	public static void executeLoadAtmCassettesStatusesRestJob(Connection connection, ICashManagementWSAPI restEjb,
			List<Integer> atms) throws SQLException {
		IntegrationController.load_atm_cassettes_statuses(connection, restEjb, atms);
	}

	public static void executeDeleteOldStatsJob(ISessionHolder sessionHolder, int days) throws SQLException {
		DataLoadController.delete_old_stats(sessionHolder, days);
	}

	private static void load_stat(AtomicInteger interruptFlag, ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api)
			throws SQLException {
		if (!(interruptFlag.get() > 0)) {
			DataLoadController.truncateTrans(connection);
			IntegrationController.load_atm_trans(interruptFlag, sessionHolder, api);
			IntegrationController.load_atm_downtime(interruptFlag, sessionHolder, api);
			AggregationController.prepare_downtimes(interruptFlag, sessionHolder);
		} else {

		}

	}

	private static void load_error_stat(AtomicInteger interruptFlag, ISessionHolder sessionHolder, ICashManagementAPI api)
			throws SQLException {
		//DataLoadController.truncateTrans(sessionHolder);
		IntegrationController.load_atm_error_trans(sessionHolder, api);
		IntegrationController.load_atm_downtime(interruptFlag, sessionHolder, api);
		AggregationController.prepare_downtimes(interruptFlag, sessionHolder);
	}

	private static void load_stat(AtomicInteger interruptFlag, ISessionHolder sessionHolder, Connection connection, ICashManagementWSAPI restEjb)
			throws SQLException {
		DataLoadController.truncateTrans(connection);
		IntegrationController.load_atm_trans(connection, restEjb);
		IntegrationController.load_atm_downtime(connection, restEjb);
		//AggregationController.prepare_downtimes(interruptFlag, connection);
	}

	private static void load_stat(AtomicInteger interruptFlag, Connection connection, ISessionHolder sessionHolder, ICashManagementAPI api,
			Date dateFrom, List<Integer> atmList, Long lastUtrnno) throws SQLException {
		IntegrationController.load_atm_trans(sessionHolder, api, dateFrom, atmList, lastUtrnno);
		IntegrationController.load_atm_downtime(sessionHolder, api, atmList);
		//AggregationController.prepare_downtimes(interruptFlag, connection);

	}

	private static void load_stat(AtomicInteger interruptFlag, Connection connection, ICashManagementWSAPI restEjb,
			ISessionHolder sessionHolder, Date dateFrom, List<Integer> atmList, Long lastUtrnno) throws SQLException {
		IntegrationController.load_atm_trans(sessionHolder, restEjb, dateFrom, atmList, lastUtrnno);
		IntegrationController.load_atm_downtime(connection, restEjb, atmList);
		//AggregationController.prepare_downtimes(interruptFlag, connection);
	}

	private static void load_reversals(Connection connection, ICashManagementWSAPI restEjb, List<Integer> atmList,
			Long lastUtrnno, long transTo, int checkTime) {
		restEjb.loadReversals(atmList, lastUtrnno, transTo, checkTime);

	}

	private static void load_reversals(Connection connection, ICashManagementWSAPI restEjb, Long lastUtrnno,
			long transTo, int checkTime) {
		restEjb.loadReversals(lastUtrnno, transTo, checkTime);

	}

	private static void aggregate_stat(DataSource dataSource, AtomicInteger interruptFlag, ISessionHolder sessionHolder, Connection connection,
			ICashManagementAPI api, CmDbJobBean bean) throws SQLException, ExtendedSQLException, InterruptedException {
		//AggregationController.aggregate_cash_out(interruptFlag, dataSource, bean);
		//AggregationController.aggregate_cash_in(interruptFlag, dataSource);
		DataLoadController.saveParams(interruptFlag, sessionHolder);
		IntegrationController.load_atm_cassettes_statuses(interruptFlag, sessionHolder, api);
	}

	private static void aggregate_stat_err_atms(DataSource dataSource, AtomicInteger interruptFlag,
			ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api, CmDbJobBean bean)
			throws SQLException, ExtendedSQLException, InterruptedException {
		//AggregationController.aggregate_cash_out(interruptFlag, dataSource, bean);
		//AggregationController.aggregate_cash_in(interruptFlag, dataSource);
		DataLoadController.saveParamsForErrAtm(sessionHolder);
		IntegrationController.load_atm_cassettes_statuses(interruptFlag, sessionHolder, api);
	}

	private static void aggregate_stat(DataSource dataSource, ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api,
			Date dateFrom, List<Integer> atmList, Long lastUtrnno, int taskId, CmWebTask cmWebTaskEJB,
			CmDbJobBean jobBean) throws SQLException, InterruptedException {
		AggregationController.aggregate_cash_out(sessionHolder, dateFrom, atmList);
		AggregationController.aggregate_cash_in(sessionHolder, dateFrom, atmList);
		DataLoadController.saveParamsForTask(sessionHolder, cmWebTaskEJB, taskId, atmList);
		IntegrationController.load_atm_cassettes_statuses(sessionHolder, api, atmList);
	}

	private static void load_and_aggregate_stat(DataSource dataSource, AtomicInteger interruptFlag,
			ISessionHolder sessionHolder, Connection connection, ICashManagementAPI api, Date dateFrom, Date dateTo, CmDbJobBean jobBean)
			throws SQLException, InterruptedException {
		DataLoadController.truncateTrans(connection);
		// DataLoadController.truncateTrans(connection, dateFrom, dateTo);
		IntegrationController.load_atm_trans(sessionHolder, api, dateFrom, dateTo);
		IntegrationController.load_atm_downtime(sessionHolder, api, dateFrom, dateTo);
		//AggregationController.prepare_downtimes(interruptFlag, connection);
		//AggregationController.aggregate_cash_out(interruptFlag, dataSource, jobBean);
		//AggregationController.aggregate_cash_in(interruptFlag, dataSource);
		DataLoadController.saveParams(interruptFlag, sessionHolder);
		IntegrationController.load_atm_cassettes_statuses(interruptFlag, sessionHolder, api);
	}

}
