package ru.bpc.cm.encashments;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.encashments.orm.AtmEncashmentReportMapper;
import ru.bpc.cm.items.encashments.HourRemaining;

public class AtmEncashmentReportController {

	private static Class<AtmEncashmentReportMapper> getMapperClass() {
		return AtmEncashmentReportMapper.class;
	}

	public static List<HourRemaining> getCashInStatRemain(ISessionHolder sessionHolder, int atmId, Date statsEndDate,
			long cashInVolume) throws SQLException {
		List<HourRemaining> report = new ArrayList<HourRemaining>();
		List<HourRemaining> cashInVolumeList = new ArrayList<HourRemaining>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		Calendar startCal = Calendar.getInstance();
		startCal.setTime(statsEndDate);
		startCal.add(Calendar.DAY_OF_YEAR, -2);

		try {
			List<HourRemaining> items = session.getMapper(getMapperClass()).getCashInStatRemain(
					new Timestamp(statsEndDate.getTime()), new Timestamp(startCal.getTimeInMillis()), atmId);

			for (HourRemaining item : items) {
				Date date = item.getStatDate();
				// TODO fix remaining with if-else
				report.add(new HourRemaining(CashManagementConstants.CASH_IN_CURR_CODE_A3,
						date.before(statsEndDate) ? item.getRemaining() : cashInVolume - item.getRemaining(), date,
						item.isEndOfStatsDate()));

				cashInVolumeList.add(new HourRemaining(CashManagementConstants.CASH_IN_VOLUME,
						date.before(statsEndDate) ? cashInVolume - item.getRemaining() : item.getRemaining(), date,
						item.isEndOfStatsDate()));
			}
			report.addAll(cashInVolumeList);
		} finally {
			session.close();
		}
		return report;
	}

	public static List<HourRemaining> getCashOutStatRemain(ISessionHolder sessionHolder, int atmId, Date statsEndDate)
			throws SQLException {
		List<HourRemaining> report = new ArrayList<HourRemaining>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		Calendar startCal = Calendar.getInstance();
		startCal.setTime(statsEndDate);
		startCal.add(Calendar.DAY_OF_YEAR, -2);

		try {
			report.addAll(session.getMapper(getMapperClass()).getCashOutStatRemain(
					new Timestamp(statsEndDate.getTime()), new Timestamp(startCal.getTimeInMillis()), atmId));
		} finally {
			session.close();
		}
		return report;
	}

	public static List<HourRemaining> getCashRecCurrStatRemain(ISessionHolder sessionHolder, int atmId,
			Date statsEndDate) throws SQLException {
		List<HourRemaining> report = new ArrayList<HourRemaining>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		Calendar startCal = Calendar.getInstance();
		startCal.setTime(statsEndDate);
		startCal.add(Calendar.DAY_OF_YEAR, -2);

		try {
			report.addAll(session.getMapper(getMapperClass()).getCashRecCurrStatRemain(
					new Timestamp(statsEndDate.getTime()), new Timestamp(startCal.getTimeInMillis()), atmId));
		} finally {
			session.close();
		}
		return report;
	}

	public static List<HourRemaining> getCashRecBillsStatRemain(ISessionHolder sessionHolder, int atmId,
			Date statsEndDate) throws SQLException {
		List<HourRemaining> report = new ArrayList<HourRemaining>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		Calendar startCal = Calendar.getInstance();
		startCal.setTime(statsEndDate);
		startCal.add(Calendar.DAY_OF_YEAR, -2);

		try {
			report.addAll(session.getMapper(getMapperClass()).getCashRecBillsStatRemain(
					new Timestamp(statsEndDate.getTime()), new Timestamp(startCal.getTimeInMillis()), atmId));
		} finally {
			session.close();
		}
		return report;
	}

}
