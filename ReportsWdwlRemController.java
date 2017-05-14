package ru.bpc.cm.reports;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.cashmanagement.CurrencyConverter;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.reports.RepCurrWdwlRemItem;
import ru.bpc.cm.items.reports.RepCurrWdwlRemRecItem;
import ru.bpc.cm.items.reports.RepDenomWdwlRemItem;
import ru.bpc.cm.items.reports.RepDenomWdwlRemRecItem;
import ru.bpc.cm.items.reports.ReportFilter;
import ru.bpc.cm.reports.orm.ReportsWdwlRemMapper;
import ru.bpc.cm.utils.IFilterItem;

public class ReportsWdwlRemController {

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<ReportsWdwlRemMapper> getMapperClass() {
		return ReportsWdwlRemMapper.class;
	}

	public static Map<Integer, List<RepCurrWdwlRemItem>> getReportCoCurrWithdrawalRemainAtm(
			ISessionHolder sessionHolder, ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepCurrWdwlRemItem>> report = new LinkedHashMap<Integer, List<RepCurrWdwlRemItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		int lostCurrCode = 0;
		int atmId = 0;
		CurrencyConverter converter = null;

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());
			List<RepCurrWdwlRemItem> repStats = mapper.getReportCoCurrWithdrawalRemainAtm(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress());

			int currentAtmID = 0;
			List<RepCurrWdwlRemItem> stats = new ArrayList<RepCurrWdwlRemItem>();
			boolean isFirstAtm = true;

			for (RepCurrWdwlRemItem repItem : repStats) {
				if (repItem.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, stats);
					}
					currentAtmID = repItem.getAtmID();
					stats = new ArrayList<RepCurrWdwlRemItem>();
					isFirstAtm = false;
				}
				RepCurrWdwlRemItem item = new RepCurrWdwlRemItem();
				item.setAtmID(currentAtmID);
				item.setCurrCode(repItem.getCurrCode());
				item.setStatDate(repItem.getStatDate());
				item.setCurrCodeA3(repItem.getCurrCodeA3());

				item.setCurrSumm(repItem.getCurrSumm());
				item.setCurrRemaining(repItem.getCurrRemaining());
				item.setEncashmentId(repItem.getEncashmentId());

				stats.add(item);
			}
			if (currentAtmID != 0) {
				report.put(currentAtmID, stats);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		for (Entry<Integer, List<RepCurrWdwlRemItem>> rep : report.entrySet()) {
			atmId = rep.getKey();
			if (repFilter.getWdwlRemCurrCode() == 0) {
				lostCurrCode = 0;
				try {
					lostCurrCode = Integer.parseInt(
							CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.REGION_CURR_CODE));
				} catch (ForecastException e) {
					_logger.error("", e);
				}
				if (lostCurrCode > 0) {
					converter = new CurrencyConverter(sessionHolder, atmId, lostCurrCode);
					rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, rep.getValue(), converter,
							lostCurrCode));
				} else {
					rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, rep.getValue(), null, 0));
				}
			} else {
				rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, rep.getValue(), null, 0));
			}
		}
		return report;
	}

	public static List<RepCurrWdwlRemItem> getReportCoCurrWithdrawalRemainGroup(ISessionHolder sessionHolder,
			ReportFilter repFilter, int groupId) {
		List<RepCurrWdwlRemItem> report = new ArrayList<RepCurrWdwlRemItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());
			report.addAll(mapper.getReportCoCurrWithdrawalRemainGroup(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress()));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, report, null, 0);
	}

	public static Map<Integer, List<RepCurrWdwlRemRecItem>> getReportCrCurrWithdrawalRemainAtm(
			ISessionHolder sessionHolder, Connection con, ReportFilter repFilter,
			List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepCurrWdwlRemRecItem>> report = new LinkedHashMap<Integer, List<RepCurrWdwlRemRecItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		int atmId = 0;
		int lostCurrCode = 0;
		CurrencyConverter converter = null;

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());

			int currentAtmID = 0;
			List<RepCurrWdwlRemRecItem> stats = new ArrayList<RepCurrWdwlRemRecItem>();
			List<RepCurrWdwlRemRecItem> repStats = mapper.getReportCrCurrWithdrawalRemainAtm(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress());
			boolean isFirstAtm = true;

			for (RepCurrWdwlRemRecItem repItem : repStats) {
				if (repItem.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, stats);
					}
					currentAtmID = repItem.getAtmID();
					stats = new ArrayList<RepCurrWdwlRemRecItem>();
					isFirstAtm = false;
				}
				repItem.setAtmID(currentAtmID);
			}
			stats.addAll(repStats);
			if (currentAtmID != 0) {
				report.put(currentAtmID, stats);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		for (Entry<Integer, List<RepCurrWdwlRemRecItem>> rep : report.entrySet()) {
			atmId = rep.getKey();
			if (repFilter.getWdwlRemCurrCode() == 0) {
				lostCurrCode = 0;
				try {
					lostCurrCode = Integer.parseInt(
							CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.REGION_CURR_CODE));
				} catch (ForecastException e) {
					_logger.error("", e);
				}
				if (lostCurrCode > 0) {
					converter = new CurrencyConverter(sessionHolder, atmId, lostCurrCode);
					rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemainRec(repFilter, rep.getValue(), converter,
							lostCurrCode));
				} else {
					rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemainRec(repFilter, rep.getValue(), null, 0));
				}
			} else {
				rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemainRec(repFilter, rep.getValue(), null, 0));
			}
		}
		return report;
	}

	public static List<RepCurrWdwlRemRecItem> getReportCrCurrWithdrawalRemainGroup(ISessionHolder sessionHolder,
			ReportFilter repFilter, int groupId) {
		List<RepCurrWdwlRemRecItem> report = new ArrayList<RepCurrWdwlRemRecItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());
			report.addAll(mapper.getReportCrCurrWithdrawalRemainGroup(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress()));
			for (RepCurrWdwlRemRecItem item : report) {
				item.setAtmID(groupId);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return ReportsUtils.aggregateCurrWithdrawalRemainRec(repFilter, report, null, 0);
	}

	public static Map<Integer, List<RepCurrWdwlRemItem>> getReportCiBillWithdrawalRemainAtm(
			ISessionHolder sessionHolder, ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepCurrWdwlRemItem>> report = new LinkedHashMap<Integer, List<RepCurrWdwlRemItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());

			int currentAtmID = 0;
			List<RepCurrWdwlRemItem> stats = new ArrayList<RepCurrWdwlRemItem>();
			boolean isFirstAtm = true;

			stats.addAll(mapper.getReportCiBillWithdrawalRemainAtm(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					Integer.valueOf(repFilter.getAtmId()), repFilter.getNameAndAddress()));

			for (RepCurrWdwlRemItem item : stats) {
				if (item.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, ReportsUtils.aggregateCashInWithdrawalRemain(repFilter, stats));
					}
					currentAtmID = item.getAtmID();
					stats = new ArrayList<RepCurrWdwlRemItem>();
					isFirstAtm = false;
				}
				item.setAtmID(currentAtmID);
			}
			if (currentAtmID != 0) {
				report.put(currentAtmID, ReportsUtils.aggregateCashInWithdrawalRemain(repFilter, stats));
			}
			for (IFilterItem<Integer> atm : filterList) {
				if (report.get(atm.getValue()) == null) {
					report.put(atm.getValue(), new ArrayList<RepCurrWdwlRemItem>());
				}
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return report;
	}

	public static List<RepCurrWdwlRemItem> getReportCiBillWithdrawalRemainGroup(ISessionHolder sessionHolder,
			ReportFilter repFilter, int groupId) {
		List<RepCurrWdwlRemItem> report = new ArrayList<RepCurrWdwlRemItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());
			report.addAll(mapper.getReportCiBillWithdrawalRemainGroup(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					Integer.valueOf(repFilter.getAtmId()), repFilter.getNameAndAddress()));
			for (RepCurrWdwlRemItem item : report) {
				item.setAtmID(groupId);
				item.setCurrCode(CashManagementConstants.CASH_IN_CURR_CODE);
				item.setCurrCodeA3(CashManagementConstants.CASH_IN_CURR_CODE_A3);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return ReportsUtils.aggregateCashInWithdrawalRemain(repFilter, report);
	}

	public static Map<Integer, List<RepCurrWdwlRemItem>> getReportCiCurrWithdrawalRemainAtm(
			ISessionHolder sessionHolder, ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepCurrWdwlRemItem>> report = new LinkedHashMap<Integer, List<RepCurrWdwlRemItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		int atmId = 0;
		int lostCurrCode = 0;
		CurrencyConverter converter = null;

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());

			int currentAtmID = 0;
			List<RepCurrWdwlRemItem> stats = new ArrayList<RepCurrWdwlRemItem>();
			stats.addAll(
					mapper.getReportCiCurrWithdrawalRemainAtm(repFilter, new Timestamp(repFilter.getDateTo().getTime()),
							new Timestamp(repFilter.getDateFrom().getTime()), repFilter.getWdwlRemCurrCode(),
							Integer.valueOf(repFilter.getAtmId()), repFilter.getNameAndAddress()));
			boolean isFirstAtm = true;

			for (RepCurrWdwlRemItem item : stats) {
				if (item.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, stats);
					}
					currentAtmID = item.getAtmID();
					stats = new ArrayList<RepCurrWdwlRemItem>();
					isFirstAtm = false;
				}
				item.setAtmID(currentAtmID);
			}
			if (currentAtmID != 0) {
				report.put(currentAtmID, stats);
			}

		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		for (Entry<Integer, List<RepCurrWdwlRemItem>> rep : report.entrySet()) {
			atmId = rep.getKey();
			if (repFilter.getWdwlRemCurrCode() == 0) {
				lostCurrCode = 0;
				try {
					lostCurrCode = Integer.parseInt(
							CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.REGION_CURR_CODE));
				} catch (ForecastException e) {
					_logger.error("", e);
				}
				if (lostCurrCode > 0) {
					converter = new CurrencyConverter(sessionHolder, atmId, lostCurrCode);
					rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, rep.getValue(), converter,
							lostCurrCode));
				} else {
					rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, rep.getValue(), null, 0));
				}
			} else {
				rep.setValue(ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, rep.getValue(), null, 0));
			}
		}
		for (IFilterItem<Integer> atm : filterList) {
			if (report.get(atm.getValue()) == null) {
				report.put(atm.getValue(), new ArrayList<RepCurrWdwlRemItem>());
			}
		}
		return report;
	}

	public static List<RepCurrWdwlRemItem> getReportCiCurrWithdrawalRemainGroup(ISessionHolder sessionHolder,
			ReportFilter repFilter, int groupId) {
		List<RepCurrWdwlRemItem> report = new ArrayList<RepCurrWdwlRemItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());
			report.addAll(mapper.getReportCiCurrWithdrawalRemainGroup(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress()));
			for (RepCurrWdwlRemItem item : report) {
				item.setAtmID(groupId);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return ReportsUtils.aggregateCurrWithdrawalRemain(repFilter, report, null, 0);
	}

	public static Map<Integer, List<RepDenomWdwlRemItem>> getReportCoDenomWithdrawalRemainAtm(
			ISessionHolder sessionHolder, ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepDenomWdwlRemItem>> report = new LinkedHashMap<Integer, List<RepDenomWdwlRemItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());

			int currentAtmID = 0;
			List<RepDenomWdwlRemItem> stats = new ArrayList<RepDenomWdwlRemItem>();
			stats.addAll(mapper.getReportCoDenomWithdrawalRemainAtm(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress()));
			boolean isFirstAtm = true;

			for (RepDenomWdwlRemItem item : stats) {
				if (item.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, stats);
					}
					currentAtmID = item.getAtmID();
					stats = new ArrayList<RepDenomWdwlRemItem>();
					isFirstAtm = false;
				}
				item.setAtmID(currentAtmID);
			}
			if (currentAtmID != 0) {
				report.put(currentAtmID, stats);
			}

		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		for (Entry<Integer, List<RepDenomWdwlRemItem>> rep : report.entrySet()) {
			rep.setValue(ReportsUtils.aggregateDenomWithdrawalRemain(repFilter, rep.getValue()));
		}
		for (IFilterItem<Integer> atm : filterList) {
			if (report.get(atm.getValue()) == null) {
				report.put(atm.getValue(), new ArrayList<RepDenomWdwlRemItem>());
			}
		}
		return report;
	}

	public static List<RepDenomWdwlRemItem> getReportCoDenomWithdrawalRemainGroup(ISessionHolder sessionHolder,
			ReportFilter repFilter, int groupId) {
		List<RepDenomWdwlRemItem> report = new ArrayList<RepDenomWdwlRemItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());
			report.addAll(mapper.getReportCoDenomWithdrawalRemainGroup(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress()));

			for (RepDenomWdwlRemItem item : report) {
				item.setAtmID(groupId);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return ReportsUtils.aggregateDenomWithdrawalRemain(repFilter, report);
	}

	public static Map<Integer, List<RepDenomWdwlRemRecItem>> getReportCrDenomWithdrawalRemainAtm(
			ISessionHolder sessionHolder, ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepDenomWdwlRemRecItem>> report = new LinkedHashMap<Integer, List<RepDenomWdwlRemRecItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());

			int currentAtmID = 0;
			List<RepDenomWdwlRemRecItem> stats = new ArrayList<RepDenomWdwlRemRecItem>();
			stats.addAll(mapper.getReportCrDenomWithdrawalRemainAtm(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress()));
			boolean isFirstAtm = true;

			for (RepDenomWdwlRemRecItem item : stats) {
				if (item.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, stats);
					}
					currentAtmID = item.getAtmID();
					stats = new ArrayList<RepDenomWdwlRemRecItem>();
					isFirstAtm = false;
				}
				item.setAtmID(currentAtmID);
			}
			if (currentAtmID != 0) {
				report.put(currentAtmID, stats);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		for (Entry<Integer, List<RepDenomWdwlRemRecItem>> rep : report.entrySet()) {
			rep.setValue(ReportsUtils.aggregateDenomWithdrawalRemainRec(repFilter, rep.getValue()));
		}
		for (IFilterItem<Integer> atm : filterList) {
			if (report.get(atm.getValue()) == null) {
				report.put(atm.getValue(), new ArrayList<RepDenomWdwlRemRecItem>());
			}
		}
		return report;
	}

	public static List<RepDenomWdwlRemRecItem> getReportCrDenomWithdrawalRemainGroup(ISessionHolder sessionHolder,
			ReportFilter repFilter, int groupId) {
		List<RepDenomWdwlRemRecItem> report = new ArrayList<RepDenomWdwlRemRecItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsWdwlRemMapper mapper = session.getMapper(getMapperClass());
			report.addAll(mapper.getReportCrDenomWithdrawalRemainGroup(repFilter,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()),
					repFilter.getWdwlRemCurrCode(), Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress()));
			for (RepDenomWdwlRemRecItem item : report) {
				item.setAtmID(groupId);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return ReportsUtils.aggregateDenomWithdrawalRemainRec(repFilter, report);
	}

}
