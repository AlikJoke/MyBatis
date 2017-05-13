package ru.bpc.cm.reports;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.items.reports.RepCurrStatItem;
import ru.bpc.cm.items.reports.RepCurrStatRecItem;
import ru.bpc.cm.items.reports.RepDenomStatItem;
import ru.bpc.cm.items.reports.RepDenomStatRecItem;
import ru.bpc.cm.items.reports.ReportFilter;
import ru.bpc.cm.reports.orm.ReportsDenomCurrMapper;
import ru.bpc.cm.utils.IFilterItem;

public class ReportsDenomCurrController {

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<ReportsDenomCurrMapper> getMapperClass() {
		return ReportsDenomCurrMapper.class;
	}

	public static Map<Integer, List<RepCurrStatItem>> getReportCoCurrStat(ISessionHolder sessionHolder,
			ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepCurrStatItem>> report = new LinkedHashMap<Integer, List<RepCurrStatItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ReportsDenomCurrMapper mapper = session.getMapper(getMapperClass());
			int currentAtmID = 0;
			List<RepCurrStatItem> stats = new ArrayList<RepCurrStatItem>();
			List<RepCurrStatItem> repStats = mapper.getReportCoCurrStat(new Timestamp(repFilter.getDateTo().getTime()),
					new Timestamp(repFilter.getDateFrom().getTime()));
			boolean isFirstAtm = true;
			// Date forEncDateTo = null;
			Date lastEncDate = null;
			for (RepCurrStatItem repItem : repStats) {
				if (repItem.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, stats);
					}
					currentAtmID = repItem.getAtmID();
					stats = new ArrayList<RepCurrStatItem>();
					isFirstAtm = false;
					lastEncDate = null;
				}
				RepCurrStatItem item = new RepCurrStatItem();
				item.setAtmID(currentAtmID);
				item.setEncDateFrom(repItem.getEncDateFrom());
				if (lastEncDate != null) {
					item.setEncDateTo(lastEncDate);
				}
				lastEncDate = item.getEncDateFrom();
				item.setCurrCode(repItem.getCurrCode());
				item.setCurrCodeA3(repItem.getCurrCodeA3());
				item.setLoaded(repItem.getLoaded());
				item.setWithdrawal(repItem.getWithdrawal());
				item.setRemaining(repItem.getRemaining());
				item.setLeftBeforeCashAdd(repItem.getLeftBeforeCashAdd());
				item.setTransCount(repItem.getTransCount());
				item.setCurrCount(repItem.getCurrCount());
				item.setLineNumber(repItem.getLineNumber());
				item.setSummTrans(repItem.getSummTrans());
				item.setRowspanStyleClass(item.getCurrCount() == 1 ? "rowspanColumnOneRow"
						: (item.getCurrCount() == item.getLineNumber() ? "rowspanColumnFirstRow"
								: (item.getLineNumber() == 1 ? "rowspanColumnLastRow" : "rowspanColumnMiddleRow")));
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
		for (IFilterItem<Integer> atm : filterList) {
			if (report.get(atm.getValue()) == null) {
				report.put(atm.getValue(), new ArrayList<RepCurrStatItem>());
			}
		}
		return report;
	}

	public static Map<Integer, List<RepCurrStatRecItem>> getReportCrCurrStat(ISessionHolder sessionHolder,
			ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepCurrStatRecItem>> report = new LinkedHashMap<Integer, List<RepCurrStatRecItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ReportsDenomCurrMapper mapper = session.getMapper(getMapperClass());
			int currentAtmID = 0;
			List<RepCurrStatRecItem> stats = new ArrayList<RepCurrStatRecItem>();
			List<RepCurrStatRecItem> repStats = mapper.getReportCrCurrStat(
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()));
			boolean isFirstAtm = true;
			// Date forEncDateTo = null;
			Date lastEncDate = null;
			for (RepCurrStatRecItem repItem : repStats) {
				if (repItem.getAtmID() != currentAtmID) {
					if (!isFirstAtm) {
						report.put(currentAtmID, stats);
					}
					currentAtmID = repItem.getAtmID();
					stats = new ArrayList<RepCurrStatRecItem>();
					isFirstAtm = false;
					lastEncDate = null;
				}
				RepCurrStatRecItem item = new RepCurrStatRecItem();
				item.setAtmID(currentAtmID);
				item.setEncDateFrom(repItem.getEncDateFrom());
				if (lastEncDate != null) {
					item.setEncDateTo(lastEncDate);
				}
				lastEncDate = item.getEncDateFrom();
				item.setCurrCode(repItem.getCurrCode());
				item.setCurrCodeA3(repItem.getCurrCodeA3());
				item.setCurrSummIn(repItem.getCurrSummIn());
				item.setCurrSummOut(repItem.getCurrSummOut());
				item.setLoaded(repItem.getLoaded());
				item.setRemaining(repItem.getLoaded());
				item.setTransCountIn(repItem.getTransCountIn());
				item.setTransCountOut(repItem.getTransCountOut());
				item.setCurrCount(repItem.getCurrCount());
				item.setLineNumber(repItem.getLineNumber());
				item.setSummTransIn(repItem.getSummTransIn());
				item.setSummTransOut(repItem.getSummTransOut());
				item.setRowspanStyleClass(item.getCurrCount() == 1 ? "rowspanColumnOneRow"
						: (item.getCurrCount() == item.getLineNumber() ? "rowspanColumnFirstRow"
								: (item.getLineNumber() == 1 ? "rowspanColumnLastRow" : "rowspanColumnMiddleRow")));
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
		for (IFilterItem<Integer> atm : filterList) {
			if (report.get(atm.getValue()) == null) {
				report.put(atm.getValue(), new ArrayList<RepCurrStatRecItem>());
			}
		}
		return report;
	}

	public static Map<Integer, List<RepDenomStatItem>> getReportCoDenomStat(ISessionHolder sessionHolder,
			ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepDenomStatItem>> report = new LinkedHashMap<Integer, List<RepDenomStatItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		if (!filterList.isEmpty()) {
			try {
				ReportsDenomCurrMapper mapper = session.getMapper(getMapperClass());
				int currentAtmID = 0;
				List<RepDenomStatItem> stats = new ArrayList<RepDenomStatItem>();
				List<RepDenomStatItem> repStats = mapper.getReportCoDenomStat(repFilter.isDenomReportSplit(),
						new Timestamp(repFilter.getDateTo().getTime()),
						new Timestamp(repFilter.getDateFrom().getTime()));
				boolean isFirstAtm = true;

				for (RepDenomStatItem repItem : repStats) {
					if (repItem.getAtmID() != currentAtmID) {
						if (!isFirstAtm) {
							report.put(currentAtmID, stats);
						}
						currentAtmID = repItem.getAtmID();
						stats = new ArrayList<RepDenomStatItem>();
						isFirstAtm = false;
					}
					RepDenomStatItem item = new RepDenomStatItem();
					item.setAtmID(currentAtmID);

					item.setEncDateFrom(repItem.getEncDateFrom());
					item.setEncDateTo(repItem.getEncDateTo());

					item.setDenomCurrCodeA3(repItem.getDenomCurrCodeA3());
					item.setDenomValue(repItem.getDenomValue());
					item.setDenomCountIn(repItem.getDenomCountIn());
					item.setDenomCountOut(repItem.getDenomCountOut());
					item.setTransCount(repItem.getTransCount());
					if (item.getDenomCountIn() == 0) {
						item.setInOutCoeff(0);
					} else {
						BigDecimal inOutCoeff = new BigDecimal(
								(double) item.getDenomCountOut() / (double) item.getDenomCountIn());
						inOutCoeff = inOutCoeff.setScale(2, BigDecimal.ROUND_UP);
						item.setInOutCoeff(inOutCoeff.doubleValue());
					}
					if (Double.valueOf(repItem.getTransCoeff()).intValue() == 0) {
						item.setTransCoeff(0);
					} else {
						BigDecimal transCoeff = new BigDecimal(item.getTransCount() / repItem.getTransCoeff());
						transCoeff = transCoeff.setScale(2, BigDecimal.ROUND_UP);
						item.setTransCoeff(transCoeff.doubleValue());
					}
					if (item.getTransCount() == 0) {
						item.setCountInTransCoeff(0);
					} else {
						BigDecimal countInTransCoeff = new BigDecimal(
								(double) item.getDenomCountOut() / (double) item.getTransCount());
						countInTransCoeff = countInTransCoeff.setScale(2, BigDecimal.ROUND_UP);
						item.setCountInTransCoeff(countInTransCoeff.doubleValue());
					}
					item.setDenomCount(repItem.getDenomCount());
					item.setLineNumber(repItem.getLineNumber());
					item.setRowspanStyleClass(item.getDenomCount() == 1 ? "rowspanColumnOneRow"
							: (item.getDenomCount() == item.getLineNumber() ? "rowspanColumnFirstRow"
									: (item.getLineNumber() == 1 ? "rowspanColumnLastRow" : "rowspanColumnMiddleRow")));

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
			for (IFilterItem<Integer> atm : filterList) {
				if (report.get(atm.getValue()) == null) {
					report.put(atm.getValue(), new ArrayList<RepDenomStatItem>());
				}
			}
		}

		return report;
	}

	public static List<RepDenomStatItem> getReportCoDenomStatForAtm(ISessionHolder sessionHolder,
			ReportFilter repFilter, int atmId) {
		List<RepDenomStatItem> stats = new ArrayList<RepDenomStatItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			ReportsDenomCurrMapper mapper = session.getMapper(getMapperClass());
			List<RepDenomStatItem> repStats = mapper.getReportCoDenomStatForAtm(repFilter.isDenomReportSplit(), atmId,
					new Timestamp(repFilter.getDateTo().getTime()), new Timestamp(repFilter.getDateFrom().getTime()));

			for (RepDenomStatItem repItem : repStats) {
				RepDenomStatItem item = new RepDenomStatItem();
				item.setAtmID(atmId);

				item.setEncDateFrom(repItem.getEncDateFrom());
				item.setEncDateTo(repItem.getEncDateTo());

				item.setDenomCurrCodeA3(repItem.getDenomCurrCodeA3());
				item.setDenomValue(repItem.getDenomValue());
				item.setDenomCountIn(repItem.getDenomCountIn());
				item.setDenomCountOut(repItem.getDenomCountOut());
				item.setTransCount(repItem.getTransCount());
				if (item.getDenomCountIn() == 0) {
					item.setInOutCoeff(0);
				} else {
					BigDecimal inOutCoeff = new BigDecimal(
							(double) item.getDenomCountOut() / (double) item.getDenomCountIn());
					inOutCoeff = inOutCoeff.setScale(2, BigDecimal.ROUND_UP);
					item.setInOutCoeff(inOutCoeff.doubleValue());
				}
				if (Double.valueOf(repItem.getTransCoeff()).intValue() == 0) {
					item.setTransCoeff(0);
				} else {
					BigDecimal transCoeff = new BigDecimal(item.getTransCount() / repItem.getTransCoeff());
					transCoeff = transCoeff.setScale(2, BigDecimal.ROUND_UP);
					item.setTransCoeff(transCoeff.doubleValue());
				}
				if (item.getTransCount() == 0) {
					item.setCountInTransCoeff(0);
				} else {
					BigDecimal countInTransCoeff = new BigDecimal(
							(double) item.getDenomCountOut() / (double) item.getTransCount());
					countInTransCoeff = countInTransCoeff.setScale(2, BigDecimal.ROUND_UP);
					item.setCountInTransCoeff(countInTransCoeff.doubleValue());
				}
				item.setDenomCount(repItem.getDenomCount());
				item.setLineNumber(repItem.getLineNumber());
				item.setRowspanStyleClass(item.getDenomCount() == 1 ? "rowspanColumnOneRow"
						: (item.getDenomCount() == item.getLineNumber() ? "rowspanColumnFirstRow"
								: (item.getLineNumber() == 1 ? "rowspanColumnLastRow" : "rowspanColumnMiddleRow")));

				stats.add(item);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}

		return stats;
	}

	public static Map<Integer, List<RepDenomStatRecItem>> getReportCrDenomStat(ISessionHolder sessionHolder,
			ReportFilter repFilter, List<IFilterItem<Integer>> filterList) {
		Map<Integer, List<RepDenomStatRecItem>> report = new LinkedHashMap<Integer, List<RepDenomStatRecItem>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		BigDecimal transCoeff = null;

		if (!filterList.isEmpty()) {
			try {
				ReportsDenomCurrMapper mapper = session.getMapper(getMapperClass());
				int currentAtmID = 0;

				List<RepDenomStatRecItem> repStats = mapper.getReportCrDenomStat(repFilter.isDenomReportSplit(),
						new Timestamp(repFilter.getDateTo().getTime()),
						new Timestamp(repFilter.getDateFrom().getTime()));
				List<RepDenomStatRecItem> stats = new ArrayList<RepDenomStatRecItem>();
				boolean isFirstAtm = true;

				for (RepDenomStatRecItem repItem : repStats) {
					if (repItem.getAtmID() != currentAtmID) {
						if (!isFirstAtm) {
							report.put(currentAtmID, stats);
						}
						currentAtmID = repItem.getAtmID();
						stats = new ArrayList<RepDenomStatRecItem>();
						isFirstAtm = false;
					}
					RepDenomStatRecItem item = new RepDenomStatRecItem();
					item.setAtmID(currentAtmID);

					item.setEncDateFrom(repItem.getEncDateFrom());
					item.setEncDateTo(repItem.getEncDateTo());

					item.setDenomCurrCodeA3(repItem.getDenomCurrCodeA3());
					item.setDenomValue(repItem.getDenomValue());
					item.setDenomCountIn(repItem.getDenomCountIn());
					item.setDenomCountOut(repItem.getDenomCountOut());
					item.setTransCountIn(repItem.getTransCountIn());
					item.setTransCountOut(repItem.getTransCountOut());
					item.setDenomLoaded(repItem.getDenomLoaded());
					if (item.getDenomCountIn() == 0) {
						item.setInOutCoeff(0);
					} else {
						BigDecimal inOutCoeff = new BigDecimal(
								(double) item.getDenomCountOut() / (double) item.getDenomCountIn());
						inOutCoeff = inOutCoeff.setScale(2, BigDecimal.ROUND_UP);
						item.setInOutCoeff(inOutCoeff.doubleValue());
					}
					if (Double.valueOf(repItem.getTransCoeffIn()).intValue() == 0) {
						item.setTransCoeffIn(0);
					} else {
						transCoeff = new BigDecimal(item.getTransCountIn() / repItem.getTransCoeffIn());
						transCoeff = transCoeff.setScale(2, BigDecimal.ROUND_UP);
						item.setTransCoeffIn(transCoeff.doubleValue());
					}
					if (Double.valueOf(repItem.getTransCoeffOut()).intValue() == 0) {
						item.setTransCoeffOut(0);
					} else {
						transCoeff = new BigDecimal(item.getTransCountIn() / repItem.getTransCoeffOut());
						transCoeff = transCoeff.setScale(2, BigDecimal.ROUND_UP);
						item.setTransCoeffOut(transCoeff.doubleValue());
					}
					item.setDenomCount(repItem.getDenomCount());
					item.setLineNumber(repItem.getLineNumber());
					item.setRowspanStyleClass(item.getDenomCount() == 1 ? "rowspanColumnOneRow"
							: (item.getDenomCount() == item.getLineNumber() ? "rowspanColumnFirstRow"
									: (item.getLineNumber() == 1 ? "rowspanColumnLastRow" : "rowspanColumnMiddleRow")));

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
			for (IFilterItem<Integer> atm : filterList) {
				if (report.get(atm.getValue()) == null) {
					report.put(atm.getValue(), new ArrayList<RepDenomStatRecItem>());
				}
			}
		}

		return report;
	}

	public static List<RepDenomStatRecItem> getReportCrDenomStatForAtm(ISessionHolder sessionHolder,
			ReportFilter repFilter, int atmId) {
		List<RepDenomStatRecItem> stats = new ArrayList<RepDenomStatRecItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		BigDecimal transCoeff = null;

		try {
			ReportsDenomCurrMapper mapper = session.getMapper(getMapperClass());
			List<RepDenomStatRecItem> repStats = mapper.getReportCrDenomStatForAtm(repFilter.isDenomReportSplit(),
					atmId, new Timestamp(repFilter.getDateTo().getTime()),
					new Timestamp(repFilter.getDateFrom().getTime()));

			for (RepDenomStatRecItem repItem : repStats) {
				RepDenomStatRecItem item = new RepDenomStatRecItem();
				item.setAtmID(atmId);

				item.setEncDateFrom(repItem.getEncDateFrom());
				item.setEncDateTo(repItem.getEncDateTo());

				item.setDenomCurrCodeA3(repItem.getDenomCurrCodeA3());
				item.setDenomValue(repItem.getDenomValue());
				item.setDenomCountIn(repItem.getDenomCountIn());
				item.setDenomCountOut(repItem.getDenomCountOut());
				item.setTransCountIn(repItem.getTransCountIn());
				item.setTransCountOut(repItem.getTransCountOut());
				item.setDenomLoaded(repItem.getDenomLoaded());
				if (item.getDenomCountIn() == 0) {
					item.setInOutCoeff(0);
				} else {
					BigDecimal inOutCoeff = new BigDecimal(
							(double) item.getDenomCountOut() / (double) item.getDenomCountIn());
					inOutCoeff = inOutCoeff.setScale(2, BigDecimal.ROUND_UP);
					item.setInOutCoeff(inOutCoeff.doubleValue());
				}
				if (Double.valueOf(repItem.getTransCoeffIn()).intValue() == 0) {
					item.setTransCoeffIn(0);
				} else {
					transCoeff = new BigDecimal(item.getTransCountIn() / repItem.getTransCoeffIn());
					transCoeff = transCoeff.setScale(2, BigDecimal.ROUND_UP);
					item.setTransCoeffIn(transCoeff.doubleValue());
				}
				if (Double.valueOf(repItem.getTransCoeffOut()).intValue() == 0) {
					item.setTransCoeffOut(0);
				} else {
					transCoeff = new BigDecimal(item.getTransCountIn() / repItem.getTransCoeffOut());
					transCoeff = transCoeff.setScale(2, BigDecimal.ROUND_UP);
					item.setTransCoeffOut(transCoeff.doubleValue());
				}
				item.setDenomCount(repItem.getDenomCount());
				item.setLineNumber(repItem.getLineNumber());
				item.setRowspanStyleClass(item.getDenomCount() == 1 ? "rowspanColumnOneRow"
						: (item.getDenomCount() == item.getLineNumber() ? "rowspanColumnFirstRow"
								: (item.getLineNumber() == 1 ? "rowspanColumnLastRow" : "rowspanColumnMiddleRow")));

				stats.add(item);
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}

		return stats;
	}

}
