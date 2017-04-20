package ru.bpc.cm.forecasting.controllers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.orm.items.NominalCountItem;
import ru.bpc.cm.forecasting.orm.ForecastNominalsMapper;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.enums.CalendarDayType;
import ru.bpc.cm.items.enums.CurrencyMode;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.forecast.nominal.NominalItem;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByCassCount;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByDenomAsc;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByDenomDesc;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByTakeOff;
import ru.bpc.cm.utils.ObjectPair;

public class ForecastNominalsController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<ForecastNominalsMapper> getMapperClass() {
		return ForecastNominalsMapper.class;
	}

	/**
	 * Makes List of denominations that are used in ATM. Also it adds to each
	 * denomination item statistic coefficients
	 * 
	 * @param con
	 *            - DB connection
	 * @param atmID
	 *            - ATM ID
	 * @param currency
	 *            - Code of currency of denominations
	 * @param encID
	 *            - ID of encashment, which is used as a priod for getting
	 *            denomination list and statistics
	 * @param cassCountForCurrency
	 *            - number of ATM cassets that are available for currency
	 * @return List of denominations
	 */
	public static final List<NominalItem> getCurrNominals(ISessionHolder sessionHolder, int atmId, int currency,
			List<Integer> encList, int cassCountForCurrency) {
		List<NominalItem> nominals = new ArrayList<NominalItem>(cassCountForCurrency);

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			nominals.addAll(session.getMapper(getMapperClass()).getCurrNominals(encList, currency, atmId));
			Collections.sort(nominals, new NominalItemComparatorByTakeOff());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return nominals;
	}

	public static final List<NominalItem> getCoCurrNominalsFromAtmCassettes(ISessionHolder sessionHolder, int atmId,
			int currency, List<Integer> encList, int minCountInOneCass, int maxCountInOneCass) {
		List<NominalItem> nominals = new ArrayList<NominalItem>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ForecastNominalsMapper mapper = session.getMapper(getMapperClass());
			nominals.addAll(
					mapper.getCoCurrNominalsFromAtmCassettes(atmId, currency, AtmCassetteType.CASH_OUT_CASS.getId()));

			for (NominalItem item : nominals) {
				item.setCassCount(1);
				item.setCurrency(currency);
				item.setMinCountInOneCass(minCountInOneCass);

				if (item.getMaxCountInOneCass() <= 0)
					item.setMaxCountInOneCass(maxCountInOneCass);

				NominalCountItem countItem = mapper.getCoCurrNominalsFromAtmCassettes_count(encList, currency, atmId,
						item.getDenom());

				if (countItem != null) {
					item.setCountLast(countItem.getCountTrans() == 0 ? 1 : countItem.getDenomCount());
					item.setDayTakeOffCoeff(countItem.getCountTrans() / countItem.getCountDays());

					if (countItem.getCountTrans() == 0)
						item.setDenomMultipleCoeff(1);
					else
						item.setDenomMultipleCoeff(countItem.getDenomCount() / countItem.getCountTrans());
				} else
					item.setCountLast(1);
			}
			Collections.sort(nominals, new NominalItemComparatorByTakeOff());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return nominals;
	}

	public static final List<NominalItem> getCrCurrNominalsFromAtmCassettes(ISessionHolder sessionHolder, int atmId,
			int currency, List<Integer> encList, int minCountInOneCass, int maxCountInOneCass) {
		List<NominalItem> nominals = new ArrayList<NominalItem>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ForecastNominalsMapper mapper = session.getMapper(getMapperClass());
			nominals.addAll(
					mapper.getCrCurrNominalsFromAtmCassettes(atmId, currency, AtmCassetteType.CASH_IN_R_CASS.getId()));

			for (NominalItem item : nominals) {
				item.setCassCount(1);
				item.setCurrency(currency);
				item.setMinCountInOneCass(minCountInOneCass);

				if (item.getMaxCountInOneCass() <= 0)
					item.setMaxCountInOneCass(maxCountInOneCass);

				NominalCountItem countItem = mapper.getCrCurrNominalsFromAtmCassettes_count(encList, currency, atmId,
						item.getDenom());
				if (countItem != null) {
					item.setCountLast(countItem.getCountTrans() == 0 ? 1 : countItem.getDenomCount());
					item.setDayTakeOffCoeff(countItem.getCountTrans() / countItem.getCountDays());

					if (countItem.getCountTrans() == 0)
						item.setDenomMultipleCoeff(1);
					else
						item.setDenomMultipleCoeff(countItem.getDenomCount() / countItem.getCountTrans());
				} else
					item.setCountLast(1);
			}

			Collections.sort(nominals, new NominalItemComparatorByTakeOff());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return nominals;
	}

	public static double getAvgDenomDemandStat(ISessionHolder sessionHolder, int atmId, Date endDate, int currency,
			int denom, int dayStart, int dayEnd) {

		Calendar calStart = Calendar.getInstance();
		calStart.setTime(endDate);
		calStart.add(Calendar.DAY_OF_YEAR, -3);

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Double avgDenomDemandStat = session.getMapper(getMapperClass()).getAvgDenomDemandStat(atmId, currency,
					dayEnd, dayStart, CalendarDayType.ATM_NOT_AVAILABLE.getId(), new Timestamp(endDate.getTime()),
					new Timestamp(calStart.getTime().getTime()), CurrencyMode.CASH_IN.name(), denom);
			if (avgDenomDemandStat != null) {
				return avgDenomDemandStat;
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return 0.0;
	}

	/**
	 * Adds cassets with nominal or changes denomination to higher one. Used
	 * when encashment sum can not be reached by current denomination set.
	 * 
	 * @param con
	 *            - DB connection
	 * @param cassCountForCurrency
	 *            - number of ATM cassets that are available for currency.
	 * @param nominals
	 *            - List of denominations to be changed
	 * @return modified List of denominations
	 * @throws ForecastException
	 */
	public static final List<NominalItem> changeNominals(ISessionHolder sessionHolder, int cassCountForCurrency,
			List<NominalItem> nominals) throws ForecastException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		ForecastNominalsMapper mapper = session.getMapper(getMapperClass());
		int nomCassCount = 0;
		for (NominalItem i : nominals) {
			nomCassCount += i.getCassCount();
		}

		if (nomCassCount < cassCountForCurrency) {
			NominalItem item = Collections.max(nominals, new NominalItemComparatorByTakeOff());
			item.setCassCount(item.getCassCount() + 1);
		} else {
			NominalItem item = Collections.max(nominals, new NominalItemComparatorByCassCount());
			if (item.getCassCount() > 1) {
				if (item.equals(Collections.max(nominals, new NominalItemComparatorByDenomAsc()))) {
					try {
						Integer denom = mapper.changeNominals(item.getCurrency(), item.getDenom());
						if (denom != null) {
							int newDenom = denom;
							if (newDenom > 0) {
								// if (item.getDenomMultipleCoeff() >= (double)
								// newDenom / (double) item.getDenom()) {
								item.setCassCount(item.getCassCount() - 1);

								NominalItem newItem = new NominalItem();
								newItem.setCassCount(1);
								newItem.setDayTakeOffCoeff(item.getDayTakeOffCoeff());
								newItem.setCurrency(item.getCurrency());
								newItem.setDenom(newDenom);
								newItem.setDenomMultipleCoeff(
										item.getDayTakeOffCoeff() / ((double) newDenom / (double) item.getDenom()));
								newItem.setCountLast(item.getCountLast() / (newDenom / item.getDenom()));
								nominals.add(newItem);
							} else
								throw new ForecastException(
										ForecastException.CAN_NOT_CHANGE_DENOMINATION_HIGHER_NOT_AVAILABLE);
						} else
							throw new ForecastException(
									ForecastException.CAN_NOT_CHANGE_DENOMINATION_HIGHER_NOT_AVAILABLE);
					} catch (Exception e) {
						logger.error("", e);
					} finally {
						session.close();
					}
				} else {
					Collections.sort(nominals, new NominalItemComparatorByDenomDesc());
					int maxDenom = nominals.get(0).getDenom();
					for (NominalItem i : nominals) {
						if (i.getDenom() <= maxDenom) {
							i.setCassCount(i.getCassCount() + 1);
							break;
						}
					}
					item.setCassCount(item.getCassCount() - 1);
				}
			} else {
				try {
					item = Collections.max(nominals, new NominalItemComparatorByDenomDesc());
					Integer denom = mapper.changeNominals(item.getCurrency(), item.getDenom());
					if (denom != null) {
						int newDenom = denom;
						if (newDenom > 0)
							if (item.getDenomMultipleCoeff() >= (double) newDenom / (double) item.getDenom())
								item.setDenom(newDenom);
							else
								throw new ForecastException(
										ForecastException.CAN_NOT_CHANGE_DENOMINATION_TAKEOFF_COEFF);
						else
							throw new ForecastException(
									ForecastException.CAN_NOT_CHANGE_DENOMINATION_HIGHER_NOT_AVAILABLE);
					} else
						throw new ForecastException(ForecastException.CAN_NOT_CHANGE_DENOMINATION_HIGHER_NOT_AVAILABLE);
				} catch (Exception e) {
					logger.error("", e);
				} finally {
					session.close();
				}
			}
		}

		return nominals;
	}

	public static double getDenomRemaining(ISessionHolder sessionHolder, int atmId, int currency, int encID,
			Date startDate, int denom) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		double res = 0;
		Date lastStatDate = null;
		try {
			ForecastNominalsMapper mapper = session.getMapper(getMapperClass());
			List<ObjectPair<Double, Timestamp>> denomRemainingWithDate = mapper.getDenomRemaining_withStatDate(atmId,
					currency, encID, new Timestamp(startDate.getTime()), denom);

			if (!denomRemainingWithDate.isEmpty()) {
				res = denomRemainingWithDate.get(0).getKey();
				lastStatDate = denomRemainingWithDate.get(0).getValue();
			}

			Double denomRemaining = mapper.getDenomRemaining(atmId, currency, encID,
					new Timestamp(lastStatDate.getTime()), denom);

			if (denomRemaining != null)
				res -= denomRemaining;

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}
}
