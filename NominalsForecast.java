package ru.bpc.cm.forecasting;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.forecasting.controllers.ForecastNominalsController;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.forecast.ForecastCurrencyItem;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.forecast.ForecastItem;
import ru.bpc.cm.items.forecast.nominal.NominalItem;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByCountInOneCass;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByDenomAsc;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByDenomDesc;


//DO NOT REMOVE THIS CLASS
public class NominalsForecast {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	protected static ForecastItem setEncNominals(Connection con, ForecastItem forecast, int atmID, List<Integer> encList)
	throws ForecastException {

		int maxDenomCount = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASH_OUT_CASS_VOLUME));
		int minDenomCount = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASH_OUT_CASS_MIN_LOAD));
		int loadType = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASS_LOADING_TYPE));
		
		ForecastCurrencyItem mainCurr = forecast.getEncashmentCurrencies().get(0);


		String atmInstId = CmCommonController.getInstIdForAtm(con, atmID);
		int maxCassCount = mainCurr.getMaxCassCount();
		List<NominalItem> nominals = new ArrayList<NominalItem>(maxCassCount);
		nominals = ForecastNominalsController.getCurrNominals(con, atmID, mainCurr.getCurrCode(), encList, maxCassCount);

		nominals = devideEncSumByNominalsMainCurr(con, nominals, mainCurr.getEncSummCalcNew(),
		        loadType, 0,
		        maxDenomCount, maxCassCount, forecast);

		nominals = mergeCass(nominals, minDenomCount, maxDenomCount);
		int cassCount = 0;
		for(NominalItem item : nominals){
			cassCount += item.getCassCount();
		}
		if (cassCount < maxCassCount) {
			nominals = extractCass(nominals, maxCassCount);
		}
		try {
			nominals = extractCassWhereTooFew(nominals, minDenomCount, maxDenomCount);
		} catch (ForecastException e) {
			logger.error("atmID = " + atmID,e);
			forecast.setNominalsRespCode(e.getCode());
		}
		Long sum = Long.valueOf(0);
		for (NominalItem j : nominals) {
			sum += j.getCountInOneCassPlan() * j.getCassCount() * j.getDenom();
		}
		mainCurr.setEncSummPlan(sum);
		mainCurr.setNominals(nominals);
		for (int i = 1; i < forecast.getEncashmentCurrencies().size(); i++) {
			maxCassCount = forecast.getEncashmentCurrencies().get(i).getMaxCassCount();
			if(maxCassCount > 0){
				nominals = new ArrayList<NominalItem>(maxCassCount);
				nominals = ForecastNominalsController.getCurrNominals(con, atmID, forecast.getEncashmentCurrencies().get(i).getCurrCode(), encList, maxCassCount);

				nominals = devideEncSumByNominalsSecCurr(con, nominals, forecast.getEncashmentCurrencies().get(i).getEncSummCalcNew(),
				        loadType, 0,
				        maxDenomCount, maxCassCount, forecast);

				nominals = mergeCass(nominals, minDenomCount, maxDenomCount);
				cassCount = 0;
				for(NominalItem item : nominals){
					cassCount += item.getCassCount();
				}
				if (cassCount < maxCassCount) {
					nominals = extractCass(nominals, maxCassCount);
				}
				try {
					nominals = extractCassWhereTooFew(nominals, minDenomCount, maxDenomCount);
				} catch (ForecastException e) {
					forecast.setNominalsRespCode(e.getCode());
				}
				sum = Long.valueOf(0);
				for (NominalItem j : nominals) {
					sum += j.getCountInOneCassPlan() * j.getCassCount() * j.getDenom();
				}
				forecast.getEncashmentCurrencies().get(i).setEncSummPlan(sum);
				forecast.getEncashmentCurrencies().get(i).setNominals(nominals);
			}
		}
		double encSummPlan = 0;
		for (ForecastCurrencyItem currency : forecast.getEncashmentCurrencies()) {
			encSummPlan += CmCommonController
			        .
			        convertValue(con, currency.getCurrCode(), forecast.getEncashmentCurrencies().get(0).getCurrCode(),
			                currency.getEncSummPlan().doubleValue(),atmInstId)
			        .doubleValue();
		}
		forecast.setEncSummPlan(encSummPlan);
		
		if(forecast.getNominalsRespCode() == ForecastItem.PROCEDURE_NOT_EXECUTED){
			forecast.setNominalsRespCode(ForecastItem.PROCEDURE_TERMINATED_SUCCESSFULLY);
		}
		
		return forecast;
	}

	private static List<NominalItem> devideEncSumByNominalsMainCurr(Connection con, List<NominalItem> nominals, double encSummCalc, int loadType,
	                                                             int iterations, int maxInCass, int maxCurrCassCount, ForecastItem forecast)
	{
		switch (loadType) {
		case 1:
			nominals = setPlanEncNomAverage(encSummCalc, maxInCass, nominals, 0);
			break;
		case 2:
			nominals = setPlanEncNomProportional(encSummCalc, maxInCass, nominals, 0);
			break;
		case 3:
			nominals = setPlanEncNomDownTop(encSummCalc, maxInCass, nominals, 0);
			break;
		case 4:
			nominals = setPlanEncNomTopDown(encSummCalc, maxInCass, nominals, 0);
			break;
		default:
		}
		if (!planSummIsEnough(nominals, encSummCalc) && iterations < 5) {
			try {
				nominals = ForecastNominalsController.changeNominals(con, maxCurrCassCount, nominals);
				devideEncSumByNominalsMainCurr(con, nominals, encSummCalc, loadType, iterations + 1, maxInCass, maxCurrCassCount,
				            forecast);
			} catch (ForecastException e) {
				logger.error("atmID = " + forecast.getAtmID(),e);
				forecast.setNominalsRespCode(e.getCode());
			}
		}
		return nominals;
	}

	private static List<NominalItem> devideEncSumByNominalsSecCurr(Connection con, List<NominalItem> nominals, double encSummCalc, int loadType,
	                                                            int iterations, int maxInCass, int maxCurrCassCount, ForecastItem forecast)
	{
		switch (loadType) {
		case 1:
			nominals = setPlanEncNomAverage(encSummCalc, maxInCass, nominals, 0);
			break;
		case 2:
			nominals = setPlanEncNomProportional(encSummCalc, maxInCass, nominals, 0);
			break;
		case 3:
			nominals = setPlanEncNomProportional(encSummCalc, maxInCass, nominals, 0);
			break;
		case 4:
			nominals = setPlanEncNomProportional(encSummCalc, maxInCass, nominals, 0);
			break;
		default:
		}
		if (!planSummIsEnough(nominals, encSummCalc) && iterations < 5) {
			try {
				nominals = ForecastNominalsController.changeNominals(con, maxCurrCassCount, nominals);
				devideEncSumByNominalsSecCurr(con, nominals, encSummCalc, loadType, iterations + 1, maxInCass, maxCurrCassCount,
				            forecast);
			} catch (ForecastException e) {
				logger.error("atmID = " + forecast.getAtmID(),e);
				forecast.setNominalsRespCode(e.getCode());
				return nominals;
			}
		}
		return nominals;
	}




	/**
	 * Modifies denomination list by merging several ATM cassetts with amount
	 * of denominations less than minimum amount allowed into 0,1,2... full cassetts
	 * and one cassette with rest amount
	 *
	 * @param nominals
	 *            - List of denominations to be changed
	 * @param minInCass
	 *            - minimum count of denominations in cassette
	 * @param maxInCass
	 *            - maximum count of denominations in cassette
	 * @return modified List of denominations
	 * @throws ForecastException
	 */
	private static List<NominalItem> mergeCass(List<NominalItem> nominals, int minInCass, int maxInCass) {
		List<NominalItem> fullItems = new ArrayList<NominalItem>();

		for (NominalItem item : nominals) {
			if (item.getCountInOneCassPlan() < minInCass) {
				int denomCount = item.getCountInOneCassPlan() * item.getCassCount();
				for (int i = 1; i > 0; i++) {
					if (denomCount < maxInCass * i) {
						if (i == 1) {
							item.setCassCount(1);
							item.setCountInOneCassPlan(denomCount);
						} else {
							NominalItem fullItem = new NominalItem();
							fullItem.setCassCount(i);
							fullItem.setCountInOneCassPlan(maxInCass);
							fullItem.setCountLast(item.getCountLast());
							fullItem.setCurrency(item.getCurrency());
							fullItem.setDayTakeOffCoeff(item.getDayTakeOffCoeff());
							fullItem.setDenom(item.getDenom());
							fullItem.setDenomMultipleCoeff(item.getDenomMultipleCoeff());
							fullItems.add(fullItem);

							item.setCassCount(1);
							item.setCountInOneCassPlan(denomCount - i * maxInCass);
						}
						i = -1;
					}
				}
			}
		}
		nominals.addAll(fullItems);
		return nominals;
	}

	private static List<NominalItem> extractCass(List<NominalItem> nominals, int maxCassCount) {
		NominalItem maxDenomCountItem = Collections.max(nominals, new NominalItemComparatorByCountInOneCass());
		maxDenomCountItem.setCountInOneCassPlan(maxDenomCountItem.getCountInOneCassPlan() * maxDenomCountItem.getCassCount()
		        / (maxDenomCountItem.getCassCount() + 1));
		maxDenomCountItem.setCassCount(maxDenomCountItem.getCassCount()+1);
		int cassCount = 0;
		for(NominalItem item : nominals){
			cassCount += item.getCassCount();
		}
		if (cassCount < maxCassCount) {
			nominals = extractCass(nominals, maxCassCount);
		}
		return nominals;
	}

	/**
	 * Modifies list of denominations by filling amount of denomination
	 * is cassette up to minimum amount level by transferring sum from one cassette
	 * to another.
	 *
	 * @param nominals
	 *            - List of denominations to be changed
	 * @param minInCass
	 *            - minimum count of denominations in cassette
	 * @param maxInCass
	 *            - maximum count of denominations in cassette
	 * @return modified List of denominations
	 * @throws ForecastException
	 */
	private static List<NominalItem> extractCassWhereTooFew(List<NominalItem> nominals, int minInCass, int maxInCass) throws ForecastException {

		for (NominalItem item : nominals) {
			if (item.getCountInOneCassPlan() < minInCass) {
				for (NominalItem i : nominals) {
					if (i.getDenom() == item.getDenom() || i.getCountInOneCassPlan() < minInCass) {
						continue;
					}
					if (item.getCountInOneCassPlan() >= minInCass) {
						break;
					}
					int diff = (minInCass - item.getCountInOneCassPlan()) * item.getDenom() / (i.getDenom() * i.getCassCount());
					if (i.getCountInOneCassPlan() - diff > minInCass) {
						item.setCountInOneCassPlan(minInCass);
						i.setCountInOneCassPlan(i.getCountInOneCassPlan() - diff);
					} else {
						diff = (i.getCountInOneCassPlan() - minInCass) * i.getDenom() / (item.getDenom() * item.getCassCount());
						item.setCountInOneCassPlan(item.getCountInOneCassPlan() + diff);
						i.setCountInOneCassPlan(minInCass);
					}
				}
			}
		}

		for (NominalItem item : nominals) {
			if (item.getCountInOneCassPlan() < minInCass) {
				throw new ForecastException(ForecastException.CAN_NOT_FILL_TO_MINIMUM);
			}
		}
		return nominals;
	}

	/**
	 * Checks whether denomination set is enough to reach encashment sum.
	 *
	 * @param nominals
	 *            - List of denominations
	 * @param encSumm
	 *            - counted encashment sum
	 * @return true if sum can be reached, false - if not
	 */
	private static boolean planSummIsEnough(List<NominalItem> nominals, double encSumm) {
		double planSumm = 0;
		for (NominalItem i : nominals) {
			planSumm += i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
		}
		return (planSumm >= encSumm * 0.95);
	}

	/**
	 * Splits encashment sum between denominations using Average algorithm
	 *
	 * @param encSumm
	 *            - encashment sum
	 * @param maxInCass
	 *            - maximum count of denominations in cassette
	 * @param nominals
	 *            - List of denominations
	 * @return modified List of denominations
	 */
	private static List<NominalItem> setPlanEncNomAverage(double encSumm, int maxInCass, List<NominalItem> nominals, int iterCount) {

		int nomSumm = 0;
		int encAmount = 0;

		for (NominalItem i : nominals) {
			nomSumm += i.getDenom() * i.getCassCount();
		}

		if (nomSumm != 0) {
			encAmount = new BigDecimal(encSumm / nomSumm).intValue();
		}

		for (NominalItem i : nominals) {
			int encAmountForNom = encAmount;
			if (encAmountForNom > maxInCass) {
				encAmountForNom = maxInCass;
				//			} else {
				//				encAmountForNom = encAmountForNom;
				//			}
			}
			i.setCountInOneCassPlan(encAmountForNom);
		}

		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() * i.getCassCount() < maxInCass) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() * i.getCassCount() == maxInCass) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0 && iterCount < 5) {
				unfullNominals = setPlanEncNomAverage(encSumm, maxInCass, unfullNominals, iterCount++);
			}
		}

		return nominals;
	}

	/**
	 * Splits encashment sum between denominations using Proportional algorithm
	 *
	 * @param encSumm
	 *            - encashment sum
	 * @param maxInCass
	 *            - maximum count of denominations in cassette
	 * @param nominals
	 *            - List of denominations
	 * @return modified List of denominations
	 */
	private static List<NominalItem> setPlanEncNomProportional(double encSumm, int maxInCass, List<NominalItem> nominals, int iterCount) {

		float propSumm = 0;
		for (NominalItem i : nominals) {
			propSumm += (i.getDenom() * i.getCountLast());
		}
		for (NominalItem i : nominals) {
			float temp = (i.getDenom() * i.getCountLast()) / propSumm;
			int encAmount = new BigDecimal(encSumm * temp / i.getDenom()).intValue();
			if (encAmount >= maxInCass * i.getCassCount()) {
				encAmount = maxInCass;
			} else {
				encAmount = encAmount / i.getCassCount();
			}
			i.setCountInOneCassPlan(encAmount);
		}
		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() < maxInCass) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() == maxInCass) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0  && iterCount < 5 ) {
				unfullNominals = setPlanEncNomProportional(encSumm, maxInCass, unfullNominals,++iterCount);
			}
		}

		return nominals;
	}

	/**
	 * Splits encashment sum between denominations using DownTop algorithm
	 *
	 * @param encSumm
	 *            - encashment sum
	 * @param maxInCass
	 *            - maximum count of denominations in cassette
	 * @param nominals
	 *            - List of denominations
	 * @return modified List of denominations
	 */
	private static List<NominalItem> setPlanEncNomDownTop(double encSumm, int maxInCass, List<NominalItem> nominals, int iterCount) {
		Collections.sort(nominals, new NominalItemComparatorByDenomAsc());
		for (NominalItem i : nominals) {
			int encAmount = new BigDecimal(encSumm / i.getDenom()).intValue();
			if (encAmount >= maxInCass * i.getCassCount()) {
				encAmount = maxInCass;
				i.setCountInOneCassPlan(maxInCass);
			} else {
				i.setCountInOneCassPlan(encAmount / i.getCassCount());
			}
			encSumm -= encAmount * i.getDenom();
		}
		Collections.sort(nominals, new NominalItemComparatorByDenomDesc());
		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() * i.getCassCount() < maxInCass) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() * i.getCassCount() == maxInCass) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0 && iterCount < 5) {
				unfullNominals = setPlanEncNomDownTop(encSumm, maxInCass, unfullNominals,++iterCount);
			}
		}
		return nominals;
	}

	/**
	 * Splits encashment sum between denominations using TopDown algorithm
	 *
	 * @param encSumm
	 *            - encashment sum
	 * @param maxInCass
	 *            - maximum count of denominations in cassette
	 * @param nominals
	 *            - List of denominations
	 * @return modified List of denominations
	 */
	private static List<NominalItem> setPlanEncNomTopDown(double encSumm, int maxInCass, List<NominalItem> nominals,int iterCount) {
		Collections.sort(nominals, new NominalItemComparatorByDenomDesc());
		for (NominalItem i : nominals) {
			int encAmount = new BigDecimal(encSumm / i.getDenom()).intValue();
			if (encAmount >= maxInCass * i.getCassCount()) {
				encAmount = maxInCass;
				i.setCountInOneCassPlan(maxInCass);
			} else {
				i.setCountInOneCassPlan(encAmount / i.getCassCount());
			}
			encSumm -= encAmount * i.getDenom();
		}
		Collections.sort(nominals, new NominalItemComparatorByDenomAsc());
		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() * i.getCassCount() < maxInCass) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() * i.getCassCount() == maxInCass) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0 && iterCount < 5) {
				unfullNominals = setPlanEncNomTopDown(encSumm, maxInCass, unfullNominals,++iterCount);
			}
		}
		return nominals;
	}

}
