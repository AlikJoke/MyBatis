package ru.bpc.cm.forecasting;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.Currency;
import ru.bpc.cm.forecasting.controllers.ForecastCommonController;
import ru.bpc.cm.forecasting.controllers.ForecastNominalsController;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.forecast.ForecastItem;
import ru.bpc.cm.items.forecast.UserForecastFilter;
import ru.bpc.cm.items.forecast.nominal.NominalItem;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByDenomAsc;
import ru.bpc.cm.items.forecast.nominal.NominalItemComparatorByDenomDesc;


//ADDED DUE TO REQUEST OF FORECASTING WITH THE SAME DENOMINATIONS THAT WERE PREVIOUSLY LODAED TO ATM, WITHOUT ANY CHANGES OF DENOMINATIONS SET
public class NominalsFixedCassForecast {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	public static void setEncNominals(Connection con, AnyAtmForecast forecast, UserForecastFilter filter)
	throws ForecastException {
		
		int atmId = forecast.getAtmId();
		List<NominalItem> nominals = null;
		
		boolean userSettedNominals = (filter != null && filter.isNewNominals());

		int loadType = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASS_LOADING_TYPE));
		boolean cashAddEnabled = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASH_ADD_ENABLED)) > 0; 
		int maxCountInOneCass = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASH_OUT_CASS_VOLUME));
		
		Currency mainCurr = forecast.getAtmCurrencies().get(0);
		
		if(userSettedNominals){
			nominals = filter.getMainCurrNominals();
			for(NominalItem nominal : nominals){
				nominal.setMaxCountInOneCass(maxCountInOneCass);
			}
		} else {
			nominals = mainCurr.getNominals();
			if(cashAddEnabled){
				mainCurr.setEncSummForCurr(prepareNominalsForCashAddCalculation
						(con, nominals, atmId, forecast.getCoEncId(), forecast.getStartDate(), 
								forecast.getForthcomingEncDate(), 
								mainCurr.getCoCoeffsCalendar(),
								mainCurr.getEncSummForCurr(),
								forecast.getDayStart(),
								forecast.getDayEnd()));
			}
			
			nominals = devideEncSumByNominalsMainCurr(con, nominals, mainCurr.getEncSummForCurr(),
			        loadType);
		}
		
		long sum = Long.valueOf(0);
		for (NominalItem j : nominals) {
			setCountInOneCassForNominal(j, userSettedNominals);
			sum += j.getCountInOneCassPlan() * j.getCassCount() * j.getDenom();
		}
		mainCurr.setPlanSummForCurr(sum);
		mainCurr.setEncSummForCurr(sum);
		mainCurr.setNominals(nominals);
		
		for (int i = 1; i < forecast.getAtmCurrencies().size(); i++) {
			Currency currency = forecast.getAtmCurrencies().get(i);

			nominals = new ArrayList<NominalItem>();
			if(userSettedNominals){
				if(currency.getCurrCode() == filter.getSecCurrCode()){
					nominals = filter.getSecCurrNominals();
					for(NominalItem nominal : nominals){
						nominal.setMaxCountInOneCass(maxCountInOneCass);
					}
				} else if(currency.getCurrCode() == filter.getSec2CurrCode()){
					nominals = filter.getSec2CurrNominals();
					for(NominalItem nominal : nominals){
						nominal.setMaxCountInOneCass(maxCountInOneCass);
					}
				} else if(currency.getCurrCode() == filter.getSec3CurrCode()){
					nominals = filter.getSec3CurrNominals();
					for(NominalItem nominal : nominals){
						nominal.setMaxCountInOneCass(maxCountInOneCass);
					}
				}
			} else {
				nominals = currency.getNominals();
				if(cashAddEnabled){
					currency.setEncSummForCurr(prepareNominalsForCashAddCalculation
							(con, nominals, atmId, forecast.getCoEncId(), forecast.getStartDate(), 
									forecast.getForthcomingEncDate(), 
									currency.getCoCoeffsCalendar(), 
									currency.getEncSummForCurr(),
									forecast.getDayStart(), 
									forecast.getDayEnd()));
				}
				
				
				nominals = devideEncSumByNominalsSecCurr(con, nominals, currency.getEncSummForCurr(),
				        loadType);
			}
			sum = Long.valueOf(0);
			for (NominalItem j : nominals) {
				setCountInOneCassForNominal(j, userSettedNominals);
				sum += j.getCountInOneCassPlan() * j.getCassCount() * j.getDenom();
			}
			currency.setPlanSummForCurr(sum);
			currency.setEncSummForCurr(sum);
			currency.setNominals(nominals);
		}
	}
	
	private static void setCountInOneCassForNominal(NominalItem nominal, boolean userSettedNominals){
		double count = 0.0;
		if(nominal.getCountInOneCassPlan() > 1){
			count += nominal.getCountInOneCassPlan(); 
		}
		if(!userSettedNominals){
			if(count < 100.0){
				count += Math.max(nominal.getMinCountInOneCass(), 100);
			} else {
				count += nominal.getMinCountInOneCass();
			}
		}
		count = Math.ceil(count/100);
		nominal.setCountInOneCassPlan((int)(Math.round(count)*100));
	}
	
	public static AnyAtmForecast setEncNominalsPeriod(Connection con, AnyAtmForecast forecast,
			Date startDate)
	throws ForecastException {
		
		int atmId = forecast.getAtmId();

		int maxDenomCount = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASH_OUT_CASS_VOLUME));
		int minDenomCount = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASH_OUT_CASS_MIN_LOAD));
		int loadType = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASS_LOADING_TYPE));
		boolean cashAddEnabled = Integer.parseInt(forecast.getAtmAttribute(AtmAttribute.CASH_ADD_ENABLED)) > 0; 
		
		Currency mainCurr = forecast.getAtmCurrencies().get(0);


		List<NominalItem> nominals = mainCurr.getNominals();
		if(cashAddEnabled){
			mainCurr.setEncSummForCurr(prepareNominalsForCashAddCalculationPeriod
					(con, nominals, atmId, loadType, startDate, forecast.getForthcomingEncDate(), 
							mainCurr.getCoCoeffsCalendar(), mainCurr.getRemaining(),
							mainCurr.getEncSummForCurr(), maxDenomCount, minDenomCount));
		}
		
		nominals = devideEncSumByNominalsMainCurr(con, nominals, mainCurr.getEncSummForCurr(),
		        loadType);
		
		Long sum = Long.valueOf(0);
		for (NominalItem j : nominals) {
			setCountInOneCassForNominal(j, false);
			sum += j.getCountInOneCassPlan() * j.getCassCount() * j.getDenom();
		}
		mainCurr.setPlanSummForCurr(sum);
		mainCurr.setEncSummForCurr(sum.doubleValue());
		mainCurr.setNominals(nominals);
		for (int i = 1; i < forecast.getAtmCurrencies().size(); i++) {
			Currency currency = forecast.getAtmCurrencies().get(i);

			nominals = currency.getNominals();
			if(cashAddEnabled){
				currency.setEncSummForCurr(prepareNominalsForCashAddCalculationPeriod
						(con, nominals, atmId, loadType, startDate, forecast.getForthcomingEncDate(), 
								currency.getCoCoeffsCalendar(), currency.getRemaining(),
								currency.getEncSummForCurr(), maxDenomCount, minDenomCount));
			}
				
				
			nominals = devideEncSumByNominalsSecCurr(con, nominals, currency.getEncSummForCurr(),
			        loadType);
			
			sum = Long.valueOf(0);
			for (NominalItem j : nominals) {
				setCountInOneCassForNominal(j, false);
				sum += j.getCountInOneCassPlan() * j.getCassCount() * j.getDenom();
			}
			currency.setPlanSummForCurr(sum);
			currency.setEncSummForCurr(sum.doubleValue());
			currency.setNominals(nominals);
		}
		
		if(forecast.getForecastResp() == ForecastItem.PROCEDURE_NOT_EXECUTED){
			forecast.setForecastResp(ForecastItem.PROCEDURE_TERMINATED_SUCCESSFULLY);
		}
		
		return forecast;
	}
	
	private static double prepareNominalsForCashAddCalculation(Connection con, List<NominalItem> nominals, 
			int atmId, int lastEncId,
			Date startDate, Date forthcomingEncDate, 
			Map<Date, Double> calendarDaysMapForecast,
			double currSummCalc, int dayStart, int dayEnd){
		for (NominalItem nominal : nominals) {
			double averageDemand = ForecastNominalsController.
					getAvgDenomDemandStat(con, atmId, startDate, nominal.getCurrency(), nominal.getDenom(), dayStart, dayEnd);
			nominal.setAverageDemand(averageDemand);
			double remaining = ForecastNominalsController.getDenomRemaining(con, atmId, nominal.getCurrency(), lastEncId, startDate, nominal.getDenom());
			nominal.setRemainingOnEncDate(new Double(Math.max(remaining 
					- ForecastCommonUtils.calculateCurrencyTakeOffForPeriod
						(con, averageDemand, atmId, startDate, 
								ForecastCommonController
									.getPeriodAvailableDates(forthcomingEncDate, startDate, con, atmId), calendarDaysMapForecast),0)).intValue());
			nominal.setMaxCountInOneCass(Math.max(nominal.getMaxCountInOneCass()-nominal.getRemainingOnEncDate()/nominal.getCassCount(),0));
			nominal.setMinCountInOneCass(Math.max(nominal.getMinCountInOneCass()-nominal.getRemainingOnEncDate()/nominal.getCassCount(),0));
			
			currSummCalc -= nominal.getRemainingOnEncDate() * nominal.getDenom();
		}
		return Math.max(currSummCalc,0);
	}
	
	private static double prepareNominalsForCashAddCalculationPeriod(
			Connection con, List<NominalItem> nominals, 
			int atmId, int loadType,
			Date startDate, Date forthcomingEncDate, 
			Map<Date, Double> calendarDaysMapForecast,
			double remainingSum, double currSummCalc, 
			int maxDenomCount, int minDenomCount){
		
		nominals = devideEncSumByNominalsSecCurr(con, nominals, remainingSum, loadType);
		
		for (NominalItem nominal : nominals) {
			double remaining = nominal.getCountInOneCassPlan();
			
			nominal.setMaxCountInOneCass(maxDenomCount);
			nominal.setMinCountInOneCass(minDenomCount);
			
			nominal.setRemainingOnEncDate(new Double(Math.max(remaining 
					- ForecastCommonUtils.calculateCurrencyTakeOffForPeriod
						(con, nominal.getAverageDemand(), atmId, startDate, 
								ForecastCommonController
									.getPeriodAvailableDates(forthcomingEncDate, startDate, con, atmId), calendarDaysMapForecast),0)).intValue());
			nominal.setMaxCountInOneCass(Math.max(nominal.getMaxCountInOneCass()-nominal.getRemainingOnEncDate()/nominal.getCassCount(),0));
			nominal.setMinCountInOneCass(Math.max(nominal.getMinCountInOneCass()-nominal.getRemainingOnEncDate()/nominal.getCassCount(),0));
			
			currSummCalc -= nominal.getRemainingOnEncDate() * nominal.getDenom();
		}
		return Math.max(currSummCalc,0);
	}
		

	private static List<NominalItem> devideEncSumByNominalsMainCurr(Connection con, List<NominalItem> nominals, double encSummCalc, int loadType)
	{
		switch (loadType) {
		case 1:
			nominals = setPlanEncNomAverage(encSummCalc, nominals, 0);
			break;
		case 2:
			nominals = setPlanEncNomProportional(encSummCalc, nominals);
			break;
		case 3:
			nominals = setPlanEncNomDownTop(encSummCalc, nominals, 0);
			break;
		case 4:
			nominals = setPlanEncNomTopDown(encSummCalc, nominals, 0);
			break;
		default:
		}
		return nominals;
	}

	private static List<NominalItem> devideEncSumByNominalsSecCurr(Connection con, List<NominalItem> nominals, double encSummCalc, int loadType)
	{
		switch (loadType) {
		case 1:
			nominals = setPlanEncNomAverage(encSummCalc, nominals, 0);
			break;
		case 2:
			nominals = setPlanEncNomProportional(encSummCalc, nominals);
			break;
		case 3:
			nominals = setPlanEncNomProportional(encSummCalc, nominals);
			break;
		case 4:
			nominals = setPlanEncNomProportional(encSummCalc, nominals);
			break;
		default:
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
	private static List<NominalItem> setPlanEncNomAverage(double encSumm, List<NominalItem> nominals, int iterCount) {

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
			if (encAmountForNom > i.getMaxCountInOneCass()) {
				encAmountForNom = i.getMaxCountInOneCass();
				//			} else {
				//				encAmountForNom = encAmountForNom;
				//			}
			}
			i.setCountInOneCassPlan(encAmountForNom);
		}

		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() * i.getCassCount() < i.getMaxCountInOneCass()) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() * i.getCassCount() == i.getMaxCountInOneCass()) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0 && iterCount < 5) {
				unfullNominals = setPlanEncNomAverage(encSumm, unfullNominals, iterCount++);
			}
		}

		return nominals;
	}
	
	private static List<NominalItem> setPlanEncNomProportional(double encSummCalc, List<NominalItem> nominals) {
		double maxSumm=0;
		for (NominalItem i : nominals){
			maxSumm+=i.getDenom()*i.getMaxCountInOneCass();
		}
		if (encSummCalc>=maxSumm){
			nominals = setPlanEncMaxNomProportional(encSummCalc, nominals);
		} else {
			nominals = setPlanEncNomProportional(encSummCalc, nominals, 0);
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
	private static List<NominalItem> setPlanEncNomProportional(double encSumm, List<NominalItem> nominals, int iterCount) {

		float propSumm = 0;
		for (NominalItem i : nominals) {
			propSumm += (i.getDenom() * Math.max(i.getCountLast(),100));
		}
		for (NominalItem i : nominals) {
			float temp = (i.getDenom() * i.getCountLast()) / propSumm;
			int encAmount = new BigDecimal(encSumm * temp / i.getDenom()).intValue();
			if (encAmount >= i.getMaxCountInOneCass() * i.getCassCount()) {
				encAmount = i.getMaxCountInOneCass();
			} else {
				encAmount = encAmount / i.getCassCount();
			}
			i.setCountInOneCassPlan(Math.max(encAmount,1));
		}
		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() < i.getMaxCountInOneCass()) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() == i.getMaxCountInOneCass()) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0  && iterCount < 5 ) {
				unfullNominals = setPlanEncNomProportional(encSumm, unfullNominals,++iterCount);
			}
		}

		return nominals;
	}
	
	private static List<NominalItem> setPlanEncMaxNomProportional(double encSumm, List<NominalItem> nominals) {
			int propCount = 0;
			for (NominalItem i : nominals) {
				propCount += Math.max(i.getCountLast(),100);
			}
			/*float propSumm = 0;
			for (NominalItem i : nominals) {
				propSumm += (i.getDenom() * Math.max(i.getCountLast(),100));
			}*/
			int maxDenom = getMaxDenom(nominals);
			float maxDenomCoeff = 1;
			for (NominalItem i : nominals) {
				if (i.getDenom()==maxDenom){
					float temp = ((float)i.getCountLast()) / propCount;
					maxDenomCoeff=temp;
					i.setCountInOneCassPlan(i.getMaxCountInOneCass());
				}
			}
			for (NominalItem i : nominals) {
				if (i.getDenom()!=maxDenom){
					float temp = ((float)i.getCountLast())/ propCount;
					int encAmount = (int)(Math.round(Math.ceil(i.getMaxCountInOneCass() * temp/(maxDenomCoeff*100)))*100);
					i.setCountInOneCassPlan(Math.max(encAmount,1));
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
	private static List<NominalItem> setPlanEncNomDownTop(double encSumm, List<NominalItem> nominals, int iterCount) {
		Collections.sort(nominals, new NominalItemComparatorByDenomAsc());
		for (NominalItem i : nominals) {
			int encAmount = new BigDecimal(encSumm / i.getDenom()).intValue();
			if (encAmount >= i.getMaxCountInOneCass() * i.getCassCount()) {
				encAmount = i.getMaxCountInOneCass();
				i.setCountInOneCassPlan(i.getMaxCountInOneCass());
			} else {
				i.setCountInOneCassPlan(encAmount / i.getCassCount());
			}
			encSumm -= encAmount * i.getDenom();
		}
		Collections.sort(nominals, new NominalItemComparatorByDenomDesc());
		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() * i.getCassCount() < i.getMaxCountInOneCass()) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() * i.getCassCount() == i.getMaxCountInOneCass()) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0 && iterCount < 5) {
				unfullNominals = setPlanEncNomDownTop(encSumm, unfullNominals,++iterCount);
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
	private static List<NominalItem> setPlanEncNomTopDown(double encSumm, List<NominalItem> nominals,int iterCount) {
		Collections.sort(nominals, new NominalItemComparatorByDenomDesc());
		for (NominalItem i : nominals) {
			int encAmount = new BigDecimal(encSumm / i.getDenom()).intValue();
			if (encAmount >= i.getMaxCountInOneCass() * i.getCassCount()) {
				encAmount = i.getMaxCountInOneCass();
				i.setCountInOneCassPlan(i.getMaxCountInOneCass());
			} else {
				i.setCountInOneCassPlan(encAmount / i.getCassCount());
			}
			encSumm -= encAmount * i.getDenom();
		}
		Collections.sort(nominals, new NominalItemComparatorByDenomAsc());
		if (!planSummIsEnough(nominals, encSumm)) {
			List<NominalItem> unfullNominals = new ArrayList<NominalItem>();
			for (NominalItem i : nominals) {
				if (i.getCountInOneCassPlan() * i.getCassCount() < i.getMaxCountInOneCass()) {
					unfullNominals.add(i);
				}
				if (i.getCountInOneCassPlan() * i.getCassCount() == i.getMaxCountInOneCass()) {
					encSumm -= i.getCountInOneCassPlan() * i.getDenom() * i.getCassCount();
				}
			}
			if (unfullNominals.size() > 0 && iterCount < 5) {
				unfullNominals = setPlanEncNomTopDown(encSumm, unfullNominals,++iterCount);
			}
		}
		return nominals;
	}
	
	/**
	 * Checks if all denominations cassettes are filled
	 * @param nominals
	 *            - List of denominations
	 */
	private static boolean checkAllNomMaxedOut(List<NominalItem> nominals) {
		int fullNomCount = 0;
		for (NominalItem i : nominals) {
			
			if (i.getCountInOneCassPlan()==i.getMaxCountInOneCass()){
				fullNomCount++;
			}
		}
		if (fullNomCount==nominals.size()){
			return true;
		} else {
			return false;
		}		
	}
	
	/**
	 * gets max denom from nominals list
	 * @param nominals
	 *            - List of denominations
	 */
	private static int getMaxDenom(List<NominalItem> nominals) {
		int maxDenom = nominals.get(0).getDenom();
		for (NominalItem i : nominals) {
			
			if (i.getDenom()>maxDenom){
				maxDenom=i.getDenom();
			}
		}
		return maxDenom;
	}

}
