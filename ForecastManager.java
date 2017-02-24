package ru.bpc.cm.forecasting;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.forecasting.anyatm.EncashmentForecast;
import ru.bpc.cm.forecasting.anyatm.EncashmentsForPeriodForecast;
import ru.bpc.cm.forecasting.anyatm.ForecastBuilder;
import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.ForecastForPeriod;
import ru.bpc.cm.forecasting.controllers.EncashmentsInsertController;
import ru.bpc.cm.forecasting.controllers.ForecastCompareController;
import ru.bpc.cm.forecasting.controllers.ForecastForPeriodController;
import ru.bpc.cm.items.enums.EncashmentType;
import ru.bpc.cm.items.enums.ForecastingMode;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.forecast.UserForecastFilter;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.IntFilterItem;
import ru.bpc.cm.utils.ObjectPair;



public class ForecastManager {
	
	private static final Logger logger = LoggerFactory
			.getLogger("CASH_MANAGEMENT");
	
	public static void makeForecast(Connection con, List<IFilterItem<Integer>> filterList, boolean useMainCurr) {
		try {
			Date startDate = new Date();
			List<ObjectPair<Integer,List<EncashmentType>>> tmpList = new ArrayList<ObjectPair<Integer,List<EncashmentType>>>();
		
			tmpList = EncashmentsInsertController.checkExistingEncashments(con,
					filterList);
			for (ObjectPair<Integer,List<EncashmentType>> item : tmpList) {
				int atmId = item.getKey();
				EncashmentType missingEncType = EncashmentType.getMissingEncashmentType(item.getValue());
				if(logger.isDebugEnabled()){
					logger.debug("Forecast, atmId = {}",atmId);
					logger.debug("missingEncType:"+missingEncType.name());
				}
				if(missingEncType == EncashmentType.NOT_NEEDED){
					continue;
				}
				
				AnyAtmForecast forecast = null;
				try{
					forecast = EncashmentForecast.makeForecastForAtm(con, atmId, startDate, null, ForecastingMode.PLAN, missingEncType, true, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
				} catch (ForecastException e) {
					logger.error("atmID = " + atmId, e);
					forecast = ForecastBuilder.getBlankAtmForecast();
					forecast.setAtmId(atmId);
					forecast.setForecastResp(e.getCode());
					forecast.setErrorForthcomingEncDate(startDate);
					forecast.setEncType(EncashmentType.NOT_NEEDED);
				} catch (Exception e) {
					logger.error("", e);
				}
				Integer existingPlanId = EncashmentsInsertController.getExistingPlanId(
						con, atmId);
				if(existingPlanId!=0) {
					forecast.setEncPlanID(existingPlanId);
				}
				
				if (forecast.getEncType() == EncashmentType.NOT_NEEDED){
					List<Integer> deleteList = new ArrayList<Integer>();
					deleteList.add(forecast.getEncPlanID());
					int routeId = EncashmentsInsertController.getRouteIdForEnc(con, forecast);
					EncashmentsInsertController.deleteEncashments(con, deleteList);
					EncashmentsInsertController.updateRoute(con, routeId);
				} else {
					int routeId = EncashmentsInsertController.getRouteIdForEnc(con, forecast);
					EncashmentsInsertController.ensureRouteConsistencyForEnc(con, forecast, missingEncType, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
					EncashmentsInsertController.insertForecastData(con, forecast);
					EncashmentsInsertController.updateRoute(con, routeId);
				}
				
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	public static void makeForecastForSimpleList(Connection con,
			List<Integer> atmList, boolean useMainCurr) {
		try {
			Date startDate = new Date();
			List<ObjectPair<Integer,List<EncashmentType>>> tmpList = new ArrayList<ObjectPair<Integer,List<EncashmentType>>>();
		
			tmpList = EncashmentsInsertController.checkExistingEncashmentsForSimpleList(con,
					atmList);
			for (ObjectPair<Integer,List<EncashmentType>> item : tmpList) {
				int atmId = item.getKey();
				EncashmentType missingEncType = EncashmentType.getMissingEncashmentType(item.getValue());
				if(logger.isDebugEnabled()){
					logger.debug("Forecast, atmId = {}",atmId);
					logger.debug("missingEncType:"+missingEncType.name());
				}
				if(missingEncType == EncashmentType.NOT_NEEDED){
					continue;
				}
				
				AnyAtmForecast forecast = null;
				try{
					forecast = EncashmentForecast.makeForecastForAtm(con, atmId, startDate, null, ForecastingMode.PLAN, missingEncType, true, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
				} catch (ForecastException e) {
					logger.error("atmID = " + atmId, e);
					forecast = ForecastBuilder.getBlankAtmForecast();
					forecast.setAtmId(atmId);
					forecast.setForecastResp(e.getCode());
					forecast.setErrorForthcomingEncDate(startDate);
					forecast.setEncType(EncashmentType.NOT_NEEDED);
				} catch (Exception e) {
					logger.error("", e);
				}
				Integer existingPlanId = EncashmentsInsertController.getExistingPlanId(
						con, atmId);
				if(existingPlanId!=0) {
					forecast.setEncPlanID(existingPlanId);
				}
				
				if (forecast.getEncType() == EncashmentType.NOT_NEEDED){
					List<Integer> deleteList = new ArrayList<Integer>();
					deleteList.add(forecast.getEncPlanID());
					int routeId = EncashmentsInsertController.getRouteIdForEnc(con, forecast);
					EncashmentsInsertController.deleteEncashments(con, deleteList);
					EncashmentsInsertController.updateRoute(con, routeId);
				} else {
					int routeId = EncashmentsInsertController.getRouteIdForEnc(con, forecast);
					EncashmentsInsertController.ensureRouteConsistencyForEnc(con, forecast, missingEncType, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
					EncashmentsInsertController.insertForecastData(con, forecast);
					EncashmentsInsertController.updateRoute(con, routeId);
				}
				
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	public static void makeForecastForPerticularDate(Connection con, List<IFilterItem<Integer>> filterList, Date startDate, boolean useMainCurr) {
		try {
			// List<ForecastItem> forecastList = new ArrayList<ForecastItem>();
			UserForecastFilter filter = new UserForecastFilter();
			int encPlanId = 0;
			filter.setNewDate(true);
			filter.setForthcomingEncDate(startDate);
			
			List<IFilterItem<Integer>> tmpList = new ArrayList<IFilterItem<Integer>>();
			for (IFilterItem<Integer> item : filterList) {
				tmpList.add(new IntFilterItem(item.getValue(), item.getLabel()));
			}
			
			for (IFilterItem<Integer> item : tmpList) {
				
				int atmId = item.getValue();
				AnyAtmForecast forecast = null;
				if(logger.isDebugEnabled()){
					logger.debug("Forecast for date, atmId = {}",atmId);
				}
				try{
					forecast = EncashmentForecast.makeForecastForAtm(con, atmId, new Date(), filter, 
							ForecastingMode.PLAN, EncashmentType.CASH_IN_AND_CASH_OUT, false, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
				} catch (ForecastException e) {
					logger.error("atmID = " + atmId, e);
					forecast = ForecastBuilder.getBlankAtmForecast();
					forecast.setAtmId(atmId);
					forecast.setForecastResp(e.getCode());
					forecast.setErrorForthcomingEncDate(startDate);
					forecast.setEncType(EncashmentType.NOT_NEEDED);
				} catch (Exception e) {
					logger.error("", e);
				}
				EncashmentsInsertController.insertForecastDataForParticularDate(con, forecast, ++encPlanId);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	public static AnyAtmForecast makeForecastForPeriod(Connection connection,
			AnyAtmForecast forecast, Date startDate, Date finishDate,ForecastingMode forecastingMode, EncashmentType encType, boolean useMainCurr) {
		try {
			forecast = EncashmentForecast.
					makeForecastForPeriodForAtm(connection, forecast, startDate, finishDate, encType, ForecastingMode.PERIOD, useMainCurr);
		} catch (ForecastException e) {
			logger.error("atmID = " + forecast.getAtmId(), e);
			forecast.setForecastResp(e.getCode());
			forecast.setErrorForthcomingEncDate(startDate);
		}
		return forecast;
	}

	public static void makeForecastForSingleAtm(Connection con,
			UserForecastFilter filter, int personID, boolean useMainCurr) {
		try {
			Date startDate = new Date(); 
			List<EncashmentType> approvedEncList = EncashmentsInsertController.checkExistingEncashments(con, filter);
			EncashmentType missingEncType = EncashmentType.getMissingEncashmentType(approvedEncList);
			
			if (missingEncType != EncashmentType.NOT_NEEDED) {
				int atmId = filter.getAtmID();
				AnyAtmForecast forecast = null;

				if(logger.isDebugEnabled()){
					logger.debug("Forecast for one ATM, atmId = {}",atmId);
				}
				try{
					forecast = EncashmentForecast.makeForecastForAtm(con, atmId, startDate, filter, 
							ForecastingMode.PLAN, missingEncType, false, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);

				} catch (ForecastException e) {
					logger.error("atmID = " + atmId, e);
					forecast = ForecastBuilder.getBlankAtmForecast();
					forecast.setAtmId(atmId);
					forecast.setForecastResp(e.getCode());
					forecast.setErrorForthcomingEncDate(startDate);
				} catch (Exception e) {
					logger.error("", e);
				}
				forecast.setEncPlanID(filter.getEncPlanID());
				forecast.setPersonID(personID);
				
				Integer existingPlanId = EncashmentsInsertController.getExistingPlanId(
						con, atmId);
				if(existingPlanId!=0) {
					forecast.setEncPlanID(existingPlanId);
				}
				
				if (forecast.getEncType() == EncashmentType.NOT_NEEDED){
					List<Integer> deleteList = new ArrayList<Integer>();
					deleteList.add(forecast.getEncPlanID());
					int routeId = EncashmentsInsertController.getRouteIdForEnc(con, forecast);
					EncashmentsInsertController.deleteEncashments(con, deleteList);
					EncashmentsInsertController.updateRoute(con, routeId);
				} else {
					int routeId = EncashmentsInsertController.getRouteIdForEnc(con, forecast);
					EncashmentsInsertController.ensureRouteConsistencyForEnc(con, forecast, missingEncType, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
					EncashmentsInsertController.insertForecastData(con, forecast);
					EncashmentsInsertController.updateRoute(con, routeId);
				}
				
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	public static void makeForecastForPeriod(Connection con,
			List<IFilterItem<Integer>> filterList, Date endDate, boolean useMainCurr) {
		try {
			Date startDate = new Date();
			for (IFilterItem<Integer> item : filterList) {
				int atmId = item.getValue();
				AnyAtmForecast forecast = null;
				if(logger.isDebugEnabled()){
					logger.debug("Forecast for period, atmId = {}",atmId);
				}
				try{
					forecast = EncashmentForecast.makeForecastForAtm(con, atmId, startDate, null, 
							ForecastingMode.PERIOD, EncashmentType.CASH_IN_AND_CASH_OUT, false, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
				} catch (ForecastException e) {
					logger.error("atmID = " + atmId, e);
					forecast = ForecastBuilder.getBlankAtmForecast();
					forecast.setAtmId(atmId);
					forecast.setForecastResp(e.getCode());
					forecast.setErrorForthcomingEncDate(startDate);
					continue;
				}
				ForecastForPeriod periodForecast = EncashmentsForPeriodForecast
						.makeStatForecast(con, forecast, startDate, endDate, 
								CashManagementConstants.PERIOD_FORECAST_STATS_PERIOD,
								ForecastingMode.PERIOD,  useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
				ForecastForPeriodController.insertPeriodForecastData(con,
						periodForecast);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	public static void makeForecastForCompare(Connection con,
			List<IFilterItem<Integer>> filterList, boolean useMainCurr) {
		try {
			int	 				atmId;
			AnyAtmForecast 			forecast 	= null;
			ForecastForPeriod 		periodForecast = null;
			
			Date startDate = null;
			Date endDate = null;
			
			for (IFilterItem<Integer> item : filterList) {
				atmId = item.getValue();
				if(logger.isDebugEnabled()){
					logger.debug("Forecast for compare, atmId = {}",atmId);
				}
				endDate = ForecastForPeriodController.getStatsEnd(con, atmId, Calendar.getInstance().getTime());
				
				if(logger.isDebugEnabled()){
					logger.debug("statsEnd = {]",endDate);
				}
				
				Calendar startDateCal = Calendar.getInstance();
				startDateCal.setTime(endDate);
				startDateCal.add(Calendar.MONTH, -CashManagementConstants.COMPARE_STATS_ENOUGH_PERIOD_MONTH);
				startDate = startDateCal.getTime();
				
				int statsCount = ForecastCompareController.getStatsDatesCount(con, atmId, startDate, endDate);
				
				
				if(statsCount >= 0){
					if(logger.isDebugEnabled()){
						logger.debug("forecasting");
					}
					startDateCal.add(Calendar.MONTH, CashManagementConstants.COMPARE_STATS_ENOUGH_PERIOD_MONTH
							-CashManagementConstants.COMPARE_STATS_CALC_PERIOD_MONTH);
					startDate = startDateCal.getTime();
					
					atmId = item.getValue();
					try{
						forecast = EncashmentForecast.makeForecastForAtm(con, atmId, startDate, null, 
								ForecastingMode.COMPARE, EncashmentType.CASH_IN_AND_CASH_OUT, false, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
					} catch (ForecastException e) {
						logger.error("atmID = " + atmId, e);
						forecast = ForecastBuilder.getBlankAtmForecast();
						forecast.setAtmId(atmId);
						forecast.setForecastResp(e.getCode());
						forecast.setErrorForthcomingEncDate(startDate);
						continue;
					}
					periodForecast = EncashmentsForPeriodForecast
							.makeStatForecast(con, forecast, startDate, endDate, 
									CashManagementConstants.COMPARE_FORECAST_STATS_PERIOD,
									ForecastingMode.COMPARE, useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
					ForecastCompareController.insertCompareForecastData(con,
							periodForecast);
				}
				
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}

}
