package ru.bpc.cm.encashments;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.AtmPidListController;
import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.cashmanagement.CmCommonUtils;
import ru.bpc.cm.cashmanagement.NotifyMessageController;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.filters.EncashmentsFilter;
import ru.bpc.cm.filters.MonitoringFilter;
import ru.bpc.cm.forecasting.anyatm.RemainingsForecast;
import ru.bpc.cm.forecasting.controllers.ForecastCommonController;
import ru.bpc.cm.items.encashments.AtmCurrStatItem;
import ru.bpc.cm.items.encashments.AtmEncashmentItem;
import ru.bpc.cm.items.encashments.AtmEncashmentSumsItem;
import ru.bpc.cm.items.encashments.AtmPeriodForecastItem;
import ru.bpc.cm.items.encashments.HourRemaining;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.enums.AtmTypeByOperations;
import ru.bpc.cm.items.enums.EncashmentFilterMode;
import ru.bpc.cm.items.enums.EncashmentStatReportMode;
import ru.bpc.cm.items.enums.ForecastingMode;
import ru.bpc.cm.items.enums.NotifyMessageType;
import ru.bpc.cm.items.forecast.ForecastErrorLevel;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.monitoring.AtmActualStateItem;
import ru.bpc.cm.items.settings.AtmUserItem;
import ru.bpc.cm.monitoring.ActualStateController;
import ru.bpc.cm.monitoring.ActualStateManager;
import ru.bpc.cm.routes.DynamicRoutingController;
import ru.bpc.cm.settings.AtmInfoController;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

public class AtmEncashmentsManager {

//	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");
	
	public static ObjectPair<List<AtmEncashmentItem>,Map<Integer,List<Integer>>> getAtmEncashmentList(ISessionHolder sessionHolder, Connection connection, EncashmentsFilter filter){
		List<AtmEncashmentItem> atmEncashmentList = null;
		List<Integer> encPlanIDList = null;
		Map<Integer,List<Integer>> currDenomMap = null;
		
		atmEncashmentList = AtmEncashmentController.getAtmEncashmentList(sessionHolder ,filter);
		encPlanIDList = new ArrayList<Integer>();
		for(AtmEncashmentItem item:atmEncashmentList){
			encPlanIDList.add(item.getEncPlanID());
		}
		
		currDenomMap = AtmEncashmentController.getCurrDenomMap(sessionHolder, encPlanIDList);
		
		for(AtmEncashmentItem item:atmEncashmentList){
			boolean recyclingPreload = CmCommonUtils.checkAtmRecyclingPreload(sessionHolder, item.getAtmID());
			item.setEncashmentCassettes(AtmEncashmentController.getAtmEncashmentCassList(sessionHolder, item.getEncPlanID()));
			try{
				item.setEncashmentErrorLevel(ForecastErrorLevel.getEncashmentErrorLevel(item.getForecastResp()).getLevel());
				item.setMinInCass(Integer.parseInt(CmCommonController.getAtmAttribute(sessionHolder, item.getAtmID(), AtmAttribute.CASH_OUT_CASS_MIN_LOAD)));
				item.setMaxInCass(Integer.parseInt(CmCommonController.getAtmAttribute(sessionHolder, item.getAtmID(), 
						recyclingPreload ? AtmAttribute.CASH_IN_R_CASS_VOLUME : AtmAttribute.CASH_OUT_CASS_VOLUME)));
			} catch (ForecastException e) {
				item.setEncashmentErrorLevel(ForecastErrorLevel.ERROR.getLevel());
				item.setMinInCass(0);
				item.setMaxInCass(0);
				item.setForecastResp(ForecastException.ATM_ATTRIBUTE_NOT_DEFINED);
			}
			item = AtmEncashmentController.getAtmEncashmentCurrencies(sessionHolder, item);
			item = AtmEncashmentController.getAtmEncashmentMaxAtmCount(sessionHolder, item);
		}
		return new ObjectPair<List<AtmEncashmentItem>, Map<Integer,List<Integer>>>(atmEncashmentList, currDenomMap);
	}
	
	public static Map<Date,Integer> getEncashmentNotAvailableDays(ISessionHolder sessionHolder, Connection connection, int atmId, Integer maxAtmCount){
		Calendar dateStart = Calendar.getInstance();
		Calendar dateFinish = Calendar.getInstance();
		dateFinish.add(Calendar.MONTH, 2);
		
		Map<Date,Integer> encashmentNotAvailableDays = new HashMap<Date, Integer>();

//		Map<Date,Integer> tmpMap = ForecastCommonController.getAvailableAtmDaysForecast(
//				connection, atmId, CmUtils.getHoursBetweenTwoDates(dateStart, dateFinish), 0, false, Calendar.getInstance(),null, false,
//				ForecastingMode.PLAN);
		Map<Date, Integer> tmpMap = ForecastCommonController
				.getAvailableAtmDaysForecast
				(sessionHolder, connection, atmId, dateStart.getTime(), dateFinish.getTime(),
						0,
						false, false,
						ForecastingMode.PLAN);

		for(Entry<Date,Integer> entry: tmpMap.entrySet()){
			encashmentNotAvailableDays.put(CmUtils.truncateDate(entry.getKey()), entry.getValue());
		}
		
		try{
			if(maxAtmCount == null){
				maxAtmCount = Integer.parseInt(CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.MAX_ATM_COUNT));
			}
		} catch (Exception e) {
			// TODO: handle exception
			maxAtmCount = 0;
		}


		for(Entry<Date,Integer> entry : encashmentNotAvailableDays.entrySet()){
			Integer atmCount = entry.getValue();
			if(maxAtmCount > 0){
				if(atmCount >= 0){
					entry.setValue(Math.max(maxAtmCount - atmCount, 0));
				}
				//NOT LIMITED
			} else if(maxAtmCount == 0){
				entry.setValue(999);
			}
		}

		return encashmentNotAvailableDays;
	}
	
	public static List<AtmPeriodForecastItem> getStatForecastList(ISessionHolder sessionHolder,
			Connection connection, List<IFilterItem<Integer>> filterList, Date endDate) {
		List<AtmPeriodForecastItem> resList = new ArrayList<AtmPeriodForecastItem>();
	
		for (IFilterItem<Integer> filter : filterList) {
			AtmPeriodForecastItem item = new AtmPeriodForecastItem();
			item.setAtmId(filter.getValue());
			item.setExtAtmId(filter.getLabel());
			item.setAtmStatus(CmCommonController.getAtmStatus(sessionHolder,
					item.getAtmId()));

			item.setEndDate(endDate);
			item.setCurrencies(CmCommonController.getAtmCurrencies(
					sessionHolder, item.getAtmId()));
			if(item.getAtmStatus().getAtmTypeByOperations() != AtmTypeByOperations.CASH_OUT_ONLY && 
					item.getAtmStatus().getAtmTypeByOperations() != AtmTypeByOperations.NOT_DEFINED){
				item.getCurrencies().add(CashManagementConstants.CASH_IN_CURR_CODE);
			}
			
			item.setCurrCodes(CmCommonUtils.getCurrenciesA3List(sessionHolder, connection,
					item.getCurrencies()));
			
			
			Map<Integer,List<AtmCurrStatItem>> currMap = getCurrenciesStatsForForecast(sessionHolder,
					item.getAtmId(), item.getCurrencies(), endDate);
			
			item.setCashInExists(CmCommonUtils.checkAtmCashIn(sessionHolder, item.getAtmId()));
			
			item.setCurrStat(currMap);
			if(currMap != null && currMap.size() > 0){
				List<AtmCurrStatItem> tmp   =currMap.values().iterator().next();
				if(tmp != null && tmp.size() > 0){
					item.setStartDate(tmp.get(0).getStatDate());
					item.setEndDate(tmp.get(tmp.size()-1).getStatDate());
				} else {
					item.setStartDate(item.getEndDate());
					item.setEndDate(endDate);
				}
			} else {
				item.setStartDate(item.getEndDate());
				item.setEndDate(endDate);
			}
			item.setEncashments(AtmEncashmentController.getEncashmentsForPeriod(sessionHolder, item.getAtmId(), endDate, item.getCurrCodes()));
			resList.add(item);
		}
		return resList;
	}
	
	private static Map<Integer, List<AtmCurrStatItem>> getCurrenciesStatsForForecast(
			ISessionHolder sessionHolder, int atmId, List<Integer> currencies,
			Date endDate) {
		Map<Integer, List<AtmCurrStatItem>> currStatMap = new LinkedHashMap<Integer, List<AtmCurrStatItem>>();
		for (Integer curr : currencies) {
			currStatMap.put(curr, AtmEncashmentController
					.getCurrenciesForPeriod(sessionHolder, atmId, curr,
							endDate));
		}
		return currStatMap;
	}
	
	public static AtmEncashmentSumsItem getAllGroupSumms(ISessionHolder sessionHolder, Connection connection, Date date,
			Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> atmsToGroups){
		
		AtmEncashmentSumsItem allGroupsItem = new AtmEncashmentSumsItem();
		
		allGroupsItem.setId("All Groups");
		allGroupsItem.setCurrencies(AtmEncashmentSummsController.getEncsCurrs(sessionHolder, date));
		allGroupsItem.setCassettes(AtmEncashmentSummsController.getEncsDenoms(sessionHolder, date));
		allGroupsItem.setEncCount(AtmEncashmentSummsController.getEncsCount(sessionHolder, date));
		
		for(Entry<IFilterItem<Integer>, List<IFilterItem<Integer>>> s : atmsToGroups.entrySet()){
			AtmEncashmentSumsItem item = getOneGroupSumms(sessionHolder, connection, date, s.getKey().getLabel(), s.getValue());
			if(item.getCassettes().size() > 0){
				allGroupsItem.getSubItems().add(item);
			}
		}
		
		return allGroupsItem;
	}
	
	private static AtmEncashmentSumsItem getOneGroupSumms(ISessionHolder sessionHolder, Connection connection, Date date,String groupName,List<IFilterItem<Integer>> atms){
		
		CmCommonUtils.storeTempAtmList(sessionHolder, atms);
		
		AtmEncashmentSumsItem groupItem = new AtmEncashmentSumsItem();
		
		groupItem.setId(groupName);
		groupItem.setCassettes(AtmEncashmentSummsController.getEncsDenoms(sessionHolder, date));
		groupItem.setCurrencies(AtmEncashmentSummsController.getEncsCurrs(sessionHolder, date));
		groupItem.setEncCount(AtmEncashmentSummsController.getEncsCount(sessionHolder, date));
		
		
		for(IFilterItem<Integer> atm : atms){
			int i = atm.getValue();
			AtmEncashmentSumsItem item = new AtmEncashmentSumsItem();
			item.setId(atm.getLabel());
			item.setCassettes(AtmEncashmentSummsController.getEncsDenoms(sessionHolder, i, date));
			item.setCurrencies(AtmEncashmentSummsController.getEncsCurrs(sessionHolder, i, date));

			Pair atmAddressAndName = CmCommonController.getAtmAddressAndName(sessionHolder, i);
			
			item.setName(atmAddressAndName.getLabel());
			item.setDescx(atmAddressAndName.getKey());
			
			if(item.getCassettes().size() > 0){
				groupItem.getSubItems().add(item);
			}
		}
		
		return groupItem;
	}
	
	//FOR DATE
	public static AtmEncashmentSumsItem getAllGroupSummsForDate(ISessionHolder sessionHolder, Connection connection, Date date,
			Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> atmsToGroups){
		
		AtmEncashmentSumsItem allGroupsItem = new AtmEncashmentSumsItem();
		
		allGroupsItem.setId("All Groups");
		allGroupsItem.setCurrencies(AtmEncashmentSummsForDateController.getEncsCurrs(sessionHolder, date));
		allGroupsItem.setCassettes(AtmEncashmentSummsForDateController.getEncsDenoms(sessionHolder, date));
		allGroupsItem.setEncCount(AtmEncashmentSummsForDateController.getEncsCount(sessionHolder, date));
		
		for(Entry<IFilterItem<Integer>, List<IFilterItem<Integer>>> s : atmsToGroups.entrySet()){
			AtmEncashmentSumsItem item = getOneGroupSummsForDate(sessionHolder, connection, date, s.getKey().getLabel(), s.getValue());
			if(item.getCassettes().size() > 0){
				allGroupsItem.getSubItems().add(item);
			}
		}
		
		return allGroupsItem;
	}
	
	private static AtmEncashmentSumsItem getOneGroupSummsForDate(ISessionHolder sessionHolder, Connection connection, Date date,String groupName,List<IFilterItem<Integer>> atms){
		
		CmCommonUtils.storeTempAtmList(sessionHolder, atms);
		
		AtmEncashmentSumsItem groupItem = new AtmEncashmentSumsItem();
		
		groupItem.setId(groupName);
		groupItem.setCassettes(AtmEncashmentSummsForDateController.getEncsDenoms(sessionHolder, date));
		groupItem.setCurrencies(AtmEncashmentSummsForDateController.getEncsCurrs(sessionHolder, date));
		groupItem.setEncCount(AtmEncashmentSummsForDateController.getEncsCount(sessionHolder, date));
		
		
		for(IFilterItem<Integer> atm : atms){
			int i = atm.getValue();
			AtmEncashmentSumsItem item = new AtmEncashmentSumsItem();
			item.setId(atm.getLabel());
			item.setCassettes(AtmEncashmentSummsForDateController.getEncsDenoms(sessionHolder, i, date));
			item.setCurrencies(AtmEncashmentSummsForDateController.getEncsCurrs(sessionHolder, i, date));

			Pair atmAddressAndName = CmCommonController.getAtmAddressAndName(sessionHolder, i);
		
			item.setName(atmAddressAndName.getLabel());
			item.setDescx(atmAddressAndName.getKey());
			
			if(item.getCassettes().size() > 0){
				groupItem.getSubItems().add(item);
			}
		}
		
		return groupItem;
	}
	
	//FORECASTING FOR PERIOD LISTED BELOW-------------------------------------------------------------------------//
	
	public static AtmEncashmentSumsItem getAllGroupSummsForPeriod(ISessionHolder sessionHolder, Connection connection, Date date,
			Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> atmsToGroups){
		
		AtmEncashmentSumsItem allGroupsItem = new AtmEncashmentSumsItem();

		
		allGroupsItem.setId("All Groups");
		allGroupsItem.setCurrencies(AtmEncashmentSummsController.getEncsCurrsForPeriod(sessionHolder, date));
		allGroupsItem.setCassettes(AtmEncashmentSummsController.getEncsDenomsForPeriod(sessionHolder, date));
		allGroupsItem.setEncCount(AtmEncashmentSummsController.getEncsCountForPeriod(sessionHolder, date));
		
		for(Entry<IFilterItem<Integer>, List<IFilterItem<Integer>>> s : atmsToGroups.entrySet()){
			AtmEncashmentSumsItem item = getOneGroupSummsForPeriod(sessionHolder, connection, date, s.getKey().getDescx(), s.getValue());
			if(item.getCassettes().size() > 0){
				allGroupsItem.getSubItems().add(item);
			}
		}
		
		return allGroupsItem;
	}
	
	private static AtmEncashmentSumsItem getOneGroupSummsForPeriod(ISessionHolder sessionHolder, Connection connection, Date date,String groupName,List<IFilterItem<Integer>> atms){
		
		CmCommonUtils.storeTempAtmList(sessionHolder, atms);
		
		AtmEncashmentSumsItem groupItem = new AtmEncashmentSumsItem();
		
		groupItem.setId(groupName);
		groupItem.setCassettes(AtmEncashmentSummsController.getEncsDenomsForPeriod(sessionHolder, date));
		groupItem.setCurrencies(AtmEncashmentSummsController.getEncsCurrsForPeriod(sessionHolder, date));
		groupItem.setEncCount(AtmEncashmentSummsController.getEncsCountForPeriod(sessionHolder, date));
		
		
		for(IFilterItem<Integer> atm : atms){
			int i = atm.getValue();
			AtmEncashmentSumsItem item = new AtmEncashmentSumsItem();
			item.setId(atm.getLabel());
			item.setCassettes(AtmEncashmentSummsController.getEncsDenomsForPeriod(sessionHolder, i, date));
			item.setCurrencies(AtmEncashmentSummsController.getEncsCurrsForPeriod(sessionHolder, i, date));

			Pair atmAddressAndName = CmCommonController.getAtmAddressAndName(sessionHolder, i);
			
			item.setName(atmAddressAndName.getLabel());
			item.setDescx(atmAddressAndName.getKey());
			
			if(item.getCassettes().size() > 0){
				groupItem.getSubItems().add(item);
			}
		}
		
		return groupItem;
	}
	
	public static List<HourRemaining> getEncashmentRemainingReport(ISessionHolder sessionHolder, Connection connection, int atmId, EncashmentStatReportMode mode) 
			throws SQLException, ForecastException{
		ObjectPair<Date,Integer> statsEndDatePair = null;
		List<HourRemaining> resList = null;
		Date defaultDate = new Date();
		
		switch (mode) {
		case CASH_IN:
			statsEndDatePair = ActualStateController.getCashInLastStat(sessionHolder, atmId);
			break;
		case CASH_IN_R_CURRS:
			statsEndDatePair = ActualStateController.getCashInLastStat(sessionHolder, atmId);
			break;
		case CASH_OUT:
			statsEndDatePair = ActualStateController.getCashOutLastStat(sessionHolder, atmId);
			break;
		}
		
		resList = RemainingsForecast.makeForecastForAtm(sessionHolder, connection, atmId, 
				statsEndDatePair == null ? defaultDate : CmUtils.getNVLValue(statsEndDatePair.getKey(), defaultDate), mode);
		AtmEncashmentController.insertTempEncReport(sessionHolder, resList);
		
		
		switch (mode) {
		case CASH_IN:
			return AtmEncashmentReportController.getCashInStatRemain(sessionHolder, atmId, 
					statsEndDatePair == null ? defaultDate : CmUtils.getNVLValue(statsEndDatePair.getKey(), defaultDate
							),Long.parseLong(CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.CASH_IN_CASS_VOLUME)));
		case CASH_IN_R_CURRS:
			return AtmEncashmentReportController.getCashRecCurrStatRemain(sessionHolder, atmId, 
					statsEndDatePair == null ? defaultDate : CmUtils.getNVLValue(statsEndDatePair.getKey(), defaultDate));
		case CASH_OUT:
			return AtmEncashmentReportController.getCashOutStatRemain(sessionHolder, atmId, 
					statsEndDatePair == null ? defaultDate : CmUtils.getNVLValue(statsEndDatePair.getKey(), defaultDate));
		}
		
		return resList;
	}
	
	
	public static void checkTodayEncashmentRoutes(ISessionHolder sessionHolder, Connection connection, Date dateToCheck){
		Calendar cal = Calendar.getInstance();
		cal.setTime(CmUtils.truncateDate(dateToCheck));
		
		
		EncashmentsFilter filter = new EncashmentsFilter();
		filter.setForthcomingEncDateFrom(cal.getTime());
		cal.add(Calendar.DAY_OF_YEAR, 1);
		filter.setForthcomingEncDateTo(cal.getTime());
		filter.setEncMode(EncashmentFilterMode.EMERGENCY.getId());
		
		List<AtmEncashmentItem> encList = AtmEncashmentController.getAtmEncashmentList(sessionHolder ,filter);
		for(AtmEncashmentItem item : encList){
			List<AtmUserItem> atmUsers = AtmInfoController.getAtmUsers(connection, item.getAtmID(), true);
			Pair routePair = DynamicRoutingController.getRouteForEncashment(connection, item);
			if(routePair != null){
				if(ActualStateController.getAtmDeviceState(sessionHolder, item.getAtmID())){
					boolean needsReorder = DynamicRoutingController.checkEncNeedsReorder(connection, item);
					if(needsReorder){
						NotifyMessageController.insertNotifyMessage(sessionHolder, NotifyMessageType.ROUTE_DEVICE_OVERLOAD, atmUsers, 
								Integer.toString(item.getAtmID()), routePair.getKey(), routePair.getLabel());
					}
				} else {
					NotifyMessageController.insertNotifyMessage(sessionHolder, NotifyMessageType.ROUTE_DEVICE_BROKEN, atmUsers, 
							Integer.toString(item.getAtmID()), routePair.getKey(), routePair.getLabel());
				}
			} else {
				if(item.isEmergencyEncashment()){
					Pair nearestAtmPair = DynamicRoutingController.getNearestAtmAndRoute(connection, item); 
					if(nearestAtmPair != null){
						NotifyMessageController.insertNotifyMessage(sessionHolder, NotifyMessageType.ROUTE_DEVICE_NEW, atmUsers, 
								Integer.toString(item.getAtmID()), nearestAtmPair.getKey(), nearestAtmPair.getLabel());
					}
				}
			}
		}
		
	}
	
	public static void checkEncashmentsNoActualization(ISessionHolder sessionHolder, Connection connection, String personid) throws SQLException{//, Date dateToCheck
		//TODO: add atms to actualize and main curr usage flag
		//Calendar cal = Calendar.getInstance();
		//cal.setTime(CmUtils.truncateDate(dateToCheck));
		
		
		//EncashmentsFilter filter = new EncashmentsFilter();
		//filter.setForthcomingEncDateFrom(cal.getTime());
		
		//cal.add(Calendar.DAY_OF_YEAR, 1);
		
		//filter.setForthcomingEncDateTo(cal.getTime());
		//filter.setEncMode(EncashmentFilterMode.EMERGENCY.getId());
		List<IFilterItem<Integer>> atmList = AtmPidListController.getAtmListForUser(sessionHolder, personid);
		CmCommonController.insertTempAtms(sessionHolder, atmList);
		MonitoringFilter addFilter = new MonitoringFilter();
		//addFilter.setAtmID(atmList);
		List<AtmActualStateItem> actualStateList = ActualStateController.getAtmActualStateList(sessionHolder, addFilter);
		for(AtmActualStateItem item : actualStateList){
			if (!item.isAtmIsOk())
				NotifyMessageController.insertNotifyMessage(sessionHolder, NotifyMessageType.DEVICE_BROKEN, Integer.valueOf(personid), 
						Integer.toString(item.getAtmID()));
			if (item.isCurrRemainingAlert())
				NotifyMessageController.insertNotifyMessage(sessionHolder, NotifyMessageType.DEVICE_CURR_REMAINING_ALERT, Integer.valueOf(personid), 
						Integer.toString(item.getAtmID()));
			if (item.isBalancesAlert())
				NotifyMessageController.insertNotifyMessage(sessionHolder, NotifyMessageType.DEVICE_BALANCES_ALERT, Integer.valueOf(personid), 
						Integer.toString(item.getAtmID()));
			if (item.isEmergencyEncashment())
				NotifyMessageController.insertNotifyMessage(sessionHolder, NotifyMessageType.DEVICE_EMERGENCY_REPLENISHMENT, Integer.valueOf(personid), 
						Integer.toString(item.getAtmID()));

		}
		
	}
	
	public static void checkEncashments(ISessionHolder sessionHolder, Connection connection, String personid) throws SQLException{
		ActualStateManager.actualizeAtmState(sessionHolder, connection, true);
		checkEncashmentsNoActualization(sessionHolder, connection, personid);
	}

	
}
