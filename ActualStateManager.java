package ru.bpc.cm.monitoring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.AtmPidListController;
import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.cashmanagement.CmCommonUtils;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.forecasting.anyatm.CheckerCommons;
import ru.bpc.cm.forecasting.anyatm.EndDatesForecast;
import ru.bpc.cm.forecasting.anyatm.HistoryDemandForecast;
import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.DayStartEnd;
import ru.bpc.cm.forecasting.controllers.HistoryDemandController;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.enums.AtmTypeByOperations;
import ru.bpc.cm.items.enums.EncashmentActionType;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;
import ru.bpc.cm.items.monitoring.AtmGroupStateItem;
import ru.bpc.cm.items.monitoring.MonitoringException;
import ru.bpc.cm.items.settings.calendars.coeffs.AtmCalendarDay;
import ru.bpc.cm.items.settings.calendars.coeffs.AtmCalendarDaysMode;
import ru.bpc.cm.items.settings.calendars.coeffs.AtmDefaultCoeffs;
import ru.bpc.cm.items.settings.calendars.coeffs.HistoryDemandItem;
import ru.bpc.cm.settings.AtmInfoController;
import ru.bpc.cm.settings.calendars.CalendarCoeffsController;
import ru.bpc.cm.settings.calendars.DefaultCoeffsHolder;
import ru.bpc.cm.settings.calendars.DefaultCoeffsProcessor;
import ru.bpc.cm.settings.calendars.ForecastAtmCalendarDayProcessor;
import ru.bpc.cm.settings.calendars.ForecastAtmCalendarSalaryProcessor;
import ru.bpc.cm.settings.calendars.NightCoeffsProcessor;
import ru.bpc.cm.settings.calendars.StatAtmCalendarDayProcessor;
import ru.bpc.cm.settings.calendars.StatAtmCalendarLastDayProcessor;
import ru.bpc.cm.settings.calendars.StatAtmCalendarSalaryProcessor;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

public class ActualStateManager {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	public static void actualizeAtmState(ISessionHolder sessionHolder, Connection con,List<IFilterItem<Integer>> filterList, boolean useMainCurr) {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing ATM state (user), start : {}", new Date());
		for (IFilterItem<Integer> item : filterList) {
			if(debug){
				logger.debug("atmId : {}", item.getValue());		
			}
//			actualizeAllAtmCalendarCoeffsForAtm(con, item.getValue());
//			actualizeLastAtmCalendarCoeffsForAtm(con, item.getValue());
			actualizeFutureAtmCalendarCoeffsForAtm(con, item.getValue());
			actualizeAtmStateForAtm(sessionHolder, con, item.getValue(),false, useMainCurr ? CmCommonController.getMainCurrEnabled(con, item.getValue()) : false);
			reFillRemainings(sessionHolder, con, item.getValue());
		}
		logger.debug("Actualizing ATM state (user), finish : {}", new Date());
	}
	
	public static void actualizeAtmState(ISessionHolder sessionHolder, Connection con, boolean useMainCurr) throws SQLException {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing ATM state (all), start : {}", new Date());
		for (IFilterItem<Integer> item : AtmPidListController.getAtmListFull(con)) {
			if(debug){
				logger.debug("atmId : {}", item.getValue());		
			}
			actualizeAtmStateForAtm(sessionHolder, con, item.getValue(),true, useMainCurr ? CmCommonController.getMainCurrEnabled(con, item.getValue()) : false);
                        actualizeDemandHistoryForAtm(con, item.getValue());
			reFillRemainings(sessionHolder, con, item.getValue());
		}
		logger.debug("Actualizing ATM state (all), finish : {}", new Date());
		ActualStateController.updateCalculatedRemainingForAtms(sessionHolder);
	}
	
	public static void actualizeAtmDemandHistory(Connection con,List<Integer> filterList) throws SQLException {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing ATM demand history (user), start : {}", new Date());
		for (Integer item : filterList) {
			if(debug){
				logger.debug("atmId : {}", item);		
			}
			actualizeDemandHistoryForAtmForPeriod(con, item);
		}
		logger.debug("Actualizing ATM demand history (user), finish : {}", new Date());
	}
	
	public static void actualizeAllAtmCalendarCoeffs(ISessionHolder sessionHolder, Connection con, boolean useMainCurr) throws SQLException {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing All ATM coeffs (all), start : {}", new Date());
		for (IFilterItem<Integer> item : AtmPidListController.getAtmListFull(con)) {
			if(debug){
				logger.debug("atmId : {}", item.getValue());		
			}
			actualizeAllAtmCalendarCoeffsForAtm(con, item.getValue());
			actualizeAtmStateForAtm(sessionHolder, con, item.getValue(),true, useMainCurr ? CmCommonController.getMainCurrEnabled(con, item.getValue()) : false);
		}
		logger.debug("Actualizing All ATM coeffs (all), finish : {}", new Date());
	}
	
	public static void actualizeAtmCalendarCoeffs(ISessionHolder sessionHolder, Connection con, List<IFilterItem<Integer>> atmList, boolean useMainCurr) throws SQLException {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing ATM coeffs ("+Arrays.toString(atmList.toArray())+"), start : {}", new Date());
		for (IFilterItem<Integer> item : atmList) {
			if(debug){
				logger.debug("atmId : {}", item.getValue());		
			}
			actualizeAllAtmCalendarCoeffsForAtm(con, item.getValue());
			actualizeAtmStateForAtm(sessionHolder, con, item.getValue(),true, useMainCurr ? CmCommonController.getMainCurrEnabled(con, item.getValue()) : false);
		}
		logger.debug("Actualizing All ATM coeffs (all), finish : {}", new Date());
	}
	
	public static void actualizeAtmCalendarCoeffsForSimpleList(ISessionHolder sessionHolder, Connection con, List<Integer> atmList, boolean useMainCurr) throws SQLException {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing ATM coeffs ("+Arrays.toString(atmList.toArray())+"), start : {}", new Date());
		for (Integer item : atmList) {
			if(debug){
				logger.debug("atmId : {}", item);		
			}
			actualizeAllAtmCalendarCoeffsForAtm(con, item);
			actualizeAtmStateForAtm(sessionHolder, con, item,true, useMainCurr ? CmCommonController.getMainCurrEnabled(con, item) : false);
		}
		logger.debug("Actualizing All ATM coeffs (all), finish : {}", new Date());
	}
	
	public static void actualizeLastAllAtmCalendarCoeffs(Connection connection) {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing Last ATM coeffs (all), start : {}", new Date());
		for (IFilterItem<Integer> item : AtmPidListController.getAtmListFull(connection)) {
			if(debug){
				logger.debug("atmId : {}", item.getValue());		
			}
			actualizeLastAtmCalendarCoeffsForAtm(connection, item.getValue());
		}
		logger.debug("Actualizing Last ATM coeffs (all), finish : {}", new Date());
		
	}
	
	public static void actualizeLastAtmCalendarCoeffs(Connection connection, List<IFilterItem<Integer>> atmList) {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing Last ATM coeffs ("+Arrays.toString(atmList.toArray())+"), start : {}", new Date());
		for (IFilterItem<Integer> item : atmList) {
			if(debug){
				logger.debug("atmId : {}", item.getValue());		
			}
			actualizeLastAtmCalendarCoeffsForAtm(connection, item.getValue());
		}
		logger.debug("Actualizing Last ATM coeffs ("+Arrays.toString(atmList.toArray())+"), finish : {}", new Date());
		
	}
	
	public static void actualizeLastAtmCalendarCoeffsForSimpleList(Connection connection, List<Integer> atmList) {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing Last ATM coeffs ("+Arrays.toString(atmList.toArray())+"), start : {}", new Date());
		for (Integer item : atmList) {
			if(debug){
				logger.debug("atmId : {}", item);		
			}
			actualizeLastAtmCalendarCoeffsForAtm(connection, item);
		}
		logger.debug("Actualizing Last ATM coeffs ("+Arrays.toString(atmList.toArray())+"), finish : {}", new Date());
	}
	
	public static void actualizeAtmCassStatuses(ISessionHolder sessionHolder, Connection con, boolean useMainCurr) throws SQLException {
		boolean debug = logger.isDebugEnabled();
		logger.debug("Actualizing ATM cass statuses (all), start : {}", new Date());
		for (IFilterItem<Integer> item : AtmPidListController.getAtmListFull(con)) {
			if(debug){
				logger.debug("atmId : {}", item.getValue());		
			}
			actualizeAtmStateForAtm(sessionHolder, con, item.getValue(),true, useMainCurr ? CmCommonController.getMainCurrEnabled(con, item.getValue()) : false);
		}
		logger.debug("Actualizing ATM cass statuses (all), finish : {}", new Date());
	}
	
	protected static void actualizeAllAtmCalendarCoeffsForAtm(Connection connection, int atmId) {
		List<AtmDefaultCoeffs> defaultCoeffsList = null;
		List<AtmCalendarDay> dayList = null;
		Date endOfStatsDate = null;
		DefaultCoeffsHolder coeffsHolder = null;
		
		List<Integer> coCurrencyList = null;
		List<Integer> ciCurrencyList = null;
		
		coCurrencyList = CmCommonController.getAtmCurrencies(connection, atmId);
		if(CmCommonUtils.checkAtmCashIn(connection, atmId)){
			ciCurrencyList = CmCommonController.getAtmCurrencies(connection);
		} else {
			ciCurrencyList = Collections.emptyList();
		}
		
		Set<Date> holidays = CalendarCoeffsController.getHolidays(connection, atmId);
		DayStartEnd dayStartEnd = new DayStartEnd(connection, atmId);
		
		endOfStatsDate = 
				getEndOfStatsDateForCoeffs(connection, atmId);
		if(endOfStatsDate == null){
			return;
		}
		
		defaultCoeffsList = DefaultCoeffsProcessor.
				calculateDefaultCoeffs(connection,atmId,coCurrencyList,ciCurrencyList, holidays, endOfStatsDate);
		CalendarCoeffsController.
				saveDefaultCoeffs(connection, atmId, defaultCoeffsList);
		
		coeffsHolder = new DefaultCoeffsHolder(connection, atmId);
		
		defaultCoeffsList = NightCoeffsProcessor.calculateNightCoeffs(connection, atmId, coCurrencyList,ciCurrencyList, coeffsHolder, dayStartEnd.getDayStart(), dayStartEnd.getDayEnd() ,endOfStatsDate);
		CalendarCoeffsController.
			saveDefaultCoeffs(connection, atmId, defaultCoeffsList);
		
		coeffsHolder = new DefaultCoeffsHolder(connection, atmId);
		
		if(!CalendarCoeffsController.checkAtmCalendarDaysExist(connection, atmId)){
			dayList = StatAtmCalendarDayProcessor.
					calculateStatAtmCalendarDays(connection,atmId, coCurrencyList, ciCurrencyList, coeffsHolder, endOfStatsDate, holidays);
			CalendarCoeffsController.
					saveAtmCalendarDays(connection, atmId, endOfStatsDate, dayList, AtmCalendarDaysMode.STATS);
			
			dayList = StatAtmCalendarSalaryProcessor.calculateLastThreeMonthStatAtmCalendarDays(connection, atmId, coCurrencyList,ciCurrencyList, coeffsHolder, endOfStatsDate, dayStartEnd.getDayStart(), dayStartEnd.getDayEnd());
			CalendarCoeffsController.
				saveAtmCalendarDay(connection, dayList);
		
		}
		
		dayList = StatAtmCalendarSalaryProcessor.calculateLastMonthStatAtmCalendarDays(connection, atmId, coCurrencyList,ciCurrencyList, coeffsHolder, endOfStatsDate, dayStartEnd.getDayStart(), dayStartEnd.getDayEnd());
		CalendarCoeffsController.
			saveAtmCalendarDay(connection, dayList);
		
		dayList = ForecastAtmCalendarDayProcessor.copyDaysForTwoMothsForward(connection, atmId,  coCurrencyList,ciCurrencyList, coeffsHolder, holidays, endOfStatsDate);
		CalendarCoeffsController.
			saveAtmCalendarDays(connection, atmId, endOfStatsDate, dayList, AtmCalendarDaysMode.FORECAST);
		
		dayList = StatAtmCalendarLastDayProcessor.calculateStatAtmCalendarDays(connection, atmId, coCurrencyList,ciCurrencyList,coeffsHolder, endOfStatsDate, dayStartEnd.getDayStart(), dayStartEnd.getDayEnd());
		CalendarCoeffsController.
			saveAtmCalendarDay(connection, dayList);
		
		dayList = ForecastAtmCalendarDayProcessor.copyDaysForTenDaysForward(connection, atmId, coCurrencyList,ciCurrencyList,coeffsHolder, holidays, endOfStatsDate);
		CalendarCoeffsController.
			saveAtmCalendarDay(connection, dayList);
		
		dayList = ForecastAtmCalendarSalaryProcessor.copyDaysForNearestSalaryPeriod(connection, atmId, coCurrencyList,ciCurrencyList, coeffsHolder, holidays, endOfStatsDate);
		CalendarCoeffsController.
			saveAtmCalendarDay(connection, dayList);

	}
	
	private static void actualizeLastAtmCalendarCoeffsForAtm(Connection connection, int atmId) {
		List<AtmCalendarDay> dayList = null;
		Date endOfStatsDate = null;

		List<Integer> coCurrencyList = null;
		List<Integer> ciCurrencyList = null;
		
		coCurrencyList = CmCommonController.getAtmCurrencies(connection, atmId);
		if(CmCommonUtils.checkAtmCashIn(connection, atmId)){
			ciCurrencyList = CmCommonController.getAtmCurrencies(connection);
		} else {
			ciCurrencyList = Collections.emptyList();
		}
		
		DefaultCoeffsHolder coeffsHolder = new DefaultCoeffsHolder(connection, atmId);
		DayStartEnd dayStartEnd = new DayStartEnd(connection, atmId);
		
		endOfStatsDate = 
				getEndOfStatsDateForCoeffs(connection, atmId);
		if(endOfStatsDate == null){
			return;
		}
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(CmUtils.truncateDate(endOfStatsDate));
		cal.add(Calendar.WEEK_OF_YEAR, -2);
		
		while(cal.getTime().before(endOfStatsDate)){
		
			dayList = StatAtmCalendarLastDayProcessor.calculateStatAtmCalendarDays(connection, atmId, coCurrencyList, ciCurrencyList, coeffsHolder, cal.getTime(), dayStartEnd.getDayStart(), dayStartEnd.getDayEnd());
			CalendarCoeffsController.
				saveAtmCalendarDay(connection, dayList);
			cal.add(Calendar.DAY_OF_YEAR, 1);
		}
		
		dayList = StatAtmCalendarSalaryProcessor.calculateLastMonthStatAtmCalendarDays(connection, atmId, coCurrencyList, ciCurrencyList, coeffsHolder, endOfStatsDate, dayStartEnd.getDayStart(), dayStartEnd.getDayEnd());
		CalendarCoeffsController.
			saveAtmCalendarDay(connection, dayList);
	}
	
	
	private static void actualizeFutureAtmCalendarCoeffsForAtm(Connection connection, int atmId) {
		List<AtmCalendarDay> dayList = null;
		Date endOfStatsDate = null;
		Set<Date> holidays = CalendarCoeffsController.getHolidays(connection, atmId);

		List<Integer> coCurrencyList = null;
		List<Integer> ciCurrencyList = null;
		
		coCurrencyList = CmCommonController.getAtmCurrencies(connection, atmId);
		if(CmCommonUtils.checkAtmCashIn(connection, atmId)){
			ciCurrencyList = CmCommonController.getAtmCurrencies(connection);
		} else {
			ciCurrencyList = Collections.emptyList();
		}
		
		DefaultCoeffsHolder coeffsHolder = new DefaultCoeffsHolder(connection, atmId);
		
		endOfStatsDate = 
				getEndOfStatsDateForCoeffs(connection, atmId);
		if(endOfStatsDate == null){
			return;
		}
		
		dayList = ForecastAtmCalendarDayProcessor.copyDaysForTwoMothsForward(connection, atmId, coCurrencyList, ciCurrencyList, coeffsHolder, holidays, endOfStatsDate);
		CalendarCoeffsController.
			saveAtmCalendarDays(connection, atmId, endOfStatsDate, dayList, AtmCalendarDaysMode.FORECAST);
		
		dayList = ForecastAtmCalendarDayProcessor.copyDaysForTenDaysForward(connection, atmId, coCurrencyList, ciCurrencyList, coeffsHolder, holidays, endOfStatsDate);
		CalendarCoeffsController.
			saveAtmCalendarDay(connection, dayList);
	
		dayList = ForecastAtmCalendarSalaryProcessor.copyDaysForNearestSalaryPeriod(connection, atmId, coCurrencyList, ciCurrencyList, coeffsHolder, holidays, endOfStatsDate);
		CalendarCoeffsController.
			saveAtmCalendarDay(connection, dayList);

	}

	private static void actualizeDemandHistoryForAtm(Connection connection, int atmId) {
		List<HistoryDemandItem> dayList = null;
		Date endOfStatsDate = null;

		endOfStatsDate = 
				getEndOfStatsDateForCoeffs(connection, atmId);
		if(endOfStatsDate == null){
			return;
		}
		
		dayList = HistoryDemandForecast.makeForecastForAtm(connection, atmId, endOfStatsDate);
		HistoryDemandController.saveAtmCalendarDays(connection, atmId, endOfStatsDate, dayList);
		
	}
	
	private static void actualizeDemandHistoryForAtmForPeriod(Connection connection, int atmId) {
		List<HistoryDemandItem> dayList = null;
		Date endOfStatsDate = null;
		Date startOfStatsDate = null;
		startOfStatsDate = 
				getStartOfStatsDateForCoeffs(connection, atmId);
		endOfStatsDate = 
				getEndOfStatsDateForCoeffs(connection, atmId);
		if(startOfStatsDate == null){
			return;
		}
		
		//TODO: cycle from start to end with limit
		Date tempDate;
		for (tempDate=startOfStatsDate; tempDate.compareTo(endOfStatsDate)<0; tempDate=DateUtils.addDays(tempDate, 1)){
			dayList = HistoryDemandForecast.makeForecastForAtm(connection, atmId, tempDate);//endOfStatsDate
			HistoryDemandController.saveAtmCalendarDays(connection, atmId, tempDate, dayList);//endOfStatsDate
		}
		
		
		
	}

	private static Date getEndOfStatsDateForCoeffs(Connection connection, int atmId){
		Date endOfStatsDate = 
				CheckerCommons.getStatsEnd(connection, atmId, Calendar.getInstance().getTime());
		
		if(endOfStatsDate != null){
			endOfStatsDate = CmUtils.truncateDate(endOfStatsDate);
		}
		return endOfStatsDate;
	}
	
	private static Date getStartOfStatsDateForCoeffs(Connection connection, int atmId){
		Date startOfStatsDate = 
				CheckerCommons.getStatsStart(connection, atmId);
		
		if(startOfStatsDate != null){
			startOfStatsDate = CmUtils.truncateDate(startOfStatsDate);
		}
		return startOfStatsDate;
	}
	
	//UPDATE LAST_UPDATE_DATE ONLY ON TASK CALL
	protected static void actualizeAtmStateForAtm(ISessionHolder sessionHolder, Connection con,int atmId,boolean updateLastUpdateDate, boolean useMainCurr){
		ObjectPair<Date, Integer> cashOutLastStat 	= null;
		ObjectPair<Date, Integer> cashInLastStat	= null;
		
		List<AtmCassetteItem> atmCassList = new ArrayList<AtmCassetteItem>();
		
		try {
			cashOutLastStat = ActualStateController.getCashOutLastStat(sessionHolder, atmId);
			ActualStateController.addCashOutCassettes(sessionHolder, atmId, cashOutLastStat.getValue(), atmCassList);
			
			cashInLastStat = ActualStateController.getCashInLastStat(sessionHolder, atmId);
			if(cashInLastStat != null && cashInLastStat.getValue() != 0 && 
					cashOutLastStat != null && cashOutLastStat.getValue() == 0){
				cashOutLastStat = cashInLastStat;
			}
			if(cashInLastStat != null && cashInLastStat.getValue() != 0){
				
				ActualStateController.addCashInRecyclingCassettes(sessionHolder, atmId, cashInLastStat.getValue(), atmCassList);
				atmCassList.add(new AtmCassetteItem(1, 0, CashManagementConstants.CASH_IN_CURR_CODE, AtmCassetteType.CASH_IN_CASS, false));
			}
			ActualStateController.saveAtmCassettes(sessionHolder, atmId, atmCassList);
			
			AnyAtmForecast outOfCurrsForecast = EndDatesForecast.
					makeForecastForAtm(con, atmId, cashOutLastStat == null ? cashInLastStat == null ? 
							new Date() : cashInLastStat.getKey() : cashOutLastStat.getKey(), useMainCurr ? CmCommonController.getMainCurrEnabled(con, atmId) : false);
			AtmTypeByOperations type = checkAtmTypeByOperations(con, atmId);
			if(type != null){
				AtmInfoController.saveAtmTypeByOperations(con, atmId, type);
			}	

			ActualStateController.actualizeAtmStateForAtm(sessionHolder, outOfCurrsForecast,cashOutLastStat,cashInLastStat,updateLastUpdateDate);
			
		} catch (Exception e) {
			logger.error("atmId = " + atmId,e);
		}
	}
	
	protected static void actualizeAtmCassStatusesForAtm(ISessionHolder sessionHolder, int atmId){
		ObjectPair<Date, Integer> cashOutLastStat 	= null;
		ObjectPair<Date, Integer> cashInLastStat	= null;
		List<AtmCassetteItem> atmCassList = new ArrayList<AtmCassetteItem>();
		
		try {
			cashOutLastStat = ActualStateController.getCashOutLastStat(sessionHolder, atmId);
			ActualStateController.addCashOutCassettes(sessionHolder, atmId, cashOutLastStat.getValue(), atmCassList);
			
			cashInLastStat = ActualStateController.getCashInLastStat(sessionHolder, atmId);
			if(cashInLastStat != null && cashInLastStat.getValue() != 0){
				
				ActualStateController.addCashInRecyclingCassettes(sessionHolder, atmId, cashInLastStat.getValue(), atmCassList);
				atmCassList.add(new AtmCassetteItem(1, 0, CashManagementConstants.CASH_IN_CURR_CODE, AtmCassetteType.CASH_IN_CASS, false));
			}
			ActualStateController.saveAtmCassettes(sessionHolder, atmId, atmCassList);
		} catch (Exception e) {
			logger.error("atmId = " + atmId,e);
		}
	}
	
	
	public static AtmGroupStateItem getAllGroupSumms(ISessionHolder sessionHolder, Connection connection,Map<IFilterItem<Integer>,List<IFilterItem<Integer>>> atmsToGroups,
			Date yesterday, Date today, Date tomorrow, Date afterTomorrow, Date weekAfterTomorrow) throws SQLException {
		
		AtmGroupStateItem allGroupsItem = new AtmGroupStateItem();
		
		allGroupsItem.setId("All Groups");
		allGroupsItem.setCurrInNextWeek(GroupStateController.getAtmsEncPeriodSums(sessionHolder, afterTomorrow, weekAfterTomorrow));
		allGroupsItem.setCurrInTomorrow(GroupStateController.getAtmsEncPlanSums(sessionHolder, tomorrow));
		allGroupsItem.setCurrInToday(GroupStateController.getAtmsEncPlanSums(sessionHolder, today));
		allGroupsItem.setCurrNowRemaining(GroupStateController.getAtmsRemainingSums(sessionHolder));
		allGroupsItem.setCurrOutYesterday(GroupStateController.getAtmsEncStatSums(sessionHolder, yesterday, EncashmentActionType.UNLOAD));


		
		for(Entry<IFilterItem<Integer>,List<IFilterItem<Integer>>> s : atmsToGroups.entrySet()){
			allGroupsItem.getSubItems().add(getOneGroupSumms(sessionHolder, connection, s.getKey().getValue(), s.getValue(),
					 yesterday, today, tomorrow, afterTomorrow, weekAfterTomorrow));
		}
		
		return allGroupsItem;
	}

	private static AtmGroupStateItem getOneGroupSumms(ISessionHolder sessionHolder, Connection connection,Integer groupId,List<IFilterItem<Integer>> atms,
			Date yesterday, Date today, Date tomorrow, Date afterTomorrow, Date weekAfterTomorrow) throws SQLException {
		
		CmCommonUtils.storeTempAtmList(connection, atms);
		
		AtmGroupStateItem groupItem = new AtmGroupStateItem();
		Pair groupNameAndDescx = CmCommonController.getAtmGroupNameAndDescx(connection, groupId);
		
		groupItem.setId(groupNameAndDescx.getKey());
		groupItem.setDescx(groupNameAndDescx.getLabel());
		groupItem.setCurrInNextWeek(GroupStateController.getAtmsEncPeriodSums(sessionHolder, afterTomorrow, weekAfterTomorrow));
		groupItem.setCurrInTomorrow(GroupStateController.getAtmsEncPlanSums(sessionHolder, tomorrow));
		groupItem.setCurrInToday(GroupStateController.getAtmsEncPlanSums(sessionHolder, today));
		groupItem.setCurrNowRemaining(GroupStateController.getAtmsRemainingSums(sessionHolder));
		groupItem.setCurrOutYesterday(GroupStateController.getAtmsEncStatSums(sessionHolder, yesterday, EncashmentActionType.UNLOAD));
		
		
		for(IFilterItem<Integer> atm : atms){
			int i = atm.getValue();
			AtmGroupStateItem item = new AtmGroupStateItem();
			item.setId(atm.getLabel());
			
			Pair atmAddressAndName = CmCommonController.getAtmAddressAndName(connection, i);
			
			item.setName(atmAddressAndName.getLabel());
			item.setDescx(atmAddressAndName.getKey());
			
			item.setCurrInNextWeek(GroupStateController.getAtmEncPeriodSums(sessionHolder, i, afterTomorrow, weekAfterTomorrow));
			item.setCurrInTomorrow(GroupStateController.getAtmEncPlanSums(sessionHolder, i, tomorrow));
			item.setCurrInToday(GroupStateController.getAtmEncPlanSums(sessionHolder, i, today));
			item.setCurrNowRemaining(GroupStateController.getAtmRemainingSums(sessionHolder, i));
			item.setCurrOutYesterday(GroupStateController.getAtmEncStatSums(sessionHolder, i, yesterday, EncashmentActionType.UNLOAD));

			groupItem.getSubItems().add(item);
		}
		
		return groupItem;
	}
	
	private static void reFillRemainings(ISessionHolder sessionHolder, Connection con, int atmId){
		try{
			Map<Integer,String> atmAttrs = CmCommonController.getAtmAttributes(con, atmId);
			
			int cashInVolume = 
					Integer.parseInt(CmUtils.getNVLValue
						(atmAttrs.get(Integer.valueOf(AtmAttribute.CASH_IN_CASS_VOLUME.getAttrID())),"0"));
			int rejectVolume = Integer.parseInt(CmUtils.getNVLValue
					(atmAttrs.get(Integer.valueOf(AtmAttribute.REJECT_CASS_VOLUME.getAttrID())),"0"));
			int cashInRVolume = Integer.parseInt(CmUtils.getNVLValue
					(atmAttrs.get(Integer.valueOf(AtmAttribute.CASH_IN_R_CASS_VOLUME.getAttrID())),"0"));
			
			ActualStateController.updateInitialsForAtm(sessionHolder, atmId, cashInVolume, rejectVolume, cashInRVolume);
		} catch (ForecastException e) {
			logger.error("atmID = " + atmId + " "+ e.getCode());
		}
	}
	
	private static AtmTypeByOperations checkAtmTypeByOperations(Connection con, int atmId){
		int coCassCount = CmCommonController.getAtmCassCount(con, atmId, AtmCassetteType.CASH_OUT_CASS);
		int ciCassCount = CmCommonController.getAtmCassCount(con, atmId, AtmCassetteType.CASH_IN_CASS);
		int crCassCount = CmCommonController.getAtmCassCount(con, atmId, AtmCassetteType.CASH_IN_R_CASS);
		
		if(coCassCount > 0){
			if(crCassCount > 0){
				return AtmTypeByOperations.CASH_OUT_PL_RECYCLING;
			} 
			if(ciCassCount > 0){
				return AtmTypeByOperations.CASH_OUT_PL_CASH_IN;
			}
			return AtmTypeByOperations.CASH_OUT_ONLY;
		}  else {
			if(crCassCount > 0){
				return AtmTypeByOperations.RECYCLING_ONLY;
			} 
			if(ciCassCount > 0){
				return AtmTypeByOperations.CASH_IN_ONLY;
			}
		}
		
		return null;
	}
	
	public static void checkLoadedBalances(ISessionHolder sessionHolder) throws MonitoringException {
		ActualStateController.checkLoadedBalances(sessionHolder);
	}
	
	public static void checkLoadedBalances(ISessionHolder sessionHolder, List<Integer> atmList)
			throws MonitoringException {
		ActualStateController.checkLoadedBalances(sessionHolder, atmList);
	}

}
