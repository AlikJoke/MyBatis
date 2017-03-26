package ru.bpc.cm.cashmanagement;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.utils.IFilterItem;

public class CmCommonUtils {

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");
	
	public static boolean checkAtmCashIn(ISessionHolder sessionHolder, int atmId) {
		return AtmCassettesController.getAtmCassCount(sessionHolder, atmId, AtmCassetteType.CASH_IN_CASS) != 0;
		
	}
	
	public static boolean checkAtmRecyclingPreload(ISessionHolder sessionHolder, int atmId) {
		return AtmCassettesController.getAtmCassCount(sessionHolder, atmId, AtmCassetteType.CASH_OUT_CASS) == 0
				&& AtmCassettesController.getAtmCassCount(sessionHolder, atmId, AtmCassetteType.CASH_IN_R_CASS) != 0;
	}
	
	public static Map<Integer, String> getCurrenciesA3List(
			ISessionHolder sessionHolder, Connection connection, List<Integer> currList) {
		Map<Integer, String> currA3Map = new LinkedHashMap<Integer, String>();
		for (Integer curr : currList) {
			if (curr == CashManagementConstants.CASH_IN_CURR_CODE) {
				currA3Map.put(curr,
						CashManagementConstants.CASH_IN_CURR_CODE_A3);
			} else {
				currA3Map.put(curr, CmCommonController
						.getCurrCodeA3(sessionHolder, curr));
			}
		}
		return currA3Map;
	}
	
	public static void storeTempAtmList(ISessionHolder sessionHolder, List<IFilterItem<Integer>> selectedAtms){
		try {
			CmCommonController.deleteTempAtms(sessionHolder);
			CmCommonController.insertTempAtms(sessionHolder, selectedAtms);
		} catch (SQLException e) {
			_logger.error("", e);
		}
	}
	
	public static void storeTempAtmIdList(ISessionHolder sessionHolder, List<Integer> selectedAtms){
		try {
			CmCommonController.deleteTempAtms(sessionHolder);
			CmCommonController.insertTempAtmsIds(sessionHolder, selectedAtms);
		} catch (SQLException e) {
			_logger.error("", e);
		}
	}
	
	public static void storeTempAtmGroupList(ISessionHolder sessionHolder, List<IFilterItem<Integer>> selectedAtmGroups){
		try {
			CmCommonController.deleteTempAtmGroups(sessionHolder);
			CmCommonController.insertTempAtmGroups(sessionHolder, selectedAtmGroups);
		} catch (SQLException e) {
			_logger.error("", e);
		}
	}
	
	public static void storeTempAtmGroupIdList(ISessionHolder sessionHolder, List<Integer> selectedAtmGroups){
		try {
			CmCommonController.deleteTempAtmGroups(sessionHolder);
			CmCommonController.insertTempAtmGroupsIds(sessionHolder, selectedAtmGroups);
		} catch (SQLException e) {
			_logger.error("", e);
		}
	}
}
