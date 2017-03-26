package ru.bpc.cm.cashmanagement;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.orm.CmCommonMapper;
import ru.bpc.cm.cashmanagement.orm.items.CodesItem;
import ru.bpc.cm.cashmanagement.orm.items.ConvertionsRateItem;
import ru.bpc.cm.cashmanagement.orm.items.FullAddressItem;
import ru.bpc.cm.config.utils.ORMUtils;
import ru.bpc.cm.items.encashments.AtmStatusItem;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.enums.AtmGroupType;
import ru.bpc.cm.items.enums.AtmStatusResponse;
import ru.bpc.cm.items.enums.AtmTypeByDemand;
import ru.bpc.cm.items.enums.AtmTypeByOperations;
import ru.bpc.cm.items.enums.CalendarDayType;
import ru.bpc.cm.items.enums.EncashmentLogType;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;
import ru.bpc.cm.utils.db.JdbcUtils;
import ru.bpc.structs.collection.SeparatePrintedCollection;

public class CmCommonController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<CmCommonMapper> getMapperClass() {
		return CmCommonMapper.class;
	}

	/**
	 * Used to get encashment's ID from encashment's Date.
	 *
	 * @param sessionHolder
	 *            - MyBatis session provider
	 * @param atmID
	 *            - ATM ID
	 * @param encDate
	 *            - date of encashment
	 * @return ID of encashment
	 */
	public static int getEncID(ISessionHolder sessionHolder, int atmId, Date encDate) {
		Integer encID = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			encID = session.getMapper(getMapperClass()).getEncID(new java.sql.Timestamp(encDate.getTime()), atmId);
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("atmId = " + atmId, e);
		} finally {
			session.close();
		}
		return encID == null ? 0 : encID;
	}

	public static boolean getEncEmergency(ISessionHolder sessionHolder, int atmId, Date encDate) {
		boolean encID = false;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Boolean emergency = session.getMapper(getMapperClass())
					.getEncEmergency(new java.sql.Timestamp(encDate.getTime()), atmId);
			encID = emergency == null ? false : emergency;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("atmID = " + atmId, e);
		} finally {
			session.close();
		}
		return encID;
	}

	public static List<Integer> getAtmCurrencies(ISessionHolder sessionHolder, int atmId) {
		List<Integer> atmCurrencies = new ArrayList<Integer>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<CodesItem> codes = session.getMapper(getMapperClass()).getAtmCurrencies(atmId);
			for (CodesItem item : codes) {
				atmCurrencies.add(item.getMainCurrCode());
				if (item.getSecCurrCode() != null && item.getSecCurrCode() > 0)
					atmCurrencies.add(item.getSecCurrCode());

				if (item.getSec2CurrCode() != null && item.getSec2CurrCode() > 0)
					atmCurrencies.add(item.getSec2CurrCode());

				if (item.getSec3CurrCode() != null && item.getSec3CurrCode() > 0)
					atmCurrencies.add(item.getSec3CurrCode());
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return atmCurrencies;
	}

	public static List<Integer> getAtmCurrencies(ISessionHolder sessionHolder) {
		List<Integer> atmCurrencies = new ArrayList<Integer>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<Integer> tempAtmCurrencies = session.getMapper(getMapperClass()).getAtmCurrencies();
			for (int i = 0; i < tempAtmCurrencies.size() && i < 4; i++)
				atmCurrencies.add(tempAtmCurrencies.get(i));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return atmCurrencies;
	}

	public static List<Integer> getCurrencyDenominations(ISessionHolder sessionHolder, int currency)
			throws ForecastException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Integer> denominations = new ArrayList<Integer>();
		try {
			denominations.addAll(session.getMapper(getMapperClass()).getCurrencyDenominations(currency));
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("currency = " + currency, e);
		} finally {
			session.close();
		}
		return denominations;
	}

	public static int getCashInEncID(ISessionHolder sessionHolder, int atmId, Date encDate) {
		int encID = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer cashInEncId = session.getMapper(getMapperClass())
					.getCashInEncID(new java.sql.Timestamp(encDate.getTime()), atmId);

			encID = cashInEncId == null ? 0 : cashInEncId;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("atmID = " + atmId, e);
		} finally {
			session.close();
		}
		return encID;
	}

	/**
	 * Used to get code of main currency of ATM
	 *
	 * @param sessionHolder
	 *            - MyBatis session provider
	 * @param atmID
	 *            - ATM ID
	 * @return currency code
	 * @throws Exception
	 */
	public static int getMainCurrCode(ISessionHolder sessionHolder, int atmId) {
		int mainCurrCode = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer mainCurrCodeObject = session.getMapper(getMapperClass()).getMainCurrCode(atmId);
			mainCurrCode = mainCurrCodeObject == null ? 0 : mainCurrCodeObject;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("", e);
		} finally {
			session.close();
		}
		return mainCurrCode;
	}

	/**
	 * Used to get main currency usage flag for ATM
	 *
	 * @param sessionHolder
	 *            - MyBatis session provider
	 * @param atmID
	 *            - ATM ID
	 * @return currency code enabled flag
	 * @throws Exception
	 */
	public static boolean getMainCurrEnabled(ISessionHolder sessionHolder, int atmId) {
		boolean mainCurrEnabled = false;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Boolean mainCurrEnabledObject = session.getMapper(getMapperClass()).getMainCurrEnabled(atmId);
			mainCurrEnabled = mainCurrEnabledObject == null ? false : mainCurrEnabledObject;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("", e);
		} finally {
			session.close();
		}
		return mainCurrEnabled;
	}

	public static String getCurrCodeA3(ISessionHolder sessionHolder, int currCode) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			return session.getMapper(getMapperClass()).getCurrCodeA3(currCode);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return null;
	}

	public static String getInstIdForAtm(ISessionHolder sessionHolder, int atmId) {
		String isntId = "";
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			String instIdTemp = session.getMapper(getMapperClass()).getInstIdForAtm(atmId);
			if (instIdTemp != null)
				isntId = instIdTemp;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("", e);
		} finally {
			session.close();
		}
		return isntId;
	}

	public static AtmTypeByDemand getAtmTypeByDemand(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer type = session.getMapper(getMapperClass()).getAtmTypeByDemand(atmId);
			if (type == CalendarDayType.SALARY_DAYS.getCalendarDayType())
				return AtmTypeByDemand.SALARY;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return AtmTypeByDemand.NON_SALARY;
	}

	public static AtmTypeByOperations getAtmTypeByOperations(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer type = session.getMapper(getMapperClass()).getAtmTypeByOperations(atmId);
			if (type != null)
				return CmUtils.getEnumValueById(AtmTypeByOperations.class, type);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return AtmTypeByOperations.NOT_DEFINED;
	}

	public static int getAtmCassCount(ISessionHolder sessionHolder, int atmId, AtmCassetteType cassType) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer cnt = session.getMapper(getMapperClass()).getAtmCassCount(atmId, cassType.getId());
			if (cnt != null)
				return cnt;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return 0;
	}

	/**
	 * Used to get code of secondary currency of ATM
	 *
	 * @param sessionHolder
	 *            - MyBatis session provider
	 * @param atmID
	 *            - ATM ID
	 * @return currency code
	 */
	public static int getSecCurrCode(ISessionHolder sessionHolder, int atmId) {
		int secCurrCode = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer secCurrCodeObject = session.getMapper(getMapperClass()).getSecCurrCode(atmId);
			secCurrCode = secCurrCodeObject == null ? 0 : secCurrCodeObject;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("", e);
		} finally {
			session.close();
		}
		return secCurrCode;
	}

	/**
	 * Used to get code of secondary2 currency of ATM
	 *
	 * @param sessionHolder
	 *            - MyBatis session provider
	 * @param atmID
	 *            - ATM ID
	 * @return currency code
	 */
	public static int getSec2CurrCode(ISessionHolder sessionHolder, int atmId) {
		int sec2CurrCode = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer sec2CurrCodeObject = session.getMapper(getMapperClass()).getSec2CurrCode(atmId);
			sec2CurrCode = sec2CurrCodeObject == null ? 0 : sec2CurrCodeObject;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("", e);
		} finally {
			session.close();
		}
		return sec2CurrCode;
	}

	/**
	 * Used to get code of secondary3 currency of ATM
	 *
	 * @param sessionHolder
	 *            - MyBatis session provider
	 * @param atmID
	 *            - ATM ID
	 * @return currency code
	 */
	public static int getSec3CurrCode(ISessionHolder sessionHolder, int atmId) {
		int sec3CurrCode = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Integer sec3CurrCodeObject = session.getMapper(getMapperClass()).getSec3CurrCode(atmId);
			sec3CurrCode = sec3CurrCodeObject == null ? 0 : sec3CurrCodeObject;
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("", e);
		} finally {
			session.close();
		}
		return sec3CurrCode;
	}

	public static String getAtmAttribute(ISessionHolder sessionHolder, int atmId, AtmAttribute atmAttribute)
			throws ForecastException {
		String attributeValue = null;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			attributeValue = session.getMapper(getMapperClass()).getAtmAttribute(atmAttribute.getAttrID(),
					atmAttribute.getGroupType().getId(), atmId);
			if (attributeValue == null) {
				if (atmAttribute.isRequired()) {
					throw new ForecastException(ForecastException.ATM_ATTRIBUTE_NOT_DEFINED);
				} else {
					attributeValue = "0";
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return attributeValue;
	}

	public static Map<Integer, String> getAtmAttributes(ISessionHolder sessionHolder, int atmId)
			throws ForecastException {
		Map<Integer, String> attributes = new HashMap<Integer, String>(AtmAttribute.values().length);
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<ObjectPair<Integer, String>> atmAttributes = session.getMapper(getMapperClass())
					.getAtmAttributes(AtmGroupType.USER_TO_ATM.getId(), atmId);

			for (ObjectPair<Integer, String> attr : atmAttributes)
				attributes.put(attr.getKey(), attr.getValue());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return attributes;
	}

	public static Double convertValue(ISessionHolder sessionHolder, int srcCurr, int destCurr, Double value,
			String instId) {
		if (srcCurr == 0 || destCurr == 0 || srcCurr == destCurr) {
			return value;
		}
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<ObjectPair<String, Double>> tmpResult = session.getMapper(getMapperClass()).convertValue(destCurr,
					srcCurr, instId);

			if (!tmpResult.isEmpty()) {
				ObjectPair<String, Double> pair = tmpResult.get(0);
				String multFlag = pair.getKey();
				if (multFlag.equals("M")) {
					return value * pair.getValue();
				}
				if (multFlag.equals("D")) {
					return value / pair.getValue();
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return value;
	}

	// ----------------
	public static Map<String, Map<String, ObjectPair<String, Double>>> getConvertionsRates(ISessionHolder sessionHolder,
			int atmId, List<Integer> currencies) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Map<String, Map<String, ObjectPair<String, Double>>> res = new HashMap<String, Map<String, ObjectPair<String, Double>>>();
		try {
			List<ConvertionsRateItem> rates = session.getMapper(getMapperClass()).getConvertionsRates(atmId,
					currencies);
			for (ConvertionsRateItem item : rates) {
				String srcCurr = item.getSrcCurrCode();
				if (!res.containsKey(srcCurr))
					res.put(srcCurr, new HashMap<String, ObjectPair<String, Double>>());

				res.get(srcCurr).put(item.getDestCurrCode(),
						new ObjectPair<String, Double>(item.getMultipleFlag(), item.getCnvtRate()));
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}

	public static void insertEncashmentMessage(ISessionHolder sessionHolder, int encPlanID, String message,
			EncashmentLogType type, int personId, String... params) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).insertEncashmentMessage(
					ORMUtils.getNextSequence(session, "s_enc_plan_log_id"), encPlanID,
					new Timestamp(new Date().getTime()), personId, message, type.getEncashmentLogType(),
					new SeparatePrintedCollection<String>(Arrays.asList(params)).toString());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void deleteEncashmentMessage(ISessionHolder sessionHolder, int logId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).deleteEncashmentMessage(logId);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static AtmStatusResponse getCashOutStatus(ISessionHolder sessionHolder, int atmId) {
		AtmStatusResponse res = null;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Date lastStatDate = null;
		int encID = 0;
		try {
			CmCommonMapper mapper = session.getMapper(getMapperClass());
			ObjectPair<Integer, java.sql.Date> pair = mapper.getCashOutStatDate(atmId);

			if (pair != null) {
				lastStatDate = JdbcUtils.getDate(pair.getValue());
				encID = pair.getKey();
			}

			List<Integer> statuses = mapper.getCashInStatus(atmId, encID, JdbcUtils.getSqlDate(lastStatDate));

			int working = 0;
			int broken = 0;

			for (Integer status : statuses) {
				int temp = status;
				if (temp == 0) {
					working++;
				} else {
					broken++;
				}
			}
			if (working == 0) {
				res = AtmStatusResponse.CASH_OUT_NW;
			} else {
				if (broken > 0) {
					res = AtmStatusResponse.CASH_OUT_PW;
				} else {
					res = AtmStatusResponse.CASH_OUT_OK;
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}

	public static AtmStatusResponse getCashInStatus(ISessionHolder sessionHolder, int atmId) {
		AtmStatusResponse res = null;
		Date lastStatDate = null;
		int encID = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CmCommonMapper mapper = session.getMapper(getMapperClass());
			ObjectPair<Integer, java.sql.Date> pair = mapper.getCashInStatDate(atmId);

			if (pair != null) {
				lastStatDate = JdbcUtils.getDate(pair.getValue());
				encID = pair.getKey();
			}
			List<Integer> statuses = mapper.getCashInStatus(atmId, encID, JdbcUtils.getSqlDate(lastStatDate));

			if (!statuses.isEmpty()) {
				Integer status = statuses.get(0);
				if (status == 0) {
					return AtmStatusResponse.CASH_IN_OK;
				} else {
					return AtmStatusResponse.CASH_IN_NW;
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}

	public static Pair getAtmAddressAndName(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			FullAddressItem item = session.getMapper(getMapperClass()).getAtmAddressAndName(atmId);

			if (item != null)
				return new Pair(CmUtils.getAtmFullAdrress(item.getState(), item.getCity(), item.getStreet()),
						item.getName());
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return new Pair(null, null);
	}

	public static Pair getAtmExtIdAndName(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Pair pair = session.getMapper(getMapperClass()).getAtmExtIdAndName(atmId);
			if (pair != null)
				return pair;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return new Pair(null, null);
	}

	public static Pair getAtmGroupNameAndDescx(ISessionHolder sessionHolder, int groupId) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Pair pair = session.getMapper(getMapperClass()).getAtmGroupNameAndDescx(groupId);
			if (pair != null)
				return pair;
			return new Pair(null, null);
		} finally {
			session.close();
		}
	}

	public static AtmStatusItem getAtmStatus(ISessionHolder sessionHolder, int atmId) {
		AtmStatusItem status = new AtmStatusItem();
		status.setAtmID(atmId);
		status.setAtmTypeByDemand(CmCommonController.getAtmTypeByDemand(sessionHolder, atmId));
		Pair addressAndName = CmCommonController.getAtmAddressAndName(sessionHolder, atmId);
		status.setAtmName(addressAndName.getLabel());
		status.setDescx(addressAndName.getKey());
		status.setAtmTypeByOperations(CmCommonController.getAtmTypeByOperations(sessionHolder, atmId));

		status.setCashInStatus(AtmStatusResponse.CASH_IN_OK);
		status.setCashOutStatus(AtmStatusResponse.CASH_OUT_OK);

		return status;
	}

	public static void insertTempAtms(ISessionHolder sessionHolder, List<IFilterItem<Integer>> selectedAtms)
			throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ORMUtils.createTemporaryTableIfNotExists(session, "t_cm_temp_atm_list");
			CmCommonMapper mapper = session.getMapper(getMapperClass());
			for (IFilterItem<Integer> item : selectedAtms)
				mapper.insertTempAtms(item.getValue());
			mapper.flush();
		} finally {
			session.close();
		}
	}

	public static void insertTempAtmsIds(ISessionHolder sessionHolder, List<Integer> selectedAtms) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ORMUtils.createTemporaryTableIfNotExists(session, "t_cm_temp_atm_list");
			CmCommonMapper mapper = session.getMapper(getMapperClass());

			for (Integer item : selectedAtms)
				mapper.insertTempAtmGroupsIds(item);
			mapper.flush();
		} finally {
			session.close();
		}
	}

	public static void deleteTempAtms(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ORMUtils.createTemporaryTableIfNotExists(session, "t_cm_temp_atm_list");
			session.getMapper(getMapperClass()).deleteTempAtms();
		} finally {
			session.close();
		}
	}

	public static void insertTempAtmGroups(ISessionHolder sessionHolder, List<IFilterItem<Integer>> selectedAtmGroups)
			throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CmCommonMapper mapper = session.getMapper(getMapperClass());
			ORMUtils.createTemporaryTableIfNotExists(session, "t_cm_temp_atm_group_list");
			if (selectedAtmGroups != null && !selectedAtmGroups.isEmpty())
				for (IFilterItem<Integer> item : selectedAtmGroups)
					mapper.insertTempAtmGroups(item.getValue());
			mapper.flush();
		} finally {
			session.close();
		}
	}

	public static void insertTempAtmGroupsIds(ISessionHolder sessionHolder, List<Integer> selectedAtmGroups)
			throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			CmCommonMapper mapper = session.getMapper(getMapperClass());
			ORMUtils.createTemporaryTableIfNotExists(session, "t_cm_temp_atm_group_list");
			for (Integer item : selectedAtmGroups)
				mapper.insertTempAtmGroupsIds(item);
			mapper.flush();
		} finally {
			session.close();
		}
	}

	public static void deleteTempAtmGroups(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ORMUtils.createTemporaryTableIfNotExists(session, "t_cm_temp_atm_group_list");
			session.getMapper(getMapperClass()).deleteTempAtmGroups();
		} finally {
			session.close();
		}
	}

}
