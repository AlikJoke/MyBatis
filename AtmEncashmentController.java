package ru.bpc.cm.encashments;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.Operation;
import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.cashmanagement.orm.items.FullCodesDto;
import ru.bpc.cm.config.utils.ORMUtils;
import ru.bpc.cm.encashments.orm.AtmEncashmentMapper;
import ru.bpc.cm.filters.EncashmentsFilter;
import ru.bpc.cm.items.encashments.AtmCurrStatItem;
import ru.bpc.cm.items.encashments.AtmEncRequestEncItem;
import ru.bpc.cm.items.encashments.AtmEncRequestFilter;
import ru.bpc.cm.items.encashments.AtmEncRequestItem;
import ru.bpc.cm.items.encashments.AtmEncSubmitFilter;
import ru.bpc.cm.items.encashments.AtmEncSubmitItem;
import ru.bpc.cm.items.encashments.AtmEncashmentItem;
import ru.bpc.cm.items.encashments.AtmEncashmentLogItem;
import ru.bpc.cm.items.encashments.AtmPeriodEncashmentItem;
import ru.bpc.cm.items.encashments.EncashmentCassItem;
import ru.bpc.cm.items.encashments.EncashmentDetailsItem;
import ru.bpc.cm.items.encashments.EncashmentItem;
import ru.bpc.cm.items.encashments.HourRemaining;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.enums.AtmGroupType;
import ru.bpc.cm.items.enums.AtmTypeByOperations;
import ru.bpc.cm.items.enums.EncashmentFilterMode;
import ru.bpc.cm.items.enums.EncashmentLogType;
import ru.bpc.cm.items.enums.EncashmentType;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;
import ru.bpc.cm.utils.TextUtil;
import ru.bpc.cm.utils.db.DbTextUtil;
import ru.bpc.cm.utils.db.JdbcUtils;
import ru.bpc.cm.utils.db.QueryConstructor;

public class AtmEncashmentController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<AtmEncashmentMapper> getMapperClass() {
		return AtmEncashmentMapper.class;
	}

	protected static List<AtmEncashmentItem> getAtmEncashmentList(ISessionHolder sessionHolder,
			EncashmentsFilter addFilter) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Connection connection = session.getConnection();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<AtmEncashmentItem> atmEncashmentList = null;
		Date currentDate = Calendar.getInstance().getTime();

		try {
			StringBuilder sql = new StringBuilder("SELECT " + "ENC_PLAN_ID, ai.ATM_ID, DATE_PREVIOUS_ENCASHMENT,"
					+ "INTERVAL_ENC_LAST_TO_FORTH, DATE_FORTHCOMING_ENCASHMENT, "
					+ "INTERVAL_ENC_FORTH_TO_FUTURE, DATE_FUTURE_ENCASHMENT, " + " IS_APPROVED, ENC_LOSTS_CURR_CODE, "
					+ "FORECAST_RESP_CODE,EMERGENCY_ENCASHMENT,COALESCE(ci.code_a3,'NOT DEFINED') as ENC_SUMM_CURR, "
					+ "ai.STATE,ai.CITY,ai.STREET, ai.STATE || ', ' || ai.CITY || ', ' || ai.STREET AS ADDRESS,ai.NAME as ATM_NAME,"
					+ "aep.ENC_LOSTS, aep.ENC_PRICE,aep.CASH_ADD_ENCASHMENT,aep.ENCASHMENT_TYPE,u.NAME as APPROVE_NAME, "
					+ "ai.TYPE as ATM_TYPE, ai.EXTERNAL_ATM_ID " + "FROM T_CM_ENC_PLAN aep "
					+ "join T_CM_ATM ai on (ai.ATM_ID = aep.ATM_ID) "
					+ "left outer join T_CM_CURR ci on(aep.ENC_LOSTS_CURR_CODE = ci.code_n3)"
					+ "left outer join T_CM_USER u on (u.ID = aep.APPROVE_ID)  " + "WHERE "
					+ " aep.atm_id in (select id from t_cm_temp_atm_list)");
			if (!addFilter.isShowApproved()) {
				sql.append("AND IS_APPROVED = 0 ");
			}
			switch (addFilter.getEncashmenFilterMode()) {
			case ANY:
				break;
			case EMERGENCY:
				sql.append(" AND (DATE_FORTHCOMING_ENCASHMENT IS NOT NULL AND EMERGENCY_ENCASHMENT > 0) ");
				break;
			case STANDARD:
				sql.append(" AND (DATE_FORTHCOMING_ENCASHMENT IS NOT NULL AND EMERGENCY_ENCASHMENT = 0) ");
				break;
			default:
				break;
			}

			QueryConstructor querConstr = new QueryConstructor();
			querConstr.setQueryBody(sql.toString(), true);
			
			querConstr.addElementIfNotNull("atm_id", "AND", "ai.ATM_ID", DbTextUtil.getOperationType(addFilter.getAtmID()),
					addFilter.getAtmID());
			querConstr.addElementIfNotNull("external_id", "AND", "ai.EXTERNAL_ATM_ID", DbTextUtil.getOperationType(addFilter.getExtAtmID()),
					addFilter.getExtAtmID());
			
			if(addFilter.getEncashmenFilterMode() != EncashmentFilterMode.NONE){
				querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", ">=",
						addFilter.getForthcomingEncDateFrom());
				querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", "<=",
						addFilter.getForthcomingEncDateTo());
			}
			
			if(TextUtil.isNotEmpty(addFilter.getNameAndAddress())){
				querConstr.addElement("atm_name", "AND ( ", "ai.NAME", DbTextUtil.getOperationType(addFilter.getNameAndAddress()),
						addFilter.getNameAndAddress(), false);
				querConstr.addElement("address", "OR", "ai.STATE || ', ' || ai.CITY || ', ' || ai.STREET",DbTextUtil.getOperationType(addFilter.getNameAndAddress()),
						addFilter.getNameAndAddress(), false);
				querConstr.addSimpleExpression("atm_name_addr", ")", " ");
			}
			
			if(addFilter.getTypeByOperations() > -1){
				querConstr.addElementInt("atm_type", "AND", "COALESCE(ai.TYPE,0)", "=", addFilter.getTypeByOperations());
			}
			
			querConstr.setQueryTail(" ORDER BY aep.DATE_FORTHCOMING_ENCASHMENT, aep.EMERGENCY_ENCASHMENT");
			pstmt = connection.prepareStatement(querConstr.getQuery());
			//pstmt.setTimestamp(1,new Timestamp(filter.getForthcomingEncDateFrom().getTime()));
			querConstr.updateQueryParameters(pstmt);
			rs = pstmt.executeQuery();

			atmEncashmentList = new ArrayList<AtmEncashmentItem>();
			while (rs.next()) {
				AtmEncashmentItem item = new AtmEncashmentItem();
				item.setEncPlanID(rs.getInt("ENC_PLAN_ID"));
				item.setAtmID(rs.getInt("ATM_ID"));
				item.setExtAtmId(rs.getString("EXTERNAL_ATM_ID"));
				item.setPreviousEncDate(JdbcUtils.getDate(rs
						.getDate("DATE_PREVIOUS_ENCASHMENT")));
				item.setIntervalLastToForth(rs
						.getInt("INTERVAL_ENC_LAST_TO_FORTH"));
				item.setForthcomingEncDate(rs
						.getTimestamp("DATE_FORTHCOMING_ENCASHMENT"));
				item.setIntervalForthToFuture(rs
						.getInt("INTERVAL_ENC_FORTH_TO_FUTURE"));
				item.setFutureEncDate(JdbcUtils.getDate(rs
						.getDate("DATE_FUTURE_ENCASHMENT")));
				item.setApprooved(rs.getBoolean("IS_APPROVED"));
				item.setEncSummCurrCode(rs.getInt("ENC_LOSTS_CURR_CODE"));
				item.setForecastResp(rs.getInt("FORECAST_RESP_CODE"));
				item.setEmergencyEncashment(rs
						.getBoolean("EMERGENCY_ENCASHMENT"));
				item.setEncSummCurr(rs.getString("ENC_SUMM_CURR"));
				item.setAddress(rs.getString("ADDRESS")); //CmUtils.getAtmFullAdrress(rs.getString("STATE"), rs.getString("CITY"),rs.getString("STREET"))
				item.setEncLosts(rs.getLong("ENC_LOSTS"));
				item.setEncPrice(rs.getLong("ENC_PRICE"));
				item.setCashAddEncashment(rs.getBoolean("CASH_ADD_ENCASHMENT"));
				item.setEditable(!item.isApprooved()
						&& item.getForthcomingEncDate().after(currentDate));
				item.setAtmName(rs.getString("ATM_NAME"));
				item.setEncType(CmUtils.getEnumValueById(EncashmentType.class, rs.getInt("ENCASHMENT_TYPE")));
				item.setApproveLogin(rs.getString("APPROVE_NAME"));
				item.setAtmType(CmUtils.getEnumValueById(AtmTypeByOperations.class, rs.getInt("ATM_TYPE")));
				
				atmEncashmentList.add(item);
			}

			return atmEncashmentList;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
			session.close();
		}
		return Collections.emptyList();
	}

	public static List<AtmEncSubmitItem> getAtmEncashmentList(ISessionHolder sessionHolder, AtmEncSubmitFilter filter) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmEncSubmitItem> atmEncashmentList = new ArrayList<AtmEncSubmitItem>();
		try {
			atmEncashmentList.addAll(session.getMapper(getMapperClass()).getAtmEncashmentList(filter,
					new Timestamp(filter.getForthcomingEncDateFrom().getTime()),
					new Timestamp(filter.getForthcomingEncDateTo().getTime()), filter.getPersonID()));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		for (AtmEncSubmitItem item : atmEncashmentList) {
			item.setEncashmentCassettes(getAtmEncashmentCassList(sessionHolder, item.getEncPlanID()));
			setEncashmentCurrencies(sessionHolder, item);
		}
		return atmEncashmentList;
	}

	public static List<AtmEncRequestEncItem> getEncReqEncashments(ISessionHolder sessionHolder, int encReqId)
			throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmEncRequestEncItem> atmEncashmentList = new ArrayList<AtmEncRequestEncItem>();
		try {
			atmEncashmentList.addAll(session.getMapper(getMapperClass()).getEncReqEncashments(encReqId));
		} finally {
			session.close();
		}
		for (AtmEncRequestEncItem item : atmEncashmentList) {
			item.setEncashmentCassettes(getAtmEncashmentCassList(sessionHolder, item.getEncPlanID()));
			setEncashmentCurrencies(sessionHolder, item);
		}
		return atmEncashmentList;
	}

	public static EncashmentItem getEncashmentById(ISessionHolder sessionHolder, int encPlanId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		EncashmentItem item = null;

		try {
			item = session.getMapper(getMapperClass()).getEncashmentById(encPlanId);
			if (item == null)
				item = new AtmEncashmentItem();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		setEncashmentCurrencies(sessionHolder, item);
		item.setEncashmentCassettes(
				AtmEncashmentController.getAtmEncashmentCassList(sessionHolder, item.getEncPlanID()));
		return item;
	}

	public static Map<Integer, List<Integer>> getCurrDenomMap(ISessionHolder sessionHolder,
			List<Integer> encPlanIDList) {
		Map<Integer, List<Integer>> currDenomMap = new HashMap<Integer, List<Integer>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Integer> currencies = new ArrayList<Integer>();
		try {
			List<Integer> denomList = session.getMapper(getMapperClass()).getCurrDenom(encPlanIDList);

			for (Integer denom : denomList)
				currencies.add(denom);

			for (Integer currency : currencies)
				currDenomMap.put(currency, CmCommonController.getCurrencyDenominations(sessionHolder, currency));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return currDenomMap;
	}

	protected static List<EncashmentCassItem> getAtmEncashmentCassList(ISessionHolder sessionHolder, int encPlanID) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<EncashmentCassItem> atmEncashmentCassList = new ArrayList<EncashmentCassItem>();
		try {
			List<EncashmentCassItem> cassList = session.getMapper(getMapperClass()).getAtmEncashmentCassList(encPlanID);

			int cassNumber = 1;
			for (EncashmentCassItem cass : cassList) {
				cass.setCassNumber(cassNumber);
				atmEncashmentCassList.add(cass);
				cassNumber++;
			}
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static AtmEncashmentItem getAtmEncashmentCurrencies(ISessionHolder sessionHolder,
			AtmEncashmentItem item) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			FullCodesDto fullCodeItem = session.getMapper(getMapperClass())
					.getAtmEncashmentCurrencies(item.getEncPlanID());

			if (fullCodeItem != null) {
				item.getAtmCurrencies().add(new Pair(fullCodeItem.getMainCurrCode(), fullCodeItem.getMainCurrCodeA3()));
				item.setMainCurrCode(Integer.parseInt(fullCodeItem.getMainCurrCode()));
				item.setMainCurrSumm(Long.parseLong(fullCodeItem.getMainCurrSumm()));
				item.setMainCurrAvgDemand(Long.parseLong(fullCodeItem.getMainCurrAvgDemand()));
				item.setMainCurrCodeA3(fullCodeItem.getMainCurrCodeA3());
				if (fullCodeItem.getSecCurrCode() != null && Integer.parseInt(fullCodeItem.getSecCurrCode()) > 0) {
					item.getAtmCurrencies()
							.add(new Pair(fullCodeItem.getSecCurrCode(), fullCodeItem.getSecCurrCodeA3()));
					item.setSecCurrCode(Integer.parseInt(fullCodeItem.getSecCurrCode()));
					item.setSecCurrSumm(Long.parseLong(fullCodeItem.getSecCurrSumm()));
					item.setSecCurrAvgDemand(Long.parseLong(fullCodeItem.getSecCurrAvgDemand()));
					item.setSecCurrCodeA3(fullCodeItem.getSecCurrCodeA3());
				}
				if (fullCodeItem.getSec2CurrCode() != null && Integer.parseInt(fullCodeItem.getSec2CurrCode()) > 0) {
					item.getAtmCurrencies()
							.add(new Pair(fullCodeItem.getSec2CurrCode(), fullCodeItem.getSec2CurrCodeA3()));
					item.setSec2CurrCode(Integer.parseInt(fullCodeItem.getSec2CurrCode()));
					item.setSec2CurrSumm(Long.parseLong(fullCodeItem.getSec2CurrSumm()));
					item.setSec2CurrAvgDemand(Long.parseLong(fullCodeItem.getSec2CurrAvgDemand()));
					item.setSec2CurrCodeA3(fullCodeItem.getSec2CurrCodeA3());
				}
				if (fullCodeItem.getSec3CurrCode() != null && Integer.parseInt(fullCodeItem.getSec3CurrCode()) > 0) {
					item.getAtmCurrencies()
							.add(new Pair(fullCodeItem.getSec3CurrCode(), fullCodeItem.getSec3CurrCodeA3()));
					item.setSec3CurrCode(Integer.parseInt(fullCodeItem.getSec3CurrCode()));
					item.setSec3CurrSumm(Long.parseLong(fullCodeItem.getSec3CurrSumm()));
					item.setSec3CurrAvgDemand(Long.parseLong(fullCodeItem.getSec3CurrAvgDemand()));
					item.setSec3CurrCodeA3(fullCodeItem.getSec3CurrCodeA3());
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return item;
	}

	private static void setEncashmentCurrencies(ISessionHolder sessionHolder, EncashmentItem item) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			FullCodesDto fullCodeItem = session.getMapper(getMapperClass())
					.getAtmEncashmentCurrencies(item.getEncPlanID());

			if (fullCodeItem != null) {
				item.getAtmCurrencies().add(new Pair(fullCodeItem.getMainCurrSumm(), fullCodeItem.getMainCurrCodeA3()));
				if (fullCodeItem.getSecCurrCode() != null && Integer.parseInt(fullCodeItem.getSecCurrCode()) > 0)
					item.getAtmCurrencies()
							.add(new Pair(fullCodeItem.getSecCurrSumm(), fullCodeItem.getSecCurrCodeA3()));

				if (fullCodeItem.getSec2CurrCode() != null && Integer.parseInt(fullCodeItem.getSec2CurrCode()) > 0)
					item.getAtmCurrencies()
							.add(new Pair(fullCodeItem.getSec2CurrSumm(), fullCodeItem.getSec2CurrCodeA3()));

				if (fullCodeItem.getSec3CurrCode() != null && Integer.parseInt(fullCodeItem.getSec3CurrCode()) > 0)
					item.getAtmCurrencies()
							.add(new Pair(fullCodeItem.getSec3CurrSumm(), fullCodeItem.getSec3CurrCodeA3()));
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	protected static AtmEncashmentItem getAtmEncashmentMaxAtmCount(ISessionHolder sessionHolder,
			AtmEncashmentItem item) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			ObjectPair<Integer, String> pair = session.getMapper(getMapperClass())
					.getAtmEncashmentMaxAtmCount(item.getAtmID(), AtmGroupType.FORECAST_REGION.getId());

			int maxAtmCountGroup = 0;
			if (pair != null) {
				item.setMaxAtmCountGroupLabel(pair.getValue());
				maxAtmCountGroup = pair.getKey();
			} else {
				item.setMaxAtmCountGroupLabel("NOT DEFINED");
			}

			item.setMaxAtmCount(Integer.parseInt(
					CmCommonController.getAtmAttribute(sessionHolder, item.getAtmID(), AtmAttribute.MAX_ATM_COUNT)));

			item.setAtmCount(session.getMapper(getMapperClass()).getAtmEncashmentMaxAtmCount_count(
					JdbcUtils.getSqlDate(item.getForthcomingEncDate()), maxAtmCountGroup));

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return item;
	}

	public static void approveSelectedEncahments(ISessionHolder sessionHolder, List<Integer> selectedEncashments,
			int personId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).approveSelectedEncahments(selectedEncashments, personId);

			for (Integer encPlanId : selectedEncashments)
				CmCommonController.insertEncashmentMessage(sessionHolder, encPlanId,
						EncashmentLogType.ENCASHMENT_APPROVE, EncashmentLogType.SYSTEM_MESSAGE, personId);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void confirmSelectedEncahments(ISessionHolder sessionHolder, List<Integer> selectedEncashments,
			int personId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).confirmSelectedEncahments(selectedEncashments, personId);

			for (Integer encPlanId : selectedEncashments)
				CmCommonController.insertEncashmentMessage(sessionHolder, encPlanId,
						EncashmentLogType.ENCASHMENT_CONFIRM, EncashmentLogType.SYSTEM_MESSAGE, personId);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}

	}

	public static void discardEncashment(ISessionHolder sessionHolder, int encPlanId, int personId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).discardEncashment(encPlanId);

			CmCommonController.insertEncashmentMessage(sessionHolder, encPlanId, EncashmentLogType.ENCASHMENT_DISCARD,
					EncashmentLogType.SYSTEM_MESSAGE, personId);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}

	}

	public static void discardEncashmentsDateChange(ISessionHolder sessionHolder, List<Integer> selectedEncashments,
			int personId, String dateStr) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).discardEncashmentsDateChange(selectedEncashments);

			for (Integer encPlanId : selectedEncashments)
				CmCommonController.insertEncashmentMessage(sessionHolder, encPlanId,
						EncashmentLogType.ENCASHMENT_DISCARD_DATE_CHANGE, EncashmentLogType.SYSTEM_MESSAGE, personId,
						dateStr);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static List<AtmEncashmentLogItem> getAtmEncashmentLogList(ISessionHolder sessionHolder, int encPlanId,
			int personId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmEncashmentLogItem> atmEncashmentLogList = new ArrayList<AtmEncashmentLogItem>();
		try {
			List<AtmEncashmentLogItem> items = session.getMapper(getMapperClass()).getAtmEncashmentLogList(encPlanId);

			for (AtmEncashmentLogItem item : items)
				item.setDeletable(item.getId().intValue() == personId);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return atmEncashmentLogList;
	}

	public static void updateReqIdForSelectedEncashmnets(ISessionHolder sessionHolder,
			List<Integer> selectedEncashments, int encReqId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).updateReqIdForSelectedEncashmnets(selectedEncashments, encReqId);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void updateReqDateForSelectedEncashmnets(ISessionHolder sessionHolder,
			List<Integer> selectedEncashments, Date reqDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).updateReqDateForSelectedEncashmnetsEncashmnets(selectedEncashments,
					new Timestamp(reqDate.getTime()));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static List<AtmEncRequestItem> getEncashmentRequests(ISessionHolder sessionHolder, int personId,
			AtmEncRequestFilter filter) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmEncRequestItem> atmEncashmentReqList = new ArrayList<AtmEncRequestItem>();
		try {
			atmEncashmentReqList.addAll(session.getMapper(getMapperClass()).getEncashmentRequests_withStartDate(personId,
					filter.isFetchAll() ? 1 : 0, new Timestamp(filter.getStartDate().getTime()),
					new Timestamp(filter.getEndDate().getTime()),
					CmUtils.getNVLValue(filter.getReqId(), Integer.valueOf(0))));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return atmEncashmentReqList;
	}

	public static List<AtmEncRequestItem> getEncashmentRequests(ISessionHolder sessionHolder, int personId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmEncRequestItem> atmEncashmentReqList = new ArrayList<AtmEncRequestItem>();
		try {
			atmEncashmentReqList.addAll(session.getMapper(getMapperClass()).getEncashmentRequests(personId,
					JdbcUtils.getSqlDate(new Date())));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return atmEncashmentReqList;
	}

	public static int generateEncashmentRequestId(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			return session.getMapper(getMapperClass()).generateEncashmentRequestId(session);
		} finally {
			session.close();
		}
	}

	public static void changeEncashmnetRequest(ISessionHolder sessionHolder, AtmEncRequestItem req, Operation action)
			throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());

		try {
			AtmEncashmentMapper mapper = session.getMapper(getMapperClass());
			switch (action) {

			case INSERT:
				mapper.changeEncashmnetRequest_insert(req.getId(), JdbcUtils.getSqlDate(req.getRequestDate()),
						req.getName(), req.getDescription(), req.getUserId());
				break;
			case DELETE:
				mapper.changeEncashmnetRequest_update2delete(req.getId());
				mapper.changeEncashmnetRequest_delete(req.getId());
				break;
			case UPDATE:
				mapper.changeEncashmnetRequest_update(req.getName(), req.getDescription(), req.getId());
				break;
			}
		} finally {
			session.close();
		}
	}

	protected static List<AtmPeriodEncashmentItem> getEncashmentsForPeriod(ISessionHolder sessionHolder, int atmId,
			Date endDate, Map<Integer, String> currCodeA3Map) {
		List<AtmPeriodEncashmentItem> res = new ArrayList<AtmPeriodEncashmentItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			res.addAll(session.getMapper(getMapperClass()).getEncashmentsForPeriod(atmId,
					new Timestamp(endDate.getTime())));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		for (AtmPeriodEncashmentItem item : res) {
			item.setAtmCurrencies(getEncashmentCurrsForPeriod(sessionHolder, item.getEncPeriodId()));
			item.setAtmCassettes(getEncashmentDenomsForPeriod(sessionHolder, item.getEncPeriodId()));
		}
		return res;
	}

	private static List<Pair> getEncashmentCurrsForPeriod(ISessionHolder sessionHolder, int encPeriodId) {
		List<Pair> res = new ArrayList<Pair>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			res.addAll(session.getMapper(getMapperClass()).getEncashmentCurrsForPeriod(encPeriodId));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}

		return res;
	}

	private static List<EncashmentCassItem> getEncashmentDenomsForPeriod(ISessionHolder sessionHolder,
			int encPeriodId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<EncashmentCassItem> atmEncashmentCassList = new ArrayList<EncashmentCassItem>();
		try {
			atmEncashmentCassList.addAll(session.getMapper(getMapperClass()).getEncashmentDenomsForPeriod(encPeriodId));
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<AtmCurrStatItem> getCurrenciesForPeriod(ISessionHolder sessionHolder, int atmId, Integer curr,
			Date endDate) {
		List<AtmCurrStatItem> res = new ArrayList<AtmCurrStatItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			res.addAll(session.getMapper(getMapperClass()).getCurrenciesForPeriod(atmId, curr,
					new Timestamp(endDate.getTime())));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}

	public static EncashmentDetailsItem getEncashmentDetails(ISessionHolder sessionHolder, int encPlanId)
			throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		EncashmentDetailsItem item = null;
		try {
			item = session.getMapper(getMapperClass()).getEncashmentDetails(encPlanId);

			if (item == null)
				item = new EncashmentDetailsItem();
		} finally {
			session.close();
		}
		return item;
	}

	public static void insertTempEncReport(ISessionHolder sessionHolder, List<HourRemaining> hours)
			throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			ORMUtils.createTemporaryTableIfNotExists(session, "t_cm_temp_enc_report");

			for (HourRemaining item : hours)
				session.getMapper(getMapperClass()).insertTempEncReport(item.getRemaining(), item.getCurrCodeA3(),
						new Timestamp(item.getStatDate().getTime()), item.isEndOfStatsDate());
			session.flushStatements();
		} finally {
			session.close();
		}
	}

}
