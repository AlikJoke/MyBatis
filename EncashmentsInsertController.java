package ru.bpc.cm.forecasting.controllers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.audit.event.AuditActionTypes;
import ru.bpc.audit.logger.AuditLogger;
import ru.bpc.audit.utils.event.EventBuilder;
import ru.bpc.cm.authorization.CmUserManagementController;
import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.config.utils.ORMUtils;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.encashments.AtmEncashmentController;
import ru.bpc.cm.forecasting.anyatm.EncashmentForecast;
import ru.bpc.cm.forecasting.anyatm.ForecastBuilder;
import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.Currency;
import ru.bpc.cm.forecasting.orm.EncashmentsInsertMapper;
import ru.bpc.cm.items.audit.EncashmentWrapper;
import ru.bpc.cm.items.enums.EncashmentLogType;
import ru.bpc.cm.items.enums.EncashmentType;
import ru.bpc.cm.items.enums.ForecastingMode;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.forecast.UserForecastFilter;
import ru.bpc.cm.items.forecast.nominal.NominalItem;
import ru.bpc.cm.items.routes.AtmRouteFilter;
import ru.bpc.cm.items.routes.RoutingException;
import ru.bpc.cm.management.User;
import ru.bpc.cm.routes.RoutingController;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class EncashmentsInsertController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<EncashmentsInsertMapper> getMapperClass() {
		return EncashmentsInsertMapper.class;
	}

	public static void insertForecastData(ISessionHolder sessionHolder, AnyAtmForecast item) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());
			if (item.getForthcomingEncDate() != null) {
				User user = CmUserManagementController.getSingleUser(session.getConnection(), item.getPersonID());
				String login = user == null ? CashManagementConstants.SYSTEM_LOGIN : user.getLogin();
				EncashmentWrapper objBefore = null;
				AuditActionTypes actionType = null;

				try {
					int encPlanID = item.getEncPlanID();
					String query = null;
					if (encPlanID == 0) {
						actionType = AuditActionTypes.ADD;
						query = "SELECT " + ORMUtils.getNextSequence(session, "SQ_CM_ENC_PLAN_ID") + " as SQ "
								+ ORMUtils.getFromDummyExpression(session);
						pstmt = session.getConnection().prepareStatement(query);
						rs = pstmt.executeQuery();
						rs.next();
						encPlanID = rs.getInt("SQ");
						JdbcUtils.close(rs);
						JdbcUtils.close(pstmt);

						mapper.insertForecastData(encPlanID, item.getAtmId(),
								JdbcUtils.getSqlDate(item.getLastEncDate()), item.getForthcomingEncInterval(),
								new Timestamp(item.getForthcomingEncDate().getTime()), item.getFutureEncInterval(),
								JdbcUtils.getSqlDate(item.getFutureEncDate()), 0, item.getLostsCurr(),
								item.getForecastResp(), item.isEmergencyEncashment(), item.getEncLosts(),
								item.getEncPrice(), false, item.getEncType().getId(), item.getLostsJointEcnashment(),
								item.getLostsSplitEcnashment(), item.getEncTypeByLosts().getId(),
								item.getEncPriceCashIn(), item.getEncPriceCashOut(), item.getEncPriceBothInOut());

						if (item.getPersonID() == 0)
							CmCommonController.insertEncashmentMessage(sessionHolder, encPlanID,
									EncashmentLogType.ENCASHMENT_CREATION, EncashmentLogType.SYSTEM_MESSAGE, -1);
						else
							CmCommonController.insertEncashmentMessage(sessionHolder, encPlanID,
									EncashmentLogType.ENCASHMENT_CREATION, EncashmentLogType.SYSTEM_MESSAGE,
									item.getPersonID());
					} else {
						actionType = AuditActionTypes.MODIFY;

						objBefore = new EncashmentWrapper(
								AtmEncashmentController.getEncashmentById(sessionHolder, encPlanID));
						mapper.updateForecastData(encPlanID, item.getAtmId(),
								JdbcUtils.getSqlDate(item.getLastEncDate()), item.getForthcomingEncInterval(),
								new Timestamp(item.getForthcomingEncDate().getTime()),
								JdbcUtils.getSqlDate(item.getFutureEncDate()), item.getFutureEncInterval(), 0,
								item.getLostsCurr(), item.getForecastResp(), item.isEmergencyEncashment(),
								item.getEncLosts(), item.getEncPrice(), false, item.getEncType().getId(),
								item.getLostsJointEcnashment(), item.getLostsSplitEcnashment(),
								item.getEncTypeByLosts().getId(), item.getEncPriceCashIn(), item.getEncPriceCashOut(),
								item.getEncPriceBothInOut());

						CmCommonController.insertEncashmentMessage(sessionHolder, encPlanID,
								EncashmentLogType.ENCASHMENT_CHANGE, EncashmentLogType.SYSTEM_MESSAGE,
								item.getPersonID());

						mapper.deleteEncPlanDenom(encPlanID);
						mapper.deleteEncPlanCurr(encPlanID);
					}
					if (item.getAtmCurrencies() != null) {
						for (Currency curr : item.getAtmCurrencies()) {
							mapper.insertEncPlanCurr(encPlanID, curr.getCurrCode(), curr.getPlanSummForCurr(),
									Math.round(curr.getAvgForecastOutDemand()));

							for (NominalItem nom : curr.getNominals())
								for (int i = 0; i < nom.getCassCount(); i++)
									mapper.insertEncPlanDenom(encPlanID, curr.getCurrCode(),
											nom.getCountInOneCassPlan(), nom.getDenom());

						}
					}
					EncashmentWrapper objAfter = new EncashmentWrapper(
							AtmEncashmentController.getEncashmentById(sessionHolder, encPlanID));

					AuditLogger.sendAsynchronously(EventBuilder.buildEventFromWrappedObjects(login, actionType,
							String.valueOf(encPlanID), objBefore, objAfter));
				} catch (Exception e) {
					logger.error("atmID = " + item.getAtmId(), e);
				}
			}
		} catch (Exception e) {
			logger.error("", e);

		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
			session.close();
		}
	}

	public static void insertForecastDataForParticularDate(ISessionHolder sessionHolder, AnyAtmForecast item,
			int encPlanID) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());
			ORMUtils.createTemporaryTableIfNotExists(session, "T_CM_TEMP_ENC_PLAN");
			ORMUtils.createTemporaryTableIfNotExists(session, "T_CM_TEMP_ENC_PLAN_CURR");
			ORMUtils.createTemporaryTableIfNotExists(session, "T_CM_TEMP_ENC_PLAN_DENOM");
			if (item.getForthcomingEncDate() != null) {
				try {
					mapper.insertForecastDataForParticularDate_toTempEncPlan(encPlanID, item.getAtmId(),
							new Timestamp(item.getForthcomingEncDate().getTime()));

					if (item.getAtmCurrencies() != null) {
						for (Currency curr : item.getAtmCurrencies()) {
							mapper.insertForecastDataForParticularDate_toTempEncPlanCurr(encPlanID, curr.getCurrCode(),
									curr.getPlanSummForCurr());

							for (NominalItem nom : curr.getNominals())
								for (int i = 0; i < nom.getCassCount(); i++)
									mapper.insertForecastDataForParticularDate_toTempEncPlanDenom(encPlanID,
											curr.getCurrCode(), nom.getCountInOneCassPlan(), nom.getDenom());
						}
					}
				} catch (Exception e) {
					logger.error("atmID = " + item.getAtmId(), e);
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void deleteEncashments(ISessionHolder sessionHolder, List<Integer> encList) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());

			if (!encList.isEmpty()) {
				mapper.deleteEncashments_deleteTempTable(encList, "T_CM_ENC_PLAN_DENOM");
				mapper.deleteEncashments_deleteTempTable(encList, "T_CM_ENC_PLAN_CURR");
				mapper.deleteEncashments_deleteTempTable(encList, "T_CM_ENC_PLAN_LOG");
				mapper.deleteEncashments_deleteTempTable(encList, "T_CM_ENC_PLAN");
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	/*
	 * public static List<ObjectPair<Integer,List<EncashmentType>>>
	 * checkExistingEncashments( Connection con, List<IFilterItem<Integer>>
	 * atmList) {
	 * 
	 * List<ObjectPair<Integer,List<EncashmentType>>> newAtmList = new
	 * ArrayList<ObjectPair<Integer,List<EncashmentType>>>();
	 * List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
	 * List<Integer> deleteEncList = null; ResultSet rs = null;
	 * PreparedStatement pstmt = null; Date dateForCheck = new Date(); try { for
	 * (IFilterItem<Integer> item : atmList) { int atmId = item.getValue();
	 * deleteEncList = new ArrayList<Integer>(); approvedEncList = new
	 * ArrayList<EncashmentType>();
	 * 
	 * String query = "SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN " +
	 * "WHERE ATM_ID = ? " + "AND DATE_FORTHCOMING_ENCASHMENT >= ? " +
	 * "AND IS_APPROVED = 0"; pstmt = con.prepareStatement(query);
	 * pstmt.setInt(1, atmId); pstmt.setDate(2,
	 * JdbcUtils.getSqlDate(dateForCheck)); rs = pstmt.executeQuery(); while
	 * (rs.next()) { deleteEncList.add(rs.getInt("ENC_PLAN_ID")); }
	 * JdbcUtils.close(pstmt); JdbcUtils.close(rs);
	 * 
	 * String deleteEncListClause = CmUtils.getIdListInClause( deleteEncList,
	 * "ENC_PLAN_ID");
	 * 
	 * StringBuffer sb = new StringBuffer( "DELETE FROM T_CM_ENC_PLAN_DENOM " +
	 * "WHERE 1=1 "); sb.append(deleteEncListClause); if
	 * (!deleteEncList.isEmpty()){ pstmt = con.prepareStatement(sb.toString());
	 * pstmt.executeUpdate(); JdbcUtils.close(pstmt); }
	 * 
	 * sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_CURR " + "WHERE 1=1 ");
	 * sb.append(deleteEncListClause); if (!deleteEncList.isEmpty()){ pstmt =
	 * con.prepareStatement(sb.toString()); pstmt.executeUpdate();
	 * JdbcUtils.close(pstmt); }
	 * 
	 * sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN " + "WHERE 1 = 1 ");
	 * sb.append(deleteEncListClause); if (!deleteEncList.isEmpty()){ pstmt =
	 * con.prepareStatement(sb.toString()); pstmt.executeUpdate();
	 * JdbcUtils.close(pstmt); }
	 * 
	 * query = "SELECT ep.ENCASHMENT_TYPE " + "FROM V_CM_ENC_FORTHCOMING ep " +
	 * "WHERE ep.ATM_ID = ? " + "AND ep.DATE_FORTHCOMING_ENCASHMENT >= ? ";
	 * pstmt = con.prepareStatement(query); pstmt.setString(1, atmId);
	 * pstmt.setDate(2, JdbcUtils.getSqlDate(dateForCheck)); rs =
	 * pstmt.executeQuery(); while (rs.next()) {
	 * approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class,
	 * rs.getInt("ENCASHMENT_TYPE"))); }
	 * 
	 * JdbcUtils.close(pstmt); JdbcUtils.close(rs);
	 * 
	 * newAtmList.add(new ObjectPair<String, List<EncashmentType>>(atmId,
	 * approvedEncList)); } atmList.clear(); } catch (Exception e) {
	 * ForecastCommonController.logger.error("", e); } finally {
	 * JdbcUtils.close(pstmt); JdbcUtils.close(rs); } return newAtmList; }
	 */

	public static List<ObjectPair<Integer, List<EncashmentType>>> checkExistingEncashments(ISessionHolder sessionHolder,
			List<IFilterItem<Integer>> atmList) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<ObjectPair<Integer, List<EncashmentType>>> newAtmList = new ArrayList<ObjectPair<Integer, List<EncashmentType>>>();
		List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
		List<Integer> updateEncList = null;
		Date dateForCheck = new Date();
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());
			for (IFilterItem<Integer> item : atmList) {
				int atmId = item.getValue();
				updateEncList = new ArrayList<Integer>();
				approvedEncList = new ArrayList<EncashmentType>();

				updateEncList.addAll(mapper.getEncPlanId(atmId, JdbcUtils.getSqlDate(dateForCheck)));

				if (!updateEncList.isEmpty()) {
					mapper.deleteEncashments_deleteTempTable(updateEncList, "T_CM_ENC_PLAN_DENOM");
					mapper.deleteEncashments_deleteTempTable(updateEncList, "T_CM_ENC_PLAN_CURR");
				}

				List<Integer> encTypes = mapper.getEncashmentType(atmId, JdbcUtils.getSqlDate(dateForCheck));

				for (Integer encType : encTypes)
					approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class, encType));

				newAtmList.add(new ObjectPair<Integer, List<EncashmentType>>(atmId, approvedEncList));
			}
			atmList.clear();
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			session.close();
		}
		return newAtmList;
	}

	public static List<ObjectPair<Integer, List<EncashmentType>>> checkExistingEncashmentsForSimpleList(
			ISessionHolder sessionHolder, List<Integer> atmList) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<ObjectPair<Integer, List<EncashmentType>>> newAtmList = new ArrayList<ObjectPair<Integer, List<EncashmentType>>>();
		List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
		List<Integer> updateEncList = null;
		Date dateForCheck = new Date();
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());
			for (Integer item : atmList) {
				int atmId = item;
				updateEncList = new ArrayList<Integer>();
				approvedEncList = new ArrayList<EncashmentType>();

				updateEncList.addAll(mapper.getEncPlanId(atmId, JdbcUtils.getSqlDate(dateForCheck)));

				if (!updateEncList.isEmpty()) {
					mapper.deleteEncashments_deleteTempTable(updateEncList, "T_CM_ENC_PLAN_DENOM");
					mapper.deleteEncashments_deleteTempTable(updateEncList, "T_CM_ENC_PLAN_CURR");
				}

				List<Integer> encTypes = mapper.getEncashmentType(atmId, JdbcUtils.getSqlDate(dateForCheck));

				for (Integer encType : encTypes)
					approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class, encType));

				newAtmList.add(new ObjectPair<Integer, List<EncashmentType>>(atmId, approvedEncList));
			}
			// atmList.clear();
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			session.close();
		}
		return newAtmList;
	}

	public static Integer getExistingPlanId(ISessionHolder sessionHolder, int atmID) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Date dateForCheck = new Date();
		try {
			Integer planId = session.getMapper(getMapperClass()).getExistingPlanId(atmID,
					JdbcUtils.getSqlDate(dateForCheck), ORMUtils.getLimitToFirstRowExpression(session));

			if (planId != null)
				return planId;
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			session.close();
		}
		return 0;
	}

	public static void ensureRouteConsistencyForEnc(ISessionHolder sessionHolder, AnyAtmForecast forecast,
			EncashmentType missingEncType, boolean useMainCurr) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());
			ObjectPair<Integer, java.sql.Date> pair = mapper
					.ensureRouteConsistencyForEnc_route(forecast.getEncPlanID());

			if (pair != null) {
				if (pair.getKey() > 1) {
					// emulating user-defined encashment
					UserForecastFilter filter = new UserForecastFilter();
					filter.setAtmID(forecast.getAtmId());
					filter.setEncPlanID(forecast.getEncPlanID());
					filter.setForthcomingEncDate(JdbcUtils.getDate(pair.getValue()));
					filter.setNewDate(true);
					try {
						forecast = EncashmentForecast.makeForecastForAtm(sessionHolder, session.getConnection(),
								forecast.getAtmId(), forecast.getStartDate(), filter, ForecastingMode.PLAN,
								missingEncType, false, useMainCurr);
					} catch (ForecastException e) {
						logger.error("atmID = " + forecast.getAtmId(), e);
						forecast = ForecastBuilder.getBlankAtmForecast();
						forecast.setAtmId(forecast.getAtmId());
						forecast.setForecastResp(e.getCode());
						forecast.setErrorForthcomingEncDate(filter.getForthcomingEncDate());
					} catch (Exception e) {
						logger.error("", e);
					}
				} else {
					// removing record from route points if date has changed
					if (!DateUtils.truncate(forecast.getForthcomingEncDate(), Calendar.DATE)
							.equals(DateUtils.truncate(JdbcUtils.getDate(pair.getValue()), Calendar.DATE)))
						mapper.ensureRouteConsistencyForEnc_deleteRoutePoint(forecast.getEncPlanID());
				}
			}
		} catch (Exception e) {
			ForecastCommonController.logger.error("Error while checking routes consistency", e);
		} finally {
			session.close();
		}
	}

	public static void updateRoute(ISessionHolder sessionHolder, int routeId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		AtmRouteFilter filter = new AtmRouteFilter();
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());
			ObjectPair<Integer, java.sql.Date> pair = mapper.getRouteById(routeId);

			if (pair != null) {
				filter.setRegion(pair.getKey());
				filter.setDateStart(JdbcUtils.getDate(pair.getValue()));
				try {
					RoutingController.recalculateRoute(sessionHolder, session.getConnection(), filter, routeId);
				} catch (RoutingException e) {
					ForecastCommonController.logger.error("Error recalculating route " + routeId, e);
				}
			}
		} catch (Exception e1) {
			ForecastCommonController.logger.error("Error updating route " + routeId, e1);
		} finally {
			session.close();
		}
	}

	public static int getRouteIdForEnc(ISessionHolder sessionHolder, AnyAtmForecast forecast) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int routeID = 0;
		try {
			Integer routeIdEnc = session.getMapper(getMapperClass()).getRouteIdForEnc(forecast.getEncPlanID());

			if (routeIdEnc != null)
				routeID = routeIdEnc;
		} catch (Exception e1) {
			ForecastCommonController.logger.error("Error getting route id for enc", e1);
		} finally {
			session.close();
		}
		return routeID;
	}

	public static List<EncashmentType> checkExistingEncashments(ISessionHolder sessionHolder,
			UserForecastFilter filter) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Date dateForCheck = new Date();
		List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
		try {
			EncashmentsInsertMapper mapper = session.getMapper(getMapperClass());
			int atmId = filter.getAtmID();
			List<Integer> deleteEncList = new ArrayList<Integer>();
			deleteEncList.addAll(mapper.getEncPlanId(atmId, JdbcUtils.getSqlDate(dateForCheck)));
			deleteEncList.remove(Integer.valueOf(filter.getEncPlanID()));

			if (!deleteEncList.isEmpty()) {
				mapper.deleteEncashments_deleteTempTable(deleteEncList, "T_CM_ENC_PLAN_DENOM");
				mapper.deleteEncashments_deleteTempTable(deleteEncList, "T_CM_ENC_PLAN_CURR");
			}

			deleteEncList.remove(Integer.valueOf(filter.getEncPlanID()));
			if (!deleteEncList.isEmpty()) {
				mapper.deleteEncashments_deleteTempTable(deleteEncList, "T_CM_ENC_PLAN_LOG");
				mapper.deleteEncashments_deleteTempTable(deleteEncList, "T_CM_ENC_PLAN");
			}

			List<Integer> encTypes = mapper.checkExistingEncashments_getEncType(atmId,
					JdbcUtils.getSqlDate(dateForCheck), EncashmentType.CASH_IN.getId(), EncashmentType.CASH_OUT.getId(),
					EncashmentType.CASH_IN_AND_CASH_OUT.getId());

			for (Integer type : encTypes)
				approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class, type));
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			session.close();
		}
		return approvedEncList;
	}
}
