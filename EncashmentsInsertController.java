package ru.bpc.cm.forecasting.controllers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.audit.event.AuditActionTypes;
import ru.bpc.audit.logger.AuditLogger;
import ru.bpc.audit.utils.event.EventBuilder;
import ru.bpc.cm.authorization.CmUserManagementController;
import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.encashments.AtmEncashmentController;
import ru.bpc.cm.forecasting.anyatm.EncashmentForecast;
import ru.bpc.cm.forecasting.anyatm.ForecastBuilder;
import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.Currency;
import ru.bpc.cm.items.audit.EncashmentWrapper;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.enums.AtmTypeByOperations;
import ru.bpc.cm.items.enums.EncashmentLogType;
import ru.bpc.cm.items.enums.EncashmentType;
import ru.bpc.cm.items.enums.ForecastingMode;
import ru.bpc.cm.items.forecast.ForecastCurrencyItem;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.forecast.ForecastItem;
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
	
	private static final Logger logger = LoggerFactory
			.getLogger("CASH_MANAGEMENT");

	public static void insertForecastData(Connection connection, AnyAtmForecast item) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			if (item.getForthcomingEncDate() != null) {
				User user = CmUserManagementController.getSingleUser(connection, item.getPersonID());
				String login = user == null ? CashManagementConstants.SYSTEM_LOGIN : user.getLogin();
				EncashmentWrapper objBefore = null;
				AuditActionTypes actionType = null;
				
				try {
					int encPlanID = item.getEncPlanID();
					String query = null;
					if (encPlanID == 0) {
						actionType = AuditActionTypes.ADD;
							// by connection	
						query = "SELECT "+JdbcUtils.getNextSequence(connection, "SQ_CM_ENC_PLAN_ID")+" as SQ "+JdbcUtils.getFromDummyExpression(connection);
						pstmt = connection.prepareStatement(query);
						rs = pstmt.executeQuery();
						rs.next();
						encPlanID = rs.getInt("SQ");
						JdbcUtils.close(rs);
						JdbcUtils.close(pstmt);

						query = "Insert into T_CM_ENC_PLAN "
								+ " (ENC_PLAN_ID, ATM_ID, DATE_PREVIOUS_ENCASHMENT, INTERVAL_ENC_LAST_TO_FORTH, DATE_FORTHCOMING_ENCASHMENT,  "
								+ " INTERVAL_ENC_FORTH_TO_FUTURE, DATE_FUTURE_ENCASHMENT, "
								+ "  IS_APPROVED, ENC_LOSTS_CURR_CODE, FORECAST_RESP_CODE, "
								+ "EMERGENCY_ENCASHMENT, ENC_LOSTS, ENC_PRICE, CASH_ADD_ENCASHMENT, ENCASHMENT_TYPE," 
								+ "ENC_LOSTS_JOINT, ENC_LOSTS_SPLIT, ENCASHMENT_TYPE_BY_LOSTS, "
								+ "ENC_PRICE_CASH_IN , ENC_PRICE_CASH_OUT, ENC_PRICE_BOTH_IN_OUT) "
								+ " VALUES "
								+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
						pstmt = connection.prepareStatement(query);
						pstmt.setInt(1, encPlanID);
						pstmt.setInt(2, item.getAtmId());
						pstmt.setDate(3,
								JdbcUtils.getSqlDate(item.getLastEncDate()));
						pstmt.setInt(4, item.getForthcomingEncInterval());
						// pstmt.setTimestamp(5, new
						// Timestamp(item.getForthcomingEncDate().getTime()));
						pstmt.setTimestamp(5, new Timestamp(item
								.getForthcomingEncDate().getTime()));
						pstmt.setInt(6, item.getFutureEncInterval());
						// pstmt.setTimestamp(7, new
						// Timestamp(item.getFutureEncDate().getTime()));
						pstmt.setDate(7,
								JdbcUtils.getSqlDate(item.getFutureEncDate()));
						pstmt.setInt(8, 0);
						pstmt.setInt(9, item.getLostsCurr());
						pstmt.setInt(10, item.getForecastResp());
						pstmt.setBoolean(11, item.isEmergencyEncashment());
						pstmt.setDouble(12, item.getEncLosts());
						pstmt.setDouble(13, item.getEncPrice());
						pstmt.setBoolean(14, false);
						pstmt.setInt(15, item.getEncType().getId());
						pstmt.setDouble(16, item.getLostsJointEcnashment());
						pstmt.setDouble(17, item.getLostsSplitEcnashment());
						pstmt.setInt(18, item.getEncTypeByLosts().getId());
						pstmt.setDouble(19, item.getEncPriceCashIn());
						pstmt.setDouble(20, item.getEncPriceCashOut());
						pstmt.setDouble(21, item.getEncPriceBothInOut());

						pstmt.executeUpdate();
						JdbcUtils.close(pstmt);

						if(item.getPersonID() == 0){
							CmCommonController.insertEncashmentMessage(connection, encPlanID, 
									EncashmentLogType.ENCASHMENT_CREATION, EncashmentLogType.SYSTEM_MESSAGE , -1);

						} else {
							CmCommonController.insertEncashmentMessage(connection, encPlanID, 
									EncashmentLogType.ENCASHMENT_CREATION, EncashmentLogType.SYSTEM_MESSAGE, item.getPersonID());
						}
						
						
					} else {
						actionType = AuditActionTypes.MODIFY;
						
						objBefore = new EncashmentWrapper(
								AtmEncashmentController.getEncashmentById(connection, encPlanID));
						
						query =
					        "UPDATE T_CM_ENC_PLAN "+
					        "SET ATM_ID = ?, " +
				                "DATE_PREVIOUS_ENCASHMENT = ?, " +
				                "INTERVAL_ENC_LAST_TO_FORTH = ?, " +
				                "DATE_FORTHCOMING_ENCASHMENT = ?, " +
				                "INTERVAL_ENC_FORTH_TO_FUTURE = ?, " +
				                "DATE_FUTURE_ENCASHMENT = ? , " +
				                "IS_APPROVED = ?, " +
				                "ENC_LOSTS_CURR_CODE = ?, " +
				                "FORECAST_RESP_CODE = ?, " +
				                "EMERGENCY_ENCASHMENT = ?, " +
				                "ENC_LOSTS = ? ," +
				                "ENC_PRICE = ? , " +
				                "CASH_ADD_ENCASHMENT = ?, " +
				                "ENCASHMENT_TYPE = ?, " +
				                "ENC_LOSTS_JOINT = ?, " +
				                "ENC_LOSTS_SPLIT = ?, " +
				                "ENCASHMENT_TYPE_BY_LOSTS = ?, " +
				                "ENC_PRICE_CASH_IN = ?, "+
				                "ENC_PRICE_CASH_OUT = ?, "+
				                "ENC_PRICE_BOTH_IN_OUT = ? "+
				           "WHERE ENC_PLAN_ID = ? ";

						pstmt = connection.prepareStatement(query);
						pstmt.setInt(1, item.getAtmId());
						pstmt.setDate(2,
								JdbcUtils.getSqlDate(item.getLastEncDate()));
						pstmt.setInt(3, item.getForthcomingEncInterval());
						// pstmt.setTimestamp(5, new
						// Timestamp(item.getForthcomingEncDate().getTime()));
						pstmt.setTimestamp(4, new Timestamp(item
								.getForthcomingEncDate().getTime()));
						pstmt.setInt(5, item.getFutureEncInterval());
						// pstmt.setTimestamp(7, new
						// Timestamp(item.getFutureEncDate().getTime()));
						pstmt.setDate(6,
								JdbcUtils.getSqlDate(item.getFutureEncDate()));
						pstmt.setInt(7, 0);
						pstmt.setInt(8, item.getLostsCurr());
						pstmt.setInt(9, item.getForecastResp());
						pstmt.setBoolean(10, item.isEmergencyEncashment());

						pstmt.setDouble(11, item.getEncLosts());
						pstmt.setDouble(12, item.getEncPrice());
						pstmt.setBoolean(13, false);
						pstmt.setInt(14, item.getEncType().getId());
						
						pstmt.setDouble(15, item.getLostsJointEcnashment());
						pstmt.setDouble(16, item.getLostsSplitEcnashment());
						pstmt.setInt(17, item.getEncTypeByLosts().getId());
						
						pstmt.setDouble(18, item.getEncPriceCashIn());
						pstmt.setDouble(19, item.getEncPriceCashOut());
						pstmt.setDouble(20, item.getEncPriceBothInOut());
						
						pstmt.setInt(21, encPlanID);

						pstmt.executeUpdate();
						pstmt.close();

						CmCommonController.insertEncashmentMessage(connection, encPlanID, 
								EncashmentLogType.ENCASHMENT_CHANGE, EncashmentLogType.SYSTEM_MESSAGE, item.getPersonID());
						
						query = "DELETE FROM T_CM_ENC_PLAN_DENOM " +
			                    "WHERE ENC_PLAN_ID = ? ";
						pstmt = connection.prepareStatement(query);
						pstmt.setInt(1, encPlanID);
						pstmt.executeUpdate();
			
						JdbcUtils.close(pstmt);
						
						query = "DELETE FROM T_CM_ENC_PLAN_CURR " +
						        "WHERE ENC_PLAN_ID = ? ";
						pstmt = connection.prepareStatement(query);
						pstmt.setInt(1, encPlanID);
						pstmt.executeUpdate();
						
						JdbcUtils.close(pstmt);

					}
					if (item.getAtmCurrencies() != null) {
						for (Currency curr : item
								.getAtmCurrencies()) {
							query = "Insert into T_CM_ENC_PLAN_CURR "
									+ " (ENC_PLAN_ID, CURR_CODE, CURR_SUMM, CURR_AVG_DEMAND) "
									+ " VALUES " + " (?, ?, ?, ?)";
							pstmt = connection.prepareStatement(query);
							pstmt.setInt(1, encPlanID);
							pstmt.setInt(2, curr.getCurrCode());
							pstmt.setLong(3, curr.getPlanSummForCurr());
							pstmt.setLong(4, Math.round(curr.getAvgForecastOutDemand()));

							pstmt.executeUpdate();
							pstmt.close();

							for (NominalItem nom : curr.getNominals()) {
								for (int i = 0; i < nom.getCassCount(); i++) {
									query = "Insert into T_CM_ENC_PLAN_DENOM "
											+ " (ENC_PLAN_ID, DENOM_CURR, DENOM_COUNT, DENOM_VALUE) "
											+ " VALUES " + " (?, ?, ?, ?)";
									pstmt = connection.prepareStatement(query);
									pstmt.setInt(1, encPlanID);
									pstmt.setInt(2, curr.getCurrCode());
									pstmt.setInt(3, nom.getCountInOneCassPlan());
									pstmt.setInt(4, nom.getDenom());

									pstmt.executeUpdate();
									pstmt.close();
								}
							}
						}
					}
					EncashmentWrapper objAfter = new EncashmentWrapper(
							AtmEncashmentController.getEncashmentById(connection, encPlanID));
					
					
					AuditLogger.sendAsynchronously(
							EventBuilder.buildEventFromWrappedObjects(login, actionType, 
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
		}
	}
	
	public static void insertForecastDataForParticularDate(Connection con, AnyAtmForecast item, int encPlanID) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			JdbcUtils.createTemporaryTableIfNotExists(con, "T_CM_TEMP_ENC_PLAN");
			JdbcUtils.createTemporaryTableIfNotExists(con, "T_CM_TEMP_ENC_PLAN_CURR");
			JdbcUtils.createTemporaryTableIfNotExists(con, "T_CM_TEMP_ENC_PLAN_DENOM");
			if (item.getForthcomingEncDate() != null) {
				try {
					String query = "Insert into T_CM_TEMP_ENC_PLAN "
							+ " (ENC_PLAN_ID, ATM_ID, DATE_FORTHCOMING_ENCASHMENT) "
							+ " VALUES "
							+ " (?, ?, ?)";
					pstmt = con.prepareStatement(query);
					pstmt.setInt(1, encPlanID);
					pstmt.setInt(2, item.getAtmId());
					pstmt.setTimestamp(3, new Timestamp(item
							.getForthcomingEncDate().getTime()));

					pstmt.executeUpdate();
					pstmt.close();
					
					if (item.getAtmCurrencies() != null) {
						for (Currency curr : item
								.getAtmCurrencies()) {
							query = "Insert into T_CM_TEMP_ENC_PLAN_CURR "
									+ " (ENC_PLAN_ID, CURR_CODE, CURR_SUMM) "
									+ " VALUES " + " (?, ?, ?)";
							pstmt = con.prepareStatement(query);
							pstmt.setInt(1, encPlanID);
							pstmt.setInt(2, curr.getCurrCode());
							pstmt.setLong(3, curr.getPlanSummForCurr());
							
							pstmt.executeUpdate();
							pstmt.close();

							for (NominalItem nom : curr.getNominals()) {
								for (int i = 0; i < nom.getCassCount(); i++) {
									query = "Insert into T_CM_TEMP_ENC_PLAN_DENOM "
											+ " (ENC_PLAN_ID, DENOM_CURR, DENOM_COUNT, DENOM_VALUE) "
											+ " VALUES " + " (?, ?, ?, ?)";
									pstmt = con.prepareStatement(query);
									pstmt.setInt(1, encPlanID);
									pstmt.setInt(2, curr.getCurrCode());
									pstmt.setInt(3, nom.getCountInOneCassPlan());
									pstmt.setInt(4, nom.getDenom());

									pstmt.executeUpdate();
									pstmt.close();
								}
							}
						}
					}
				} catch (Exception e) {
					logger.error("atmID = " + item.getAtmId(), e);
				}

			}
		} catch (Exception e) {
			logger.error("", e);

		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
	}

	public static void deleteEncashments(Connection con, List<Integer> encList) {
	
		PreparedStatement pstmt = null;
		try {
	
			String deleteEncListClause = CmUtils.getIdListInClause(encList,
					"ENC_PLAN_ID");
	
			StringBuffer sb = new StringBuffer(
					"DELETE FROM T_CM_ENC_PLAN_DENOM " + "WHERE 1=1 ");
			sb.append(deleteEncListClause);
			if (!encList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
			
	
			sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_CURR "
					+ "WHERE 1=1 ");
			sb.append(deleteEncListClause);
			if (!encList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
	
			sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_LOG "
					+ "WHERE 1=1 ");
			sb.append(deleteEncListClause);
			if (!encList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
	
			sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN " + "WHERE 1 = 1 ");
			sb.append(deleteEncListClause);
			if (!encList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
	
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
		}
	}

	/*public static List<ObjectPair<Integer,List<EncashmentType>>> checkExistingEncashments(
			Connection con, List<IFilterItem<Integer>> atmList) {
	
		List<ObjectPair<Integer,List<EncashmentType>>> newAtmList = new ArrayList<ObjectPair<Integer,List<EncashmentType>>>();
		List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
		List<Integer> deleteEncList = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Date dateForCheck = new Date();
		try {
			for (IFilterItem<Integer> item : atmList) {
				int atmId = item.getValue();
				deleteEncList = new ArrayList<Integer>();
				approvedEncList = new ArrayList<EncashmentType>();
				
				String query = "SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN "
						+ "WHERE ATM_ID = ? "
						+ "AND DATE_FORTHCOMING_ENCASHMENT >= ? "
						+ "AND IS_APPROVED = 0";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, atmId);
				pstmt.setDate(2,
						JdbcUtils.getSqlDate(dateForCheck));
				rs = pstmt.executeQuery();
				while (rs.next()) {
					deleteEncList.add(rs.getInt("ENC_PLAN_ID"));
				}
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
	
				String deleteEncListClause = CmUtils.getIdListInClause(
						deleteEncList, "ENC_PLAN_ID");
	
				StringBuffer sb = new StringBuffer(
						"DELETE FROM T_CM_ENC_PLAN_DENOM " + "WHERE 1=1 ");
				sb.append(deleteEncListClause);
				if (!deleteEncList.isEmpty()){
					pstmt = con.prepareStatement(sb.toString());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
	
				sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_CURR "
						+ "WHERE 1=1 ");
				sb.append(deleteEncListClause);
				if (!deleteEncList.isEmpty()){
					pstmt = con.prepareStatement(sb.toString());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
	
				sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN "
						+ "WHERE 1 = 1 ");
				sb.append(deleteEncListClause);
				if (!deleteEncList.isEmpty()){
					pstmt = con.prepareStatement(sb.toString());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
                }
	
				query = "SELECT ep.ENCASHMENT_TYPE "
						+ "FROM V_CM_ENC_FORTHCOMING ep "
						+ "WHERE ep.ATM_ID = ? "
						+ "AND ep.DATE_FORTHCOMING_ENCASHMENT >= ? ";
				pstmt = con.prepareStatement(query);
				pstmt.setString(1, atmId);
				pstmt.setDate(2,
						JdbcUtils.getSqlDate(dateForCheck));
				rs = pstmt.executeQuery();
				while (rs.next()) {
					approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class, rs.getInt("ENCASHMENT_TYPE")));
				}
	
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
				
				newAtmList.add(new ObjectPair<String, List<EncashmentType>>(atmId, approvedEncList));
			}
			atmList.clear();
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return newAtmList;
	}*/
	
	public static List<ObjectPair<Integer,List<EncashmentType>>> checkExistingEncashments(
			Connection con, List<IFilterItem<Integer>> atmList) {
	
		List<ObjectPair<Integer,List<EncashmentType>>> newAtmList = new ArrayList<ObjectPair<Integer,List<EncashmentType>>>();
		List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
		List<Integer> updateEncList = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Date dateForCheck = new Date();
		try {
			for (IFilterItem<Integer> item : atmList) {
				int atmId = item.getValue();
				updateEncList = new ArrayList<Integer>();
				approvedEncList = new ArrayList<EncashmentType>();
				
				String query = "SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN "
						+ "WHERE ATM_ID = ? "
						+ "AND DATE_FORTHCOMING_ENCASHMENT >= ? "
						+ "AND IS_APPROVED = 0";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, atmId);
				pstmt.setDate(2,
						JdbcUtils.getSqlDate(dateForCheck));
				rs = pstmt.executeQuery();
				while (rs.next()) {
					updateEncList.add(rs.getInt("ENC_PLAN_ID"));
				}
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
	
				String deleteEncListClause = CmUtils.getIdListInClause(
						updateEncList, "ENC_PLAN_ID");
	
				StringBuffer sb = new StringBuffer(
						"DELETE FROM T_CM_ENC_PLAN_DENOM " + "WHERE 1=1 ");
				sb.append(deleteEncListClause);
				if (!updateEncList.isEmpty()){
					pstmt = con.prepareStatement(sb.toString());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
	
				sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_CURR "
						+ "WHERE 1=1 ");
				sb.append(deleteEncListClause);
				if (!updateEncList.isEmpty()){
					pstmt = con.prepareStatement(sb.toString());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
	
				query = "SELECT ep.ENCASHMENT_TYPE "
						+ "FROM V_CM_ENC_FORTHCOMING ep "
						+ "WHERE ep.ATM_ID = ? "
						+ "AND ep.DATE_FORTHCOMING_ENCASHMENT >= ? ";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, atmId);
				pstmt.setDate(2,
						JdbcUtils.getSqlDate(dateForCheck));
				rs = pstmt.executeQuery();
				while (rs.next()) {
					approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class, rs.getInt("ENCASHMENT_TYPE")));
				}
	
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
				
				newAtmList.add(new ObjectPair<Integer, List<EncashmentType>>(atmId, approvedEncList));
			}
			atmList.clear();
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return newAtmList;
	}
	
	public static List<ObjectPair<Integer,List<EncashmentType>>> checkExistingEncashmentsForSimpleList(
			Connection con, List<Integer> atmList) {
	
		List<ObjectPair<Integer,List<EncashmentType>>> newAtmList = new ArrayList<ObjectPair<Integer,List<EncashmentType>>>();
		List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
		List<Integer> updateEncList = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Date dateForCheck = new Date();
		try {
			for (Integer item : atmList) {
				int atmId = item;
				updateEncList = new ArrayList<Integer>();
				approvedEncList = new ArrayList<EncashmentType>();
				
				String query = "SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN "
						+ "WHERE ATM_ID = ? "
						+ "AND DATE_FORTHCOMING_ENCASHMENT >= ? "
						+ "AND IS_APPROVED = 0";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, atmId);
				pstmt.setDate(2,
						JdbcUtils.getSqlDate(dateForCheck));
				rs = pstmt.executeQuery();
				while (rs.next()) {
					updateEncList.add(rs.getInt("ENC_PLAN_ID"));
				}
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
	
				String deleteEncListClause = CmUtils.getIdListInClause(
						updateEncList, "ENC_PLAN_ID");
	
				StringBuffer sb = new StringBuffer(
						"DELETE FROM T_CM_ENC_PLAN_DENOM " + "WHERE 1=1 ");
				sb.append(deleteEncListClause);
				if (!updateEncList.isEmpty()){
					pstmt = con.prepareStatement(sb.toString());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
	
				sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_CURR "
						+ "WHERE 1=1 ");
				sb.append(deleteEncListClause);
				if (!updateEncList.isEmpty()){
					pstmt = con.prepareStatement(sb.toString());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
	
				query = "SELECT ep.ENCASHMENT_TYPE "
						+ "FROM V_CM_ENC_FORTHCOMING ep "
						+ "WHERE ep.ATM_ID = ? "
						+ "AND ep.DATE_FORTHCOMING_ENCASHMENT >= ? ";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, atmId);
				pstmt.setDate(2,
						JdbcUtils.getSqlDate(dateForCheck));
				rs = pstmt.executeQuery();
				while (rs.next()) {
					approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class, rs.getInt("ENCASHMENT_TYPE")));
				}
	
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
				
				newAtmList.add(new ObjectPair<Integer, List<EncashmentType>>(atmId, approvedEncList));
			}
			//atmList.clear();
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return newAtmList;
	}
	
	public static Integer getExistingPlanId(
			Connection con, int atmID) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Date dateForCheck = new Date();
		try {
				String query = "SELECT * FROM ( SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN "
						+ "WHERE ATM_ID = ? "
						+ "AND DATE_FORTHCOMING_ENCASHMENT >= ? "
						+ "AND IS_APPROVED = 0 ORDER BY ENC_PLAN_ID DESC) "
						+ " "+JdbcUtils.getLimitToFirstRowExpression(con)+" ";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, atmID);
				pstmt.setDate(2,
						JdbcUtils.getSqlDate(dateForCheck));
				rs = pstmt.executeQuery();
				if (rs.next()) {
					return rs.getInt("ENC_PLAN_ID");
				}
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return 0;
	}
	
	public static void ensureRouteConsistencyForEnc(ISessionHolder sessionHolder, Connection con, AnyAtmForecast forecast, EncashmentType missingEncType, boolean useMainCurr){
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		String sql = "Select route.ROUTE_STATUS, pnt.VISITED_FLAG, plan.DATE_FORTHCOMING_ENCASHMENT from T_CM_ROUTE route, T_CM_ROUTE_POINT pnt, T_CM_ENC_PLAN plan " +
					"where route.ID=pnt.route_id and pnt.point_src_id=plan.enc_plan_id " +
					"and plan.enc_plan_id=?";
		String deleteSql = "delete from T_CM_ROUTE_POINT " +
						"where POINT_SRC_ID=?";
		try {
			pstmt = con.prepareStatement(sql);
			pstmt.setInt(1, forecast.getEncPlanID());
			rs = pstmt.executeQuery();
			if(rs.next()){
				if (rs.getInt("ROUTE_STATUS")>1){
					//emulating user-defined encashment
					UserForecastFilter filter = new UserForecastFilter();
					filter.setAtmID(forecast.getAtmId());
					filter.setEncPlanID(forecast.getEncPlanID());
					filter.setForthcomingEncDate(JdbcUtils.getDate(rs.getDate("DATE_FORTHCOMING_ENCASHMENT")));
					filter.setNewDate(true);
					try {
						forecast = EncashmentForecast.makeForecastForAtm(sessionHolder, con, forecast.getAtmId(), forecast.getStartDate(), filter, ForecastingMode.PLAN, missingEncType, false, useMainCurr);
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
					//removing record from route points if date has changed
					if (!DateUtils.truncate(forecast.getForthcomingEncDate(), Calendar.DATE).equals(DateUtils.truncate(JdbcUtils.getDate(rs.getDate("DATE_FORTHCOMING_ENCASHMENT")), Calendar.DATE))){
						pstmt = con.prepareStatement(deleteSql);
						pstmt.setInt(1, forecast.getEncPlanID());
						pstmt.executeUpdate();
					}
					
				}
			}
		} catch (SQLException e) {
			ForecastCommonController.logger.error("Error while checking routes consistency", e);
		} finally {
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}
	
	public static void updateRoute(Connection con, int routeId){
		String infosql = "Select route.ID, route.ORG_ID, route.ROUTE_DATE from T_CM_ROUTE route where route.ID=?";
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		AtmRouteFilter filter = new AtmRouteFilter();
		try {
			pstmt = con.prepareStatement(infosql);
			pstmt.setInt(1, routeId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				filter.setRegion(rs.getInt("ORG_ID"));
				filter.setDateStart(JdbcUtils.getDate(rs.getDate("ROUTE_DATE")));
				JdbcUtils.close(rs);
				JdbcUtils.close(pstmt);
				try {
					RoutingController.recalculateRoute(con, filter, routeId);
				} catch (RoutingException e) {
					ForecastCommonController.logger.error("Error recalculating route "+routeId, e);
				}
			}
		} catch (SQLException e1) {
			ForecastCommonController.logger.error("Error updating route "+routeId, e1);
		} finally {
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}
	
	public static int getRouteIdForEnc(Connection con, AnyAtmForecast forecast){
		String infosql = "Select route.ID from T_CM_ROUTE route, T_CM_ROUTE_POINT pnt where route.ID=pnt.ROUTE_ID and pnt.POINT_SRC_ID=?";
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		int routeID = 0;
		try {
			pstmt = con.prepareStatement(infosql);
			pstmt.setInt(1, forecast.getEncPlanID());
			rs = pstmt.executeQuery();
			if(rs.next()){
				routeID = rs.getInt("ID");
			}
		} catch (SQLException e1) {
			ForecastCommonController.logger.error("Error getting route id for enc", e1);
		} finally {
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
		return routeID;
	}

	public static List<EncashmentType> checkExistingEncashments(Connection con,
			UserForecastFilter filter) {
	
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Date dateForCheck = new Date();
		List<EncashmentType> approvedEncList = new ArrayList<EncashmentType>();
		try {
			int atmId = filter.getAtmID();
			List<Integer> deleteEncList = new ArrayList<Integer>();
			String query = "SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN "
					+ "WHERE ATM_ID = ? "
					+ "AND DATE_FORTHCOMING_ENCASHMENT >= ? "
					+ "AND IS_APPROVED = 0 ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			pstmt.setDate(2,
					JdbcUtils.getSqlDate(dateForCheck));
			// pstmt.setInt(3, filter.getEncPlanID());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				deleteEncList.add(rs.getInt("ENC_PLAN_ID"));
			}
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
			
			deleteEncList.remove(Integer.valueOf(filter.getEncPlanID()));
			
			String deleteEncListClause = CmUtils.getIdListInClause(
					deleteEncList, "ENC_PLAN_ID");
	
			StringBuffer sb = new StringBuffer(
					"DELETE FROM T_CM_ENC_PLAN_DENOM " + "WHERE 1=1 ");
			sb.append(deleteEncListClause);
			if (!deleteEncList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
	
			sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_CURR "
					+ "WHERE 1=1 ");
			sb.append(deleteEncListClause);
			if (!deleteEncList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
	
			deleteEncList.remove(Integer.valueOf(filter.getEncPlanID()));
	
			deleteEncListClause = CmUtils.getIdListInClause(deleteEncList,
					"ENC_PLAN_ID");
	
			sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN_LOG "
					+ "WHERE 1=1 ");
			sb.append(deleteEncListClause);
			if (!deleteEncList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
	
			sb = new StringBuffer("DELETE FROM T_CM_ENC_PLAN " + "WHERE 1 = 1 ");
			sb.append(deleteEncListClause);
			if (!deleteEncList.isEmpty()){
				pstmt = con.prepareStatement(sb.toString());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
			}
	
			query = "SELECT ep.ENCASHMENT_TYPE FROM T_CM_ENC_PLAN ep "
					+ "WHERE ep.ATM_ID = ? "
					+ "AND ep.DATE_FORTHCOMING_ENCASHMENT >= ? "
					+ "AND ep.IS_APPROVED = 1 " 
					+ "AND "
					+ "( ep.ENCASHMENT_TYPE = ? " 
						+ "AND NOT EXISTS "+ 
							"(SELECT null FROM T_CM_ENC_CASHIN_STAT ecs " +
							"WHERE ep.ATM_ID = ecs.ATM_ID " +
							"AND ecs.CASH_IN_ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT) " +
						"OR ep.ENCASHMENT_TYPE = ? " + 
						"AND NOT EXISTS "+ 
							"(SELECT null FROM T_CM_ENC_CASHOUT_STAT ecs " +
							"WHERE ep.ATM_ID = ecs.ATM_ID " +
							"AND ecs.ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT) "+ 
						"OR ep.ENCASHMENT_TYPE = ? " +
						"AND NOT EXISTS "+ 
							"(SELECT null FROM T_CM_ENC_CASHOUT_STAT ecs " +
							"WHERE ep.ATM_ID = ecs.ATM_ID " +
							"AND ecs.ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT) " +
						"AND NOT EXISTS "+ 
							"(SELECT null FROM T_CM_ENC_CASHIN_STAT ecs " +
							"WHERE ep.ATM_ID = ecs.ATM_ID " +
							"AND ecs.CASH_IN_ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT)) ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			pstmt.setDate(2,
					JdbcUtils.getSqlDate(dateForCheck));
			pstmt.setInt(3, EncashmentType.CASH_IN.getId());
			pstmt.setInt(4, EncashmentType.CASH_OUT.getId());
			pstmt.setInt(5, EncashmentType.CASH_IN_AND_CASH_OUT.getId());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				approvedEncList.add(CmUtils.getEnumValueById(EncashmentType.class, rs.getInt("ENCASHMENT_TYPE")));
			}

			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		} catch (Exception e) {
			ForecastCommonController.logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return approvedEncList;
	}

}
