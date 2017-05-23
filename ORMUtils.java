package ru.bpc.cm.orm.common;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.utils.CollectionUtils;
import ru.bpc.cm.utils.db.JdbcUtils;

/**
 * Класс, содержащий различные статические методы, требующиеся для работы с
 * базой данных. Является ORM-аналогом {@link JdbcUtils}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 09.03.2017
 * @version 1.1.6
 *
 */
public class ORMUtils {

	public static final int MAX_PARAMS_FOR_IN_CONDITION = 999;

	public static final int ORACLE_DUP_VAL_ERROR_CODE = 1;
	public static final int DB2_DUP_VAL_ERROR_CODE = 1;
	public static final int POSTGRES_DUP_VAL_ERROR_CODE = 1;
	
	public static int getDuplicateValueErrorCode(SqlSession session) throws SQLException {
		String DbName = getDbName(session);
		if (DbName == "Oracle") {
			return ORACLE_DUP_VAL_ERROR_CODE;
		} else if (DbName == "DB2") {
			return DB2_DUP_VAL_ERROR_CODE;
		} else if (DbName == "PostgreSQL") {
			return POSTGRES_DUP_VAL_ERROR_CODE;
		} else {
			throw new SQLException();
		}
	}
	
	public static String getNextSequence(SqlSession session, String seqName) throws SQLException {
		String DbName = getDbName(session);
		if (DbName == "Oracle" || DbName == "DB2") {
			return seqName + ".nextval";
		} else if (DbName == "PostgreSQL") {
			return "nextval('" + seqName + "')";
		} else {
			throw new SQLException();
		}
	}

	public static String getLimitToFirstRowExpression(SqlSession session) {
		String DbName = getDbName(session);
		if (DbName == "Oracle") {
			return " where rownum = 1 ";
		} else if (DbName == "DB2") {
			return " where rownum = 1 ";
		} else if (DbName == "PostgreSQL") {
			return " limit 1 ";
		} else {
			return " where rownum = 1 ";
		}
	}

	public static String getSingleRowExpression(SqlSession session) {
		String DbName = getDbName(session);
		if (DbName == "Oracle") {
			return " and rownum = 1 ";
		} else if (DbName == "DB2") {
			return " and rownum = 1 ";
		} else if (DbName == "PostgreSQL") {
			return " limit 1 ";
		} else {
			return " and rownum = 1 ";
		}
	}

	public static String getDbName(SqlSession session) {
		String DbName;
		try {
			DbName = session.getConnection().getMetaData().getDatabaseProductName();
			if (DbName.startsWith("DB2") && !DbName.isEmpty()) {
				DbName = "DB2";
			}
		} catch (SQLException e) {
			DbName = "Undefined";
		}
		return DbName;
	}

	public static String getCurrentSequence(SqlSession session, String seqName) throws SQLException {
		String DbName = getDbName(session);
		if (DbName == "Oracle" || DbName == "DB2") {
			return seqName + ".currval";
		} else if (DbName == "PostgreSQL") {
			return "currval('" + seqName + "')";
		} else {
			throw new SQLException();
		}
	}

	public static String getFromDummyExpression(SqlSession session) throws SQLException {
		String DbName = getDbName(session);
		if (DbName == "Oracle") {
			return " from DUAL";
		} else if (DbName == "DB2") {
			return " from SYSIBM.SYSDUMMY1";
		} else if (DbName == "PostgreSQL") {
			return "";
		} else {
			throw new SQLException();
		}
	}

	public static void createTemporaryTableIfNotExists(SqlSession session, String tableName) throws SQLException {
		String DbName = getDbName(session);
		String tempSql;
		String signature = "";
		if (DbName == "PostgreSQL") {
			if (tableName.equalsIgnoreCase("t_cm_temp_atm_group_list")) {
				signature = "id NUMERIC(9)";
			} else if (tableName.equalsIgnoreCase("t_cm_temp_atm_list")) {
				signature = "id NUMERIC(9)";
			} else if (tableName.equalsIgnoreCase("t_cm_temp_enc_plan_curr")) {
				signature = "enc_plan_id NUMERIC(6) NOT NULL," + " curr_code NUMERIC(3) NOT NULL,"
						+ " curr_summ NUMERIC(15) NOT NULL";
			} else if (tableName.equalsIgnoreCase("t_cm_temp_enc_plan_denom")) {
				signature = "enc_plan_id NUMERIC(6) NOT NULL," + " denom_value NUMERIC(6) NOT NULL,"
						+ " denom_count NUMERIC(4) NOT NULL," + " denom_curr NUMERIC(3) NOT NULL";

			} else if (tableName.equalsIgnoreCase("t_cm_temp_enc_plan")) {
				signature = "enc_plan_id NUMERIC(6) NOT NULL," + " atm_id NUMERIC(9) NOT NULL,"
						+ " date_forthcoming_encashment TIMESTAMP";
			} else if (tableName.equalsIgnoreCase("t_cm_temp_enc_report")) {
				signature = "remaining NUMERIC(15) NOT NULL," + " curr_code VARCHAR(7) NOT NULL,"
						+ " stat_date TIMESTAMP," + " end_of_stats_date NUMERIC(1)";
			}
			tempSql = "CREATE TEMPORARY TABLE IF NOT EXISTS " + tableName + " (" + signature + ") "
					+ " ON COMMIT DELETE ROWS";
			Statement stmt = session.getConnection().createStatement();
			stmt.executeUpdate(tempSql);
		}
	}

	public static String getTruncateTableUnrecoverable(SqlSession session, String tableName) throws SQLException {
		String DbName = getDbName(session);
		if (DbName == "Oracle") {
			return " TRUNCATE TABLE " + tableName + " DROP STORAGE ";
		} else if (DbName == "DB2") {
			return " DELETE FROM " + tableName;
		} else if (DbName == "PostgreSQL") {
			return "TRUNCATE " + tableName;
		} else {
			throw new SQLException();
		}
	}

	public static String getDeleteFromTableFieldInConditional(SqlSession session, String tableName, String columnName,
			List<Integer> inClauseList) throws SQLException {
		String query = "";
		if (inClauseList != null && !inClauseList.isEmpty()) {
			String DbName = getDbName(session);
			boolean splitFlag = false;
			if (DbName == "Oracle") {
				query = " DELETE FROM " + tableName + " where ";
			} else if (DbName == "DB2") {
				query = " DELETE FROM " + tableName + " where ";
			} else if (DbName == "PostgreSQL") {
				query = "TRUNCATE " + tableName + " where ";
			} else {
				throw new SQLException();
			}
			for (List<Integer> inList : CollectionUtils.splitListBySizeView(inClauseList,
					MAX_PARAMS_FOR_IN_CONDITION)) {
				if (splitFlag) {
					query += " or ";
				}
				query += columnName + " in (" + StringUtils.join(inList, ",") + ") ";
				splitFlag = true;
			}
		}

		return query;
	}

	/**
	 * Возвращает первое значение из коллекции, если коллекция не пуста.
	 * <p>
	 * 
	 * @param <T>
	 * 
	 * @param defaultValue
	 *            - дефолтное значение, если коллекция пуста.
	 * @param list
	 *            - список некоторых значений.
	 * @return первое значение, либо {@code null}, либо дефолтное значение.
	 */
	public static <T> T getSingleValue(List<T> list, T defaultValue) {
		if (list == null || list.isEmpty()) {
			return defaultValue;
		}
		return list.get(0);
	}

	/**
	 * Возвращает первое значение из коллекции, если коллекция не пуста.
	 * <p>
	 * 
	 * @param <T>
	 * 
	 * @param list
	 *            - список некоторых значений.
	 * @return первое значение, либо {@code null}.
	 */
	public static <T> T getSingleValue(List<T> list) {
		if (list == null || list.isEmpty()) {
			return null;
		}
		return list.get(0);
	}
	
	public static <T> T getRandomValue(List<T> list) {
		if (list == null || list.isEmpty()) {
			return null;
		}
		Random rand = new Random();
		return list.get(rand.nextInt(list.size()));
	}

	/**
	 * Возвращает {@code value}, если {@code value != null}, иначе
	 * {@code defaultValue}.
	 * <p>
	 * 
	 * @param value
	 *            - значение, проверяемое на существование; может быть
	 *            {@code null}.
	 * @param defaultValue
	 *            - значение, которое будет выдано, если {@code value == null};
	 *            не может быть {@code null}.
	 * @return not-null value.
	 */
	public static <T> T getNotNullValue(T value, T defaultValue) {
		if (value == null) {
			return defaultValue;
		}
		return value;
	}
	
	public static boolean isExpired(long current) {
		if (System.currentTimeMillis() > current + 1000 * 60)
			return true;
		return false;
	}
}
