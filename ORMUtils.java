package ru.bpc.cm.config.utils;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.utils.db.JdbcUtils;

/**
 * Класс, содержащий различные статические методы, требующиеся для работы с
 * базой данных. Является ORM-аналогом {@link JdbcUtils}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 09.03.2017
 * @version 1.1.4
 *
 */
public class ORMUtils {

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
}
