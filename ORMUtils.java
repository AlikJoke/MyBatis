package ru.bpc.cm.config.utils;

import java.sql.SQLException;

import org.apache.ibatis.session.SqlSession;

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
}
