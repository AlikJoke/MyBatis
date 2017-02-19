package ru.bpc.cm.cashmanagement.orm.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

public class PbClassHandler implements TypeHandler<String> {

	@Override
	public void setParameter(PreparedStatement ps, int i, String parameter, JdbcType jdbcType) throws SQLException {
		// while nothing
	}

	@Override
	public String getResult(ResultSet rs, String columnName) throws SQLException {
		return rs.getInt(columnName) == 0 ? rs.getString("CASS_CURR") : "na";
	}

	@Override
	public String getResult(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getInt(columnIndex) == 0 ? rs.getString("CASS_CURR") : "na";
	}

	@Override
	public String getResult(CallableStatement cs, int columnIndex) throws SQLException {
		// while nothing
		return null;
	}

}