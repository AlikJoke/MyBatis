package ru.bpc.cm.cashmanagement.orm.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

public class DisabledCNTTypeHandler implements TypeHandler<Boolean> {

	@Override
	public void setParameter(PreparedStatement ps, int i, Boolean parameter, JdbcType jdbcType) throws SQLException {
		// while nothing
	}

	@Override
	public Boolean getResult(ResultSet rs, String columnName) throws SQLException {
		Integer arg = rs.getInt(columnName);
		return arg == null ? false : arg > 0;
	}

	@Override
	public Boolean getResult(ResultSet rs, int columnIndex) throws SQLException {
		Integer arg = rs.getInt(columnIndex);
		return arg == null ? false : arg > 0;
	}

	@Override
	public Boolean getResult(CallableStatement cs, int columnIndex) throws SQLException {
		// while nothing
		return null;
	}

}