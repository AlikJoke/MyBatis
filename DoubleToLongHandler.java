package ru.bpc.cm.orm.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

public class DoubleToLongHandler implements TypeHandler<Long> {

	@Override
	public void setParameter(PreparedStatement ps, int i, Long parameter, JdbcType jdbcType) throws SQLException {
		// while nothing
	}

	@Override
	public Long getResult(ResultSet rs, String columnName) throws SQLException {
		return Math.round(rs.getDouble(columnName));
	}

	@Override
	public Long getResult(ResultSet rs, int columnIndex) throws SQLException {
		return Math.round(rs.getDouble(columnIndex));
	}

	@Override
	public Long getResult(CallableStatement cs, int columnIndex) throws SQLException {
		// while nothing
		return null;
	}
}
