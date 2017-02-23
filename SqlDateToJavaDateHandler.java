package ru.bpc.cm.cashmanagement.orm.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import ru.bpc.cm.utils.db.JdbcUtils;

public class SqlDateToJavaDateHandler implements TypeHandler<Date> {

	@Override
	public void setParameter(PreparedStatement ps, int i, Date parameter, JdbcType jdbcType) throws SQLException {
		// while nothing
	}

	@Override
	public Date getResult(ResultSet rs, String columnName) throws SQLException {
		return JdbcUtils.getDate(rs.getDate(columnName));
	}

	@Override
	public Date getResult(ResultSet rs, int columnIndex) throws SQLException {
		return JdbcUtils.getDate(rs.getDate(columnIndex));
	}

	@Override
	public Date getResult(CallableStatement cs, int columnIndex) throws SQLException {
		// while nothing
		return null;
	}

}