package ru.bpc.cm.cashmanagement.orm.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import ru.bpc.cm.utils.db.JdbcUtils;

public class TimestampToHourTypeHandler implements TypeHandler<Integer> {

	@Override
	public void setParameter(PreparedStatement ps, int i, Integer parameter, JdbcType jdbcType) throws SQLException {
		// while nothing
	}

	@Override
	public Integer getResult(ResultSet rs, String columnName) throws SQLException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(JdbcUtils.getTimestamp(rs.getTimestamp(columnName)));
		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	@Override
	public Integer getResult(ResultSet rs, int columnIndex) throws SQLException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(JdbcUtils.getTimestamp(rs.getTimestamp(columnIndex)));
		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	@Override
	public Integer getResult(CallableStatement cs, int columnIndex) throws SQLException {
		// while nothing
		return null;
	}
}