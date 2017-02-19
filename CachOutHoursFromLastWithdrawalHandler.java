package ru.bpc.cm.cashmanagement.orm.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.poi.hssf.record.formula.functions.T;

import ru.bpc.cm.utils.CmUtils;

public class CachOutHoursFromLastWithdrawalHandler implements TypeHandler<T> {

	@Override
	public void setParameter(PreparedStatement ps, int i, T parameter, JdbcType jdbcType) throws SQLException {
		// while nothing
	}

	@Override
	public T getResult(ResultSet rs, String columnName) throws SQLException {
		Timestamp arg = rs.getTimestamp(columnName);
		return T.class.cast(CmUtils.getHoursBetweenTwoDates(arg, new Date()));
	}

	@Override
	public T getResult(ResultSet rs, int columnIndex) throws SQLException {
		Timestamp arg = rs.getTimestamp(columnIndex);
		return T.class.cast(CmUtils.getHoursBetweenTwoDates(arg, new Date()));
	}

	@Override
	public T getResult(CallableStatement cs, int columnIndex) throws SQLException {
		// while nothing
		return null;
	}
}
