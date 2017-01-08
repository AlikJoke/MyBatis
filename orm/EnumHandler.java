package ru.bpc.cm.cashmanagement.orm.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.poi.hssf.record.formula.functions.T;

import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.utils.CmUtils;

public class EnumHandler implements TypeHandler<T> {

	@Override
	public void setParameter(PreparedStatement ps, int i, T parameter, JdbcType jdbcType) throws SQLException {
		// while nothing
	}

	@Override
	public T getResult(ResultSet rs, String columnName) throws SQLException {
		Integer arg = rs.getInt(columnName);
		return T.class.cast(CmUtils.getEnumValueById(AtmCassetteType.class, arg));
	}

	@Override
	public T getResult(ResultSet rs, int columnIndex) throws SQLException {
		Integer arg = rs.getInt(columnIndex);
		return T.class.cast(CmUtils.getEnumValueById(AtmCassetteType.class, arg));
	}

	@Override
	public T getResult(CallableStatement cs, int columnIndex) throws SQLException {
		// while nothing
		return null;
	}

}
