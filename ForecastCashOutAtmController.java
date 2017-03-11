package ru.bpc.cm.forecasting.controllers;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.forecasting.orm.ForecastCashOutAtmMapper;

public class ForecastCashOutAtmController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");
	
	private static Class<ForecastCashOutAtmMapper> getMapperClass() {
		return ForecastCashOutAtmMapper.class;
	}
	
	/**
	 * Used to get sum of currency that is left in the ATM by the moment of planning.
	 *
	 * @param con
	 *            - DB connection
	 * @param atmID
	 *            - ATM ID
	 * @param currency
	 *            - code of currency
	 * @return remaining sum of currency
	 */
	public static double getCurrRemaining(ISessionHolder sessionHolder, int atmId, int currency, int encID, Date startDate) {
		double res = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<Double> currRemaining = session.getMapper(getMapperClass()).getCurrRemaining(atmId, currency, encID,
					new Timestamp(startDate.getTime()));

			if (!currRemaining.isEmpty())
				res = currRemaining.get(0);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return res;
	}

}
