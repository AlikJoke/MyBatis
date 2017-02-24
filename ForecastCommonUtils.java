package ru.bpc.cm.forecasting;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ru.bpc.cm.forecasting.controllers.ForecastCommonController;
import ru.bpc.cm.items.forecast.ForecastStatDay;

public class ForecastCommonUtils {
	
	protected static double calculateCurrencyTakeOffForPeriod(Connection con,
			double averageDemandWithCoeffs, int atmId, Date startDate,
			int period, Map<Date, Double> calendarDayMapForecast) {
		Map<Date, ForecastStatDay> dayMapForecast = getDayMapForecast(con,
				startDate, atmId, period, averageDemandWithCoeffs);
		return getTakeOffForecast(dayMapForecast, calendarDayMapForecast);
	}

	private static double getTakeOffForecast(Map<Date, ForecastStatDay> days,
			Map<Date, Double> calendar) {
		double averageDemand = 0;
		double takeOffSumm = 0;
		double takeOffAverage = 0;
		double atmNotAvailableDays = 0;
		int daysSize = days.size() == 0 ? 1 : days.size();

		for (Entry<Date, ForecastStatDay> entry : days.entrySet()) {
			Date day = entry.getKey();
			if (calendar.get(day) != null) {
				if (calendar.get(day) != -1) {
					takeOffSumm += entry.getValue().getTakeOffs();
				}
				entry.getValue().setCalendarCoeff(calendar.get(day));
			} else {
				entry.getValue().setCalendarCoeff(1);
				takeOffSumm += entry.getValue().getTakeOffs();
			}
		}

		takeOffAverage = takeOffSumm / (daysSize - atmNotAvailableDays);

		for (ForecastStatDay item : days.values()) {
			if (item.getCalendarCoeff() != -1) {
				averageDemand += takeOffAverage * (item.getCalendarCoeff());
			}
		}
		return averageDemand;
	}
	
	private static Map<Date, ForecastStatDay> getDayMapForecast(
			Connection con, Date startDate, int atmId, int period,
			double takeOffAverage) {
		List<Date> days = new ArrayList<Date>(
				ForecastCommonController.getAtmAvailableDaysForecast(con,
						atmId, period, startDate, null));
		Map<Date, ForecastStatDay> forecastDayMap = new HashMap<Date, ForecastStatDay>();
		for (Date date : days) {
			ForecastStatDay day = new ForecastStatDay();
			day.setDay(date);
			day.setTakeOffs(takeOffAverage);
			forecastDayMap.put(day.getDay(), day);
		}
		return forecastDayMap;
	}

	
}
