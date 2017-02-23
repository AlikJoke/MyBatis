package ru.bpc.cm.forecasting.orm;

import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.ForecastCompareController;

/**
 * Интерфейс-маппер для класса {@link ForecastCompareController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 23.02.2017
 * @version 1.0.0
 *
 */
public interface ForecastCompareMapper extends IMapper {

	void insertCompareForecastData();
	
	int getStatsDatesCount();
}
