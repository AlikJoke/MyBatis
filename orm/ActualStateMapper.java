package ru.bpc.cm.monitoring.orm;

import java.util.List;

import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.SelectProvider;

import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.filters.MonitoringFilter;
import ru.bpc.cm.items.monitoring.AtmActualStateItem;
import ru.bpc.cm.monitoring.ActualStateController;

public interface ActualStateMapper extends IMapper {
	
	@SelectProvider(type = ActualStateController.class, method = "builderAtmActualStateQuery")
	@Options(useCache = true)
	List<AtmActualStateItem> getAtmActualStateList(MonitoringFilter filter);
}
