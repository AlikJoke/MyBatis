package ru.bpc.cm.cashmanagement.orm;

import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.cashmanagement.AtmGroupListController;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.items.settings.AtmGroupItem;
import ru.bpc.cm.utils.IFilterItem;

/**
 * Интерфейс-маппер для класса {@link AtmGroupListController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 24.02.2017
 * @version 1.0.0
 *
 */
public interface AtmGroupListMapper extends IMapper {

	@ConstructorArgs({
		@Arg(column = "id", javaType = Integer.class),
		@Arg(column = "name", javaType = String.class)
	})
	@Select("SELECT id, name FROM t_cm_atm_group WHERE type_id = #{typeId} ORDER BY name")
	@Options(useCache = true, fetchSize = 1000)
	List<IFilterItem<Integer>> getFullGroupList(@Param("typeId") Integer typeId);
	
	@ConstructorArgs({
		@Arg(column = "id", javaType = Integer.class),
		@Arg(column = "name", javaType = String.class)
	})
	@Select("SELECT id, name FROM t_cm_atm_group WHERE type_id <> #{typeId} ORDER BY name")
	@Options(useCache = true, fetchSize = 1000)
	List<IFilterItem<Integer>> getFullAttrGroupList(@Param("typeId") Integer typeId);
	
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmGroupList();
}
