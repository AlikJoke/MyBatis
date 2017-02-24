package ru.bpc.cm.cashmanagement.orm;

import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import ru.bpc.cm.cashmanagement.AtmGroupListController;
import ru.bpc.cm.cashmanagement.orm.builders.AtmGroupListBuilder;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.items.settings.AtmGroupItem;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.Pair;

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
	
	@Results({
		@Result(column = "ID", property = "atmGroupID", javaType = Integer.class),
		@Result(column = "NAME", property = "name", javaType = String.class),
		@Result(column = "DESCRIPTION", property = "descx", javaType = String.class),
		@Result(column = "TYPE_ID", property = "type", javaType = Integer.class)
	})
	@SelectProvider(type = AtmGroupListBuilder.class, method = "getAtmGroupListBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmGroupList(@Param("personId") String personId, @Param("typeId") Integer typeId,
			@Param("fetchSubordinatesGroups") Boolean fetchSubordinatesGroups);
	
	@SelectProvider(type = AtmGroupListBuilder.class, method = "getAtmGroupIdListBuilder")
	@ResultType(Integer.class)
	List<Integer> getAtmGroupIdList(@Param("personId") String personId, @Param("typeId") Integer typeId,
			@Param("fetchSubordinatesGroups") Boolean fetchSubordinatesGroups);
	
	@Results({
		@Result(column = "NAME", javaType = String.class),
		@Result(column = "DESCRIPTION", javaType = String.class)
	})
	@SelectProvider(type = AtmGroupListBuilder.class, method = "getAtmGroupListForAtmBuilder")
	List<Pair> getAtmGroupListForAtm(@Param("personId") String personId, @Param("typeId") Integer typeId,
			@Param("fetchSubordinatesGroups") Boolean fetchSubordinatesGroups, @Param("atmId") Integer atmId);
}
