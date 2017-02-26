package ru.bpc.cm.cashmanagement.orm;

import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import ru.bpc.cm.cashmanagement.AtmPidListController;
import ru.bpc.cm.cashmanagement.orm.builders.AtmPidListBuilder;
import ru.bpc.cm.cashmanagement.orm.items.AtmGroupItem;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.items.settings.AtmGroupAtmItem;
import ru.bpc.cm.items.settings.calendars.CalendarAtmItem;
import ru.bpc.cm.management.Institute;
import ru.bpc.cm.utils.IntFilterItem;
import ru.bpc.cm.utils.Pair;

/**
 * Интерфейс-маппер для класса {@link AtmPidListController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 26.02.2017
 * @version 1.0.0
 *
 */
public interface AtmPidListMapper extends IMapper {

	@Results({
		@Result(column = "atm_id", property = "value", javaType = Integer.class),
		@Result(column = "external_atm_id", property = "label", javaType = String.class),
		@Result(column = "name", property = "descx", javaType = String.class)
	})
	@Select("SELECT distinct atm_id, name, external_atm_id FROM t_cm_atm WHERE "
			+ " atm_id in (select distinct atm_id from T_CM_CASHOUT_CASS_STAT union "
			+ " select distinct atm_id from T_CM_CASHIN_STAT union "
			+ " select distinct atm_id from T_CM_CASHIN_R_CASS_STAT) ORDER BY atm_id ")
	@ResultType(IntFilterItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<IntFilterItem> getAtmListFull();

	@ConstructorArgs({
		@Arg(column = "ATM_GROUP_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "ATM_GROUP_NAME", javaType = String.class)
	})
	@Select("SELECT agr.atm_id, agr.atm_group_id, ag.name as ATM_GROUP_NAME, aig.name as ATM_NAME, "
			+ "aig.external_atm_id FROM t_cm_atm2atm_group agr "
			+ "JOIN t_cm_atm_group ag ON (ag.id = agr.atm_group_id) "
			+ "JOIN t_cm_atm aig ON (aig.atm_id = agr.atm_id) "
			+ "WHERE agr.atm_id in (select id from t_cm_temp_atm_list) "
			+ "AND agr.atm_group_id in (select id from t_cm_temp_atm_group_list) ORDER BY ATM_GROUP_NAME, atm_id")
	@ResultType(AtmGroupItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmListSplitByGroupList();
	
	@ConstructorArgs({
		@Arg(column = "ATM_GROUP_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "ATM_GROUP_NAME", javaType = String.class)
	})
	@Select("SELECT agr.atm_id, agr.atm_group_id, ag.name as ATM_GROUP_NAME, aig.name as ATM_NAME, "
			+ "aig.external_atm_id FROM t_cm_atm2atm_group agr "
			+ "JOIN t_cm_atm_group ag ON (ag.id = agr.atm_group_id) "
			+ "JOIN t_cm_atm aig ON (aig.atm_id = agr.atm_id) "
			+ "WHERE agr.atm_id in (select id from t_cm_temp_atm_list) "
			+ "AND agr.atm_group_id in (select id from t_cm_temp_atm_group_list) ORDER BY ATM_GROUP_NAME, atm_id")
	@ResultType(AtmGroupItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmListSplitByGroupListDescx();
	
	@ConstructorArgs({
		@Arg(column = "ATM_GROUP_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "ATM_GROUP_NAME", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListSplitByGroupListDescxBuilder1")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmListSplitByGroupListDescx1(@Param("atmIdFilter") Integer atmIdFilter,
			@Param("nameAndAddressFilter") String nameAndAddressFilter);
	
	@ConstructorArgs({
		@Arg(column = "ATM_GROUP_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "ATM_GROUP_NAME", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListSplitByGroupListDescxBuilder2")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmListSplitByGroupListDescx2(@Param("atmId") Integer atmId,
			@Param("nameAndAddress") String nameAndAddress);
	
	@ConstructorArgs({
		@Arg(column = "ATM_GROUP_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "NAME", javaType = String.class)
	})
	@Select("SELECT agr.ATM_ID, a.NAME as ATM_NAME,agr.ATM_GROUP_ID,ag.NAME, a.external_atm_id FROM "
			+ "T_CM_ATM2ATM_GROUP agr join T_CM_ATM_GROUP ag on(ag.ID = agr.ATM_GROUP_ID) "
			+ "join T_CM_ATM a on (agr.ATM_ID=a.ATM_ID) WHERE ag.TYPE_ID = #{id} ORDER BY name,ATM_ID")
	@ResultType(AtmGroupItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmListSplitByGroupForAdm(@Param("id") Integer id);
	
	@ConstructorArgs({
		@Arg(column = "ATM_GROUP_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "NAME", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListSplitByGroupBuilder")
	@ResultType(AtmGroupItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupItem> getAtmListSplitByGroup(@Param("filterGroupsList") Boolean filterGroupsList,
			@Param("instList") List<Institute> instList, @Param("id") Integer id);
	
	@Results({
		@Result(column = "ATM_ID", property = "value", javaType = Integer.class),
		@Result(column = "EXTERNAL_ATM_ID", property = "label", javaType = String.class),
		@Result(column = "ATM_NAME", property = "descx", javaType = String.class)
	})
	@ResultType(IntFilterItem.class)
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListForGroupListBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<IntFilterItem> getAtmListForGroupList(@Param("sortByOutOfCurrDate") Boolean sortByOutOfCurrDate);
	
	@Results({
		@Result(column = "ATM_ID", property = "value", javaType = Integer.class),
		@Result(column = "EXTERNAL_ATM_ID", property = "label", javaType = String.class),
		@Result(column = "ATM_NAME", property = "descx", javaType = String.class)
	})
	@ResultType(IntFilterItem.class)
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListForGroupListBuilder_inst")
	@Options(useCache = true, fetchSize = 1000)
	List<IntFilterItem> getAtmListForGroupList(@Param("sortByOutOfCurrDate") Boolean sortByOutOfCurrDate,
			@Param("instList") List<Institute> instList);
	
	@Result(column = "atm_id", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT distinct a2ag.atm_id FROM t_cm_atm2atm_group a2ag "
			+ "join t_cm_atm a on (a2ag.atm_id = a.atm_id) WHERE a2ag.atm_group_id in (#{atmGroups})")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getAtmListForGroupList(@Param("atmGroups") String atmGroups);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "INST_ID", javaType = String.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)
	})
	@Select("SELECT agr.ATM_ID as ATM_ID,ai.EXTERNAL_ATM_ID,ai.INST_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME "
			+ "FROM T_CM_ATM2ATM_GROUP agr join T_CM_ATM ai on (ai.ATM_ID = agr.ATM_ID) "
			+ "WHERE agr.ATM_GROUP_ID = #{groupId} ORDER BY atm_id ")
	@ResultType(AtmGroupAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupAtmItem> getAtmListForGroup(@Param("groupId") Integer groupId);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ATM_NAME", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListForUserBuilder")
	@ResultType(IntFilterItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<IntFilterItem> getAtmListForUser(@Param("personId") String personId, @Param("typeId") Integer typeId);

	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "INST_ID", javaType = String.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListFullBuilder")
	@ResultType(AtmGroupAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupAtmItem> getAtmListFull(@Param("personId") String personId, @Param("typeId") Integer typeId,
			@Param("groupId") Integer groupId);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAtmListFullBuilder_inst")
	@ResultType(AtmGroupAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupAtmItem> getAtmListFull(@Param("personId") String personId, @Param("typeId") Integer typeId,
			@Param("groupId") Integer groupId, @Param("instList") List<Institute> instList);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "INST_ID", javaType = String.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)
	})
	@Select("SELECT ai.ATM_ID,ai.EXTERNAL_ATM_ID,ai.INST_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME "
			+ "FROM T_CM_ATM ai WHERE NOT EXISTS (SELECT null FROM T_CM_ATM2ATM_GROUP agr1 "
			+ "join T_CM_ATM_GROUP ag1 on (ag1.ID = agr1.ATM_GROUP_ID) WHERE ag1.ID = #{groupId} "
			+ "AND agr1.ATM_ID = ai.ATM_ID AND ag1.TYPE_ID = #{typeId}) ORDER BY atm_id")
	@ResultType(AtmGroupAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupAtmItem> getAvaliableAtmsListForGroup(@Param("groupId") Integer groupId,
			@Param("typeId") Integer typeId);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAvaliableAtmsListForGroupBuilder")
	@ResultType(AtmGroupAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupAtmItem> getAvaliableAtmsListForGroup(@Param("groupId") Integer groupId,
			@Param("typeId") Integer typeId, @Param("instList") List<Institute> instList);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "INST_ID", javaType = String.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)
	})
	@Select("SELECT ai.ATM_ID,ai.EXTERNAL_ATM_ID,ai.INST_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME "
			+ "FROM T_CM_ATM ai WHERE NOT EXISTS (SELECT null FROM T_CM_ATM2ATM_GROUP agr1 "
			+ "join T_CM_ATM_GROUP ag1 on (ag1.ID = agr1.ATM_GROUP_ID) WHERE ag1.ID = #{groupId} "
			+ "AND agr1.ATM_ID = ai.ATM_ID AND ag1.TYPE_ID != #{typeId}) ORDER BY atm_id")
	@ResultType(AtmGroupAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupAtmItem> getAvaliableAtmsListForAttributeGroup(@Param("groupId") Integer groupId,
			@Param("typeId") Integer typeId);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getAvaliableAtmsListForAttributeGroupBuilder")
	@ResultType(AtmGroupAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmGroupAtmItem> getAvaliableAtmsListForAttributeGroup(@Param("groupId") Integer groupId,
			@Param("typeId") Integer typeId, @Param("instList") List<Institute> instList);
	
	@Results({
		@Result(column = "ATM_ID", property = "atmId", javaType = Integer.class),
		@Result(column = "DESCX", property = "calendarDescx", javaType = String.class),
		@Result(column = "CITY", property = "city", javaType = String.class),
		@Result(column = "STATE", property = "state", javaType = String.class),
		@Result(column = "STREET", property = "street", javaType = String.class),
		@Result(column = "CL_ID", property = "calendarId", javaType = Integer.class),
		@Result(column = "INST_ID", property = "instId", javaType = String.class),
		@Result(column = "ATM_NAME", property = "atmName", javaType = String.class),
		@Result(column = "EXTERNAL_ATM_ID", property = "extAtmId", javaType = String.class)
	})
	@Select("select distinct a.atm_id,a.EXTERNAL_ATM_ID,DESCX,a.STATE,a.CITY,a.STREET,c.CL_ID,a.INST_ID, a.NAME as ATM_NAME "
			+ "from t_cm_atm a left outer join t_cm_calendar c on (a.CALENDAR_ID = c.CL_ID) order by a.atm_id")
	@ResultType(CalendarAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<CalendarAtmItem> getCalendarAtmList();
	
	@Results({
		@Result(column = "ATM_ID", property = "atmId", javaType = Integer.class),
		@Result(column = "DESCX", property = "calendarDescx", javaType = String.class),
		@Result(column = "CITY", property = "city", javaType = String.class),
		@Result(column = "STATE", property = "state", javaType = String.class),
		@Result(column = "STREET", property = "street", javaType = String.class),
		@Result(column = "CL_ID", property = "calendarId", javaType = Integer.class),
		@Result(column = "INST_ID", property = "instId", javaType = String.class),
		@Result(column = "ATM_NAME", property = "atmName", javaType = String.class),
		@Result(column = "EXTERNAL_ATM_ID", property = "extAtmId", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getCalendarAtmListBuilder_Adm")
	@ResultType(CalendarAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<CalendarAtmItem> getCalendarAtmList_Adm(@Param("instList") List<Institute> instList);
	
	@Results({
		@Result(column = "ATM_ID", property = "atmId", javaType = Integer.class),
		@Result(column = "DESCX", property = "calendarDescx", javaType = String.class),
		@Result(column = "CITY", property = "city", javaType = String.class),
		@Result(column = "STATE", property = "state", javaType = String.class),
		@Result(column = "STREET", property = "street", javaType = String.class),
		@Result(column = "CL_ID", property = "calendarId", javaType = Integer.class),
		@Result(column = "INST_ID", property = "instId", javaType = String.class),
		@Result(column = "ATM_NAME", property = "atmName", javaType = String.class),
		@Result(column = "EXTERNAL_ATM_ID", property = "extAtmId", javaType = String.class)
	})
	@SelectProvider(type = AtmPidListBuilder.class, method = "getCalendarAtmListBuilder_User")
	@ResultType(CalendarAtmItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<CalendarAtmItem> getCalendarAtmList_User(@Param("instList") List<Institute> instList);
	
	@Delete("DELETE FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{groupId} ")
	void deleteAtmGroupAtms(@Param("groupId") Integer groupId);
	
	@Insert("INSERT INTO T_CM_ATM2ATM_GROUP(ATM_GROUP_ID, ATM_ID) VALUES(#{groupId},#{atmId})")
	void saveAtmGroupAtms(@Param("groupId") Integer groupId, @Param("atmId") Integer atmId);
	
	@Delete("DELETE FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{groupId} AND "
			+ "ATM_ID in (select id from t_cm_temp_atm_list)")
	void deleteAtmGroupAtmsTemp(@Param("groupId") Integer groupId);

	@Insert("INSERT INTO T_CM_ATM2ATM_GROUP(ATM_GROUP_ID, ATM_ID) VALUES(#{groupId},#{atmId})")
	void saveAtmGroupAtmsTemp(@Param("groupId") Integer groupId, @Param("atmId") Integer atmId);
	
	@Result(column = "DESCX", javaType = String.class)
	@ResultType(Pair.class)
	@Select("SELECT COALESCE(ag.NAME,'_') as DESCX FROM T_CM_ATM2ATM_GROUP agr "
			+ "join T_CM_ATM_GROUP ag on (agr.atm_group_id = ag.id) WHERE ATM_ID =  #{atmId} "
			+ "AND ag.id != #{groupId} AND ag.type_id = #{typeId}")
	Pair getDescx_saveAttributeGroupAtms(@Param("atmId") Integer atmId, @Param("groupId") Integer groupId,
			@Param("typeId") Integer typeId);

	@Delete("DELETE FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{groupId}")
	void delete_saveAttributeGroupAtms(@Param("groupId") Integer groupId);
	
	@Delete("DELETE FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{groupId} AND "
			+ "ATM_ID in (select id from t_cm_temp_atm_list)")
	void delete_saveAttributeGroupAtmsTemp(@Param("groupId") Integer groupId);
}
