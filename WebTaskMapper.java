package ru.bpc.cm.tasks.orm;

import java.sql.Timestamp;
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
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.type.JdbcType;

import ru.bpc.cm.items.tasks.WebTaskItem;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.tasks.orm.builders.WebTaskBuilder;
import ru.bpc.cm.utils.IntFilterItem;

/**
 * Интерфейс-маппер для класса WebTaskController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 29.04.2017
 * @version 1.0.0
 *
 */
public interface WebTaskMapper extends IMapper {

	@Results({
		@Result(column = "TASK_ID", property = "taskId", javaType = Integer.class, jdbcType = JdbcType.INTEGER),
		@Result(column = "NAME", property = "name", javaType = String.class),
		@Result(column = "CRON", property = "cron", javaType = String.class),
		@Result(column = "DESCRIPTION", property = "descx", javaType = String.class),
		@Result(column = "TYPE", property = "intType", javaType = Integer.class, jdbcType = JdbcType.INTEGER)
	})
	@ResultType(WebTaskItem.class)
	@Select("SELECT t.TASK_ID,t.NAME,t.DESCRIPTION, t.CRON,t.TYPE FROM T_CM_TASK t WHERE 1=1")
	@Options(useCache = true, fetchSize = 1000)
	List<WebTaskItem> getWebTaskFullList();
	
	@Results({
		@Result(column = "TASK_ID", property = "taskId", javaType = Integer.class, jdbcType = JdbcType.INTEGER),
		@Result(column = "NAME", property = "name", javaType = String.class),
		@Result(column = "CRON", property = "cron", javaType = String.class),
		@Result(column = "DESCRIPTION", property = "descx", javaType = String.class),
		@Result(column = "TYPE", property = "intType", javaType = Integer.class, jdbcType = JdbcType.INTEGER),
		@Result(column = "USER_NAME", property = "userName", javaType = String.class)
	})
	@ResultType(WebTaskItem.class)
	@Select("SELECT t.TASK_ID,t.NAME,t.DESCRIPTION, t.CRON,t.TYPE,u.NAME as USER_NAME FROM T_CM_TASK t "
			+ "join t_cm_user u on (u.ID = t.USER_ID) WHERE 1=1 AND (t.TYPE = #{intType} OR #{intType} = 0)")
	@Options(useCache = true, fetchSize = 1000)
	List<WebTaskItem> getWebTaskList(@Param("intType") Integer intType);
	
	@Results({
		@Result(column = "TASK_ID", property = "first", javaType = Integer.class, jdbcType = JdbcType.INTEGER),
		@Result(column = "VALUE", property = "second", javaType = String.class),
		@Result(column = "TYPE", property = "third", javaType = Integer.class, jdbcType = JdbcType.INTEGER)
	})
	@ResultType(TripleObject.class)
	@Select("SELECT t.TASK_ID,t.VALUE,t.TYPE FROM T_CM_TASK_PARAM t WHERE 1=1 AND t.TASK_ID = #{taskId}")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Integer, String, Integer>> getWebTaskParamList(@Param("taskId") Integer taskId);
	
	@Result(column = "USER_ID", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT t.USER_ID FROM T_CM_TASK t WHERE t.TASK_ID = #{taskId} ")
	@Options(useCache = true)
	Integer getWebTaskUserId(@Param("taskId") Integer taskId);
	
	@Update("update T_CM_TASK SET LAST_EXEC=#{lastExecTime, jdbcType=TIMESTAMP} WHERE TASK_ID = #{taskId} ")
	void setLastExecTime(@Param("taskId") Integer taskId, @Param("lastExecTime") Timestamp lastExecTime);
	
	@Result(column = "LAST_EXEC", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("Select LAST_EXEC from T_CM_TASK WHERE TASK_ID = #{taskId} ")
	Timestamp getLastExecTime(@Param("taskId") Integer taskId);
	
	@Result(column = "TASK_ID", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = WebTaskBuilder.class, method = "getTaskIdForChangeWebTaskBuilder")
	@Options(useCache = true)
	Integer getTaskIdForChangeWebTask(@Param("session") SqlSession session);

	@Insert("INSERT INTO T_CM_TASK (TASK_ID,CRON,TYPE,DESCRIPTION,NAME,USER_ID) VALUES (#{taskId}, #{cron, jdbcType = VARCHAR}, "
			+ "#{taskType}, #{descx, jdbcType = VARCHAR}, #{name, jdbcType = VARCHAR}, #{userId})")
	void changeWebTask_insert(@Param("taskId") Integer taskId, @Param("cron") String cron,
			@Param("taskType") Integer taskType, @Param("descx") String descx, @Param("name") String name,
			@Param("userId") Integer userId);

	@Delete("DELETE FROM T_CM_TASK_PARAM WHERE TASK_ID = #{taskId}")
	void changeWebTask_deleteFromTaskParam(@Param("taskId") Integer taskId);

	@Delete("DELETE FROM T_CM_TASK WHERE TASK_ID = #{taskId}")
	void changeWebTask_deleteFromTask(@Param("taskId") Integer taskId);

	@Update("UPDATE T_CM_TASK SET CRON = #{cron}, DESCRIPTION = #{descx}, NAME = #{name} WHERE TASK_ID = #{taskId}")
	void changeWebTask_update(@Param("taskId") Integer taskId, @Param("cron") String cron, @Param("descx") String descx,
			@Param("name") String name);

	@Insert("INSERT INTO T_CM_TASK_PARAM(TASK_ID, VALUE, TYPE) VALUES(#{taskId}, #{value}, #{taskType})")
	void insertWebTaskParams(@Param("taskId") Integer taskId, @Param("value") String value,
			@Param("taskType") Integer taskType);

	@Update("UPDATE T_CM_TASK_PARAM SET VALUE = #{value} WHERE TASK_ID = #{taskId} and TYPE = #{taskType}")
	void updateWebTaskParams(@Param("taskId") Integer taskId, @Param("value") String value,
			@Param("taskType") Integer taskType);
	
	@ConstructorArgs({
		@Arg(column = "id", javaType = Integer.class),
		@Arg(column = "description", javaType = String.class)
	})
	@ResultType(IntFilterItem.class)
	@SelectProvider(type = WebTaskBuilder.class, method = "getGroupListParamBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<IntFilterItem> getGroupListParam(@Param("groupIdParam") String groupIdParam, @Param("typeId") Integer typeId);
}
