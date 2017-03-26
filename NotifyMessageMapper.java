package ru.bpc.cm.cashmanagement.orm;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

import ru.bpc.cm.cashmanagement.NotifyMessageController;
import ru.bpc.cm.cashmanagement.orm.builders.NotifyMessageBuilder;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.items.cashmanagement.NotifyMessageItem;

/**
 * Интерфейс-маппер для класса {@link NotifyMessageController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 24.02.2017
 * @version 1.0.0
 *
 */
public interface NotifyMessageMapper extends IMapper {

	@InsertProvider(type = NotifyMessageBuilder.class, method = "insertNotifyMessageBuilder")
	void insertNotifyMessage(@Param("nextSeq") String nextSeq, @Param("currTime") Timestamp currTime,
			@Param("userId") int userId, @Param("msgId") int msgId,
			@Param("printedCollection") String printedCollection);

	@ConstructorArgs({
		@Arg(column = "CREATE_DATE", javaType = Timestamp.class),
		@Arg(column = "IS_NEW", javaType = Boolean.class),
		@Arg(column = "ID", javaType = Integer.class),
		@Arg(column = "MESSAGE_TYPE", javaType = Integer.class),
		@Arg(column = "PARAMS", javaType = String.class)
	})
	@SelectProvider(type = NotifyMessageBuilder.class, method = "getNotifyMessageListBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<NotifyMessageItem> getNotifyMessageList(@Param("userId") Integer userId, @Param("newOnly") Boolean newOnly,
			@Param("dateFrom") Date dateFrom);

	@Result(column = "CNT", javaType = Boolean.class)
	@Select("SELECT count(1) as CNT FROM t_cm_notification_message WHERE USER_ID = #{userId} and IS_NEW = 1 ")
	boolean getNewNotifyMessagesCheck(@Param("userId") Integer userId);

	@Update("UPDATE t_cm_notification_message SET IS_NEW = 0 WHERE ID = #{msgId} ")
	void updateNotifyMessage(@Param("msgId") Integer msgId);
}
