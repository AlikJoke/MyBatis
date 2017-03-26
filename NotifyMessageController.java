package ru.bpc.cm.cashmanagement;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.orm.NotifyMessageMapper;
import ru.bpc.cm.config.utils.ORMUtils;
import ru.bpc.cm.items.cashmanagement.NotifyMessageItem;
import ru.bpc.cm.items.enums.NotifyMessageType;
import ru.bpc.cm.items.settings.AtmUserItem;
import ru.bpc.cm.utils.db.JdbcUtils;
import ru.bpc.structs.collection.SeparatePrintedCollection;

public class NotifyMessageController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<NotifyMessageMapper> getMapperClass() {
		return NotifyMessageMapper.class;
	}

	public static void insertNotifyMessage(ISessionHolder sessionHolder, NotifyMessageType msgType, int userId,
			String... params) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			NotifyMessageMapper mapper = session.getMapper(getMapperClass());
			mapper.insertNotifyMessage(ORMUtils.getNextSequence(session, "s_notify_msg_id"),
					new Timestamp(new Date().getTime()), userId, msgType.getId(),
					new SeparatePrintedCollection<String>(Arrays.asList(params)).toString());
			mapper.flush();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void insertNotifyMessage(ISessionHolder sessionHolder, NotifyMessageType msgType,
			List<AtmUserItem> userIds, String... params) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			NotifyMessageMapper mapper = session.getMapper(getMapperClass());
			String nextSequence = ORMUtils.getNextSequence(session, "s_notify_msg_id");
			String collection = new SeparatePrintedCollection<String>(Arrays.asList(params)).toString();

			for (AtmUserItem userId : userIds)
				mapper.insertNotifyMessage(nextSequence, new Timestamp(new Date().getTime()), userId.getId(),
						msgType.getId(), collection);
			mapper.flush();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static List<NotifyMessageItem> getNotifyMessageList(ISessionHolder sessionHolder, int personId,
			Date dateFrom, boolean newOnly) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<NotifyMessageItem> notifyMessageList = new ArrayList<NotifyMessageItem>();
		try {
			notifyMessageList.addAll(session.getMapper(getMapperClass()).getNotifyMessageList(personId, newOnly,
					JdbcUtils.getSqlDate(dateFrom)));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return notifyMessageList;
	}

	public static boolean getNewNotifyMessagesCheck(ISessionHolder sessionHolder, int personId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Boolean cnt = session.getMapper(getMapperClass()).getNewNotifyMessagesCheck(personId);
			if (cnt != null)
				return cnt;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return false;
	}

	public static void updateNotifyMessage(ISessionHolder sessionHolder, int msgId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			NotifyMessageMapper mapper = session.getMapper(getMapperClass());
			mapper.updateNotifyMessage(msgId);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}
}
