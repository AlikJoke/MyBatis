package ru.bpc.cm.tasks;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.ejb.EJBException;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.audit.event.AuditActionTypes;
import ru.bpc.audit.logger.AuditException;
import ru.bpc.audit.logger.AuditLogger;
import ru.bpc.audit.utils.event.EventBuilder;
import ru.bpc.cm.Operation;
import ru.bpc.cm.items.audit.WebTaskWrapper;
import ru.bpc.cm.items.enums.AtmGroupType;
import ru.bpc.cm.items.enums.WebTaskParamType;
import ru.bpc.cm.items.enums.WebTaskParamValueType;
import ru.bpc.cm.items.tasks.BooleanParamItem;
import ru.bpc.cm.items.tasks.DateParamItem;
import ru.bpc.cm.items.tasks.ListParamItem;
import ru.bpc.cm.items.tasks.LongParamItem;
import ru.bpc.cm.items.tasks.SelectOneParamItem;
import ru.bpc.cm.items.tasks.StackParamItem;
import ru.bpc.cm.items.tasks.StringParamItem;
import ru.bpc.cm.items.tasks.WebTaskFilter;
import ru.bpc.cm.items.tasks.WebTaskItem;
import ru.bpc.cm.items.tasks.WebTaskParamItem;
import ru.bpc.cm.orm.common.ORMUtils;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.tasks.orm.WebTaskMapper;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.IntFilterItem;

public class WebTaskController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<WebTaskMapper> getMapperClass() {
		return WebTaskMapper.class;
	}

	public static List<WebTaskItem> getWebTaskFullList(ISessionHolder sessionHolder) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<WebTaskItem> webTaskList = new ArrayList<WebTaskItem>();
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			webTaskList.addAll(mapper.getWebTaskFullList());
			for (WebTaskItem item : webTaskList) {
				item.convertFormIntType();
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return webTaskList;
	}

	public static List<WebTaskItem> getWebTaskList(ISessionHolder sessionHolder, WebTaskFilter filter) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<WebTaskItem> webTaskList = new ArrayList<WebTaskItem>();
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			mapper.getWebTaskList(filter.getIntType());
			webTaskList.addAll(mapper.getWebTaskFullList());
			for (WebTaskItem item : webTaskList) {
				item.convertFormIntType();
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return webTaskList;
	}

	public static List<WebTaskParamItem> getWebTaskParamList(ISessionHolder sessionHolder, int taskId) {
		List<WebTaskParamItem> webTaskParamList = new ArrayList<WebTaskParamItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			List<TripleObject<Integer, String, Integer>> paramList = mapper.getWebTaskParamList(taskId);

			for (TripleObject<Integer, String, Integer> param : paramList) {
				int typeKey = param.getThird();

				WebTaskParamValueType paramValueType = WebTaskParamType.getWebTaskParamValueTypeByKey(typeKey);
				WebTaskParamType paramType = WebTaskParamType.getWebTaskParamTypeByKey(typeKey);

				switch (paramValueType) {
				case SELECT_STACK_VALUES:
					StackParamItem stackListItem = new StackParamItem();
					stackListItem.setTaskId(param.getFirst());
					stackListItem.setType(paramType);
					stackListItem.setSelectItems(
							WebTaskParamValueHolder.getSelectItemsForTaskParam(sessionHolder, paramType));

					stackListItem.setSelectedValuesSimple(CmUtils.getIdListFromString(param.getSecond()));
					for (IFilterItem<Integer> selectedValue : stackListItem.getSelectedValues()) {
						((IntFilterItem) selectedValue)
								.setLabel(stackListItem.getLabelsMap().get(selectedValue.getValue()));
					}
					webTaskParamList.add(stackListItem);
					break;
				case SELECT_LIST_VALUES:
					ListParamItem listItem = new ListParamItem();
					listItem.setTaskId(param.getFirst());
					listItem.setType(paramType);
					listItem.setSelectItems(
							WebTaskParamValueHolder.getSelectItemsForTaskParam(sessionHolder, paramType));
					listItem.setSelectedValues(CmUtils.getIdListFromString(param.getSecond()));
					webTaskParamList.add(listItem);
					break;
				case SELECT_ONE_VALUE:
					SelectOneParamItem periodItem = new SelectOneParamItem();
					periodItem.setTaskId(param.getFirst());
					periodItem.setType(paramType);
					periodItem.setSelectItems(
							WebTaskParamValueHolder.getSelectItemsForTaskParam(sessionHolder, paramType));
					periodItem.setSelectedValue(Integer.parseInt(param.getSecond()));
					webTaskParamList.add(periodItem);
					break;
				case STRING_VALUE:
					StringParamItem stringItem = new StringParamItem();
					stringItem.setTaskId(param.getFirst());
					stringItem.setType(paramType);
					stringItem.setValue(param.getSecond());
					webTaskParamList.add(stringItem);
					break;
				case BOOLEAN_VALUE:
					BooleanParamItem booleanItem = new BooleanParamItem();
					booleanItem.setTaskId(param.getFirst());
					booleanItem.setType(paramType);
					booleanItem.setValue(param.getSecond());
					webTaskParamList.add(booleanItem);
					break;
				case DATE_VALUE:
					DateParamItem dateItem = new DateParamItem();
					dateItem.setTaskId(param.getFirst());
					dateItem.setType(paramType);
					dateItem.setValue(param.getSecond());
					webTaskParamList.add(dateItem);
					break;
				case LONG_VALUE:
					LongParamItem longItem = new LongParamItem();
					longItem.setTaskId(param.getFirst());
					longItem.setType(paramType);
					longItem.setValue(param.getSecond());
					webTaskParamList.add(longItem);
					break;
				default:
					break;
				}
			}
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return webTaskParamList;
	}

	public static int getWebTaskUserId(ISessionHolder sessionHolder, int taskId) {
		int webTaskUserId = 0;
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			List<Integer> userIds = mapper.getWebTaskUserId(taskId);
			webTaskUserId = ORMUtils.getSingleValue(userIds, 0);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return webTaskUserId;
	}

	public static List<WebTaskParamItem> getWebTaskParamStringList(ISessionHolder sessionHolder, int taskId) {
		List<WebTaskParamItem> webTaskParamList = new ArrayList<WebTaskParamItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			List<TripleObject<Integer, String, Integer>> paramList = mapper.getWebTaskParamList(taskId);
			for (TripleObject<Integer, String, Integer> param : paramList) {
				StringParamItem item = new StringParamItem();
				item.setTaskId(param.getFirst());
				item.setType(WebTaskParamType.getWebTaskParamTypeByKey(param.getThird()));
				item.setValue(param.getSecond());
				webTaskParamList.add(item);
			}
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			session.close();
		}
		return webTaskParamList;
	}

	public static void setLastExecTime(ISessionHolder sessionHolder, int taskId, long lastExecTime) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			mapper.setLastExecTime(taskId, new Timestamp(lastExecTime));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static Long getLastExecTime(ISessionHolder sessionHolder, int taskId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Timestamp result = null;
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			result = mapper.getLastExecTime(taskId);
			if (result != null) {
				return result.getTime();
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return null;
	}

	public static WebTaskItem changeWebTask(ISessionHolder sessionHolder, WebTaskItem task,
			List<WebTaskParamItem> params, Operation operation, int userId, String userLogin)
			throws SQLException, AuditException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			switch (operation) {
			case INSERT:
				int taskId = ORMUtils.getSingleValue(mapper.getTaskIdForChangeWebTask(session), 0);
				WebTaskWrapper objAfter = new WebTaskWrapper(task, params);

				AuditLogger.sendAsynchronously(EventBuilder.buildEventFromWrappedObjects(userLogin,
						AuditActionTypes.ADD, String.valueOf(taskId), null, objAfter));

				mapper.changeWebTask_insert(taskId, task.getCron(), task.getType().getWebTaskType(), task.getDescx(),
						task.getName(), userId);

				insertWebTaskParams(sessionHolder, taskId, params);

				task.setTaskId(taskId);
				break;
			case DELETE:
				mapper.changeWebTask_deleteFromTaskParam(task.getTaskId());
				mapper.changeWebTask_deleteFromTask(task.getTaskId());
				break;
			case UPDATE:
				mapper.changeWebTask_deleteFromTaskParam(task.getTaskId());
				insertWebTaskParams(sessionHolder, task.getTaskId(), params);
				mapper.changeWebTask_update(task.getTaskId(), task.getCron(), task.getDescx(), task.getName());
				break;
			}
		} finally {
			session.close();
		}
		return task;
	}

	public static void insertWebTaskParams(ISessionHolder sessionHolder, int taskId, List<WebTaskParamItem> params) {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			for (WebTaskParamItem item : params) {
				mapper.insertWebTaskParams(taskId, item.getValue(), item.getType().getWebTaskParamType());
			}
			mapper.flush();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void updateWebTaskParams(ISessionHolder sessionHolder, int taskId, List<WebTaskParamItem> params) {
		SqlSession session = sessionHolder.getBatchSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			for (WebTaskParamItem item : params) {
				mapper.updateWebTaskParams(taskId, item.getValue(), item.getType().getWebTaskParamType());
			}
			mapper.flush();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static List<IntFilterItem> getGroupListParam(ISessionHolder sessionHolder, String groupIdParam) {
		List<IntFilterItem> datalist = new ArrayList<IntFilterItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			WebTaskMapper mapper = session.getMapper(getMapperClass());
			datalist.addAll(mapper.getGroupListParam(AtmGroupType.USER_TO_ATM.getId()));
			return datalist;
		} catch (Exception e) {
			logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

}
