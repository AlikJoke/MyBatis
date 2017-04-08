package ru.bpc.cm.cashmanagement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJBException;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.orm.AtmPidListMapper;
import ru.bpc.cm.cashmanagement.orm.items.AtmGroupItem;
import ru.bpc.cm.items.enums.AtmGroupType;
import ru.bpc.cm.items.reports.ReportFilter;
import ru.bpc.cm.items.settings.AtmGroupAtmItem;
import ru.bpc.cm.items.settings.calendars.CalendarAtmItem;
import ru.bpc.cm.management.AtmGroup;
import ru.bpc.cm.management.Institute;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.IntFilterItem;
import ru.bpc.cm.utils.Pair;

public class AtmPidListController {

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<AtmPidListMapper> getMapperClass() {
		return AtmPidListMapper.class;
	}

	public static List<IFilterItem<Integer>> getAtmListFull(ISessionHolder sessionHolder) {
		List<IFilterItem<Integer>> result = new ArrayList<IFilterItem<Integer>>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			result.addAll(session.getMapper(getMapperClass()).getAtmListFull());
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	/**
	 * Retrieves Map containing user-filtered ATMs splitted by user-filtered
	 * groups. Currently used on reports forms.
	 */
	public static Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> getAtmListSplitByGroupList(
			ISessionHolder sessionHolder) {
		Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> result = new LinkedHashMap<IFilterItem<Integer>, List<IFilterItem<Integer>>>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<ru.bpc.cm.cashmanagement.orm.items.AtmGroupItem> atmsList = session.getMapper(getMapperClass())
					.getAtmListSplitByGroupList();

			int currentGroupId = 0;
			String lastGroupDescription = null;
			List<IFilterItem<Integer>> atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
			for (ru.bpc.cm.cashmanagement.orm.items.AtmGroupItem atm : atmsList) {
				int rsGroupId = atm.getAtmGroupId();
				if (currentGroupId != rsGroupId) {
					if (currentGroupId != 0)
						result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);

					currentGroupId = rsGroupId;
					atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
				}
				atmsOfGroup.add(new IntFilterItem(atm.getAtmId(), atm.getExtAtmId(), atm.getAtmName()));
				lastGroupDescription = atm.getAtmGroupName();
			}
			if (currentGroupId > 0)
				result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	/**
	 * Retrieves Map containing user-filtered ATMs splitted by user-filtered
	 * groups. Currently used on reports forms.
	 */
	public static Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> getAtmListSplitByGroupListDescx(
			ISessionHolder sessionHolder) {
		Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> result = new LinkedHashMap<IFilterItem<Integer>, List<IFilterItem<Integer>>>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<AtmGroupItem> atmsList = session.getMapper(getMapperClass()).getAtmListSplitByGroupListDescx();

			int currentGroupId = 0;
			String lastGroupDescription = null;
			List<IFilterItem<Integer>> atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
			for (AtmGroupItem atm : atmsList) {
				int rsGroupId = atm.getAtmGroupId();
				if (currentGroupId != rsGroupId) {
					if (currentGroupId != 0)
						result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);
					currentGroupId = rsGroupId;
					atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
				}
				atmsOfGroup.add(new IntFilterItem(atm.getAtmId(), atm.getExtAtmId(), atm.getAtmName()));
				lastGroupDescription = atm.getAtmGroupName();
			}
			if (currentGroupId != 0)
				result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	/**
	 * Retrieves Map containing user-filtered ATMs splitted by user-filtered
	 * groups. Atms are filtered by id and Name/Address. Currently used on
	 * reports forms.
	 * 
	 * @param nameAndAddressFilter
	 */
	public static Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> getAtmListSplitByGroupListDescx(
			ISessionHolder sessionHolder, String atmIdFilter, String nameAndAddressFilter) {
		Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> result = new LinkedHashMap<IFilterItem<Integer>, List<IFilterItem<Integer>>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<AtmGroupItem> atmsList = session.getMapper(getMapperClass()).getAtmListSplitByGroupListDescx2(
					atmIdFilter == null ? null : Integer.valueOf(atmIdFilter), nameAndAddressFilter);
			int currentGroupId = 0;
			String lastGroupDescription = null;
			List<IFilterItem<Integer>> atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
			for (AtmGroupItem atm : atmsList) {
				int rsGroupId = atm.getAtmGroupId();
				if (currentGroupId != rsGroupId) {
					if (currentGroupId != 0)
						result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);
					currentGroupId = rsGroupId;
					atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
				}
				atmsOfGroup.add(new IntFilterItem(atm.getAtmId(), atm.getExtAtmId(), atm.getAtmName()));
				lastGroupDescription = atm.getAtmGroupName();
			}
			if (currentGroupId != 0)
				result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	/**
	 * Retrieves Map containing user-filtered ATMs splitted by user-filtered
	 * groups. Atms are filtered by id and Name/Address. Currently used on
	 * reports forms.
	 * 
	 * @param nameAndAddressFilter
	 */
	public static Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> getAtmListSplitByGroupListDescx(
			ISessionHolder sessionHolder, ReportFilter repFilter) {
		Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> result = new LinkedHashMap<IFilterItem<Integer>, List<IFilterItem<Integer>>>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			List<AtmGroupItem> items = mapper.getAtmListSplitByGroupListDescx2(
					repFilter.getAtmId() == null ? null : Integer.valueOf(repFilter.getAtmId()),
					repFilter.getNameAndAddress());

			int currentGroupId = 0;
			String lastGroupDescription = null;
			List<IFilterItem<Integer>> atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
			for (AtmGroupItem item : items) {
				int rsGroupId = item.getAtmGroupId();
				if (currentGroupId != rsGroupId) {
					if (currentGroupId != 0)
						result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);

					currentGroupId = rsGroupId;
					atmsOfGroup = new ArrayList<IFilterItem<Integer>>();
				}
				atmsOfGroup.add(new IntFilterItem(item.getAtmId(), item.getExtAtmId(), item.getAtmName()));
				lastGroupDescription = item.getAtmGroupName();
			}
			if (currentGroupId != 0)
				result.put(new IntFilterItem(currentGroupId, lastGroupDescription), atmsOfGroup);
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	/**
	 * Retrieves Map containing all ATMs splitted by all groups. Currently used
	 * on ATM User form to fetch ATM list for Global Admininstrator
	 */
	public static Map<String, List<String>> getAtmListSplitByGroup(ISessionHolder sessionHolder) {
		Map<String, List<String>> result = new LinkedHashMap<String, List<String>>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			List<AtmGroupItem> items = mapper.getAtmListSplitByGroupForAdm(AtmGroupType.USER_TO_ATM.getId());

			int currentGroupId = 0;
			String lastGroupDescription = null;
			List<String> atmsOfGroup = new ArrayList<String>();
			for (AtmGroupItem item : items) {
				int rsGroupId = item.getAtmGroupId();
				if (currentGroupId != rsGroupId) {
					if (currentGroupId != 0) {
						result.put(lastGroupDescription, atmsOfGroup);
					}
					currentGroupId = rsGroupId;
					atmsOfGroup = new ArrayList<String>();
				}
				atmsOfGroup.add(CmUtils.getAtmNameLabel(item.getExtAtmId(), item.getAtmName()));
				lastGroupDescription = item.getAtmGroupName();
			}
			if (currentGroupId > 0)
				result.put(lastGroupDescription, atmsOfGroup);
			return result;
		} catch (Exception e) {
			_logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

	/**
	 * Retrieves Map containing all ATMs splitted by all groups
	 */
	public static Map<String, List<String>> getAtmListSplitByGroup(ISessionHolder sessionHolder,
			List<Institute> instList, boolean filterGroupsList) {
		Map<String, List<String>> result = new LinkedHashMap<String, List<String>>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			List<AtmGroupItem> items = mapper.getAtmListSplitByGroup(filterGroupsList, instList,
					AtmGroupType.USER_TO_ATM.getId());

			int currentGroupId = 0;
			String lastGroupDescription = null;
			List<String> atmsOfGroup = new ArrayList<String>();
			for (AtmGroupItem item : items) {
				int rsGroupId = item.getAtmGroupId();
				if (currentGroupId != rsGroupId) {
					if (currentGroupId != 0) {
						result.put(lastGroupDescription, atmsOfGroup);
					}
					currentGroupId = rsGroupId;
					atmsOfGroup = new ArrayList<String>();
				}
				atmsOfGroup.add(CmUtils.getAtmNameLabel(item.getExtAtmId(), item.getAtmName()));
				lastGroupDescription = item.getAtmGroupName();
			}
			if (currentGroupId > 0)
				result.put(lastGroupDescription, atmsOfGroup);
			return result;
		} catch (Exception e) {
			_logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

	public static List<IFilterItem<Integer>> getAtmListForGroupList(ISessionHolder sessionHolder,
			boolean sortByOutOfCurrDate) {
		List<IFilterItem<Integer>> result = new ArrayList<IFilterItem<Integer>>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			result.addAll(mapper.getAtmListForGroupList(sortByOutOfCurrDate));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	public static List<IFilterItem<Integer>> getAtmListForGroupList(ISessionHolder sessionHolder,
			List<Institute> instList, boolean sortByOutOfCurrDate) {
		List<IFilterItem<Integer>> result = new ArrayList<IFilterItem<Integer>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			result.addAll(mapper.getAtmListForGroupList(sortByOutOfCurrDate, instList));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	public static List<Integer> getAtmListForGroupList(ISessionHolder sessionHolder, String atmGroups) {
		List<Integer> result = new ArrayList<Integer>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			result.addAll(mapper.getAtmListForGroupList(atmGroups));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return result;
	}

	public static List<AtmGroupAtmItem> getAtmListForGroup(ISessionHolder sessionHolder, int groupId) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList.addAll(mapper.getAtmListForGroup(groupId));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	public static List<AtmGroupAtmItem> getAtmListForGroup(ISessionHolder sessionHolder, int groupId,
			List<Institute> instList) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList.addAll(mapper.getAtmListForGroup(groupId, instList));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	public static List<IFilterItem<Integer>> getAtmListForUser(ISessionHolder sessionHolder, String personid) {
		List<IFilterItem<Integer>> atmList = new ArrayList<IFilterItem<Integer>>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmList.addAll(mapper.getAtmListForUser(personid, AtmGroupType.USER_TO_ATM.getId()));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmList;
	}

	public static List<AtmGroupAtmItem> getAtmListFull(ISessionHolder sessionHolder, String personid, int atmGroupID) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList.addAll(mapper.getAtmListFull(personid, AtmGroupType.USER_TO_ATM.getId(), atmGroupID));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	public static List<AtmGroupAtmItem> getAtmListFull(ISessionHolder sessionHolder, String personid, int atmGroupID,
			List<Institute> instList) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList
					.addAll(mapper.getAtmListFull(personid, AtmGroupType.USER_TO_ATM.getId(), atmGroupID, instList));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	public static List<AtmGroupAtmItem> getAvaliableAtmsListForGroup(ISessionHolder sessionHolder, String personid,
			int atmGroupID) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList.addAll(mapper.getAvaliableAtmsListForGroup(atmGroupID, AtmGroupType.USER_TO_ATM.getId()));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	public static List<AtmGroupAtmItem> getAvaliableAtmsListForGroup(ISessionHolder sessionHolder, String personid,
			int atmGroupID, List<Institute> instList) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList.addAll(
					mapper.getAvaliableAtmsListForGroup(atmGroupID, AtmGroupType.USER_TO_ATM.getId(), instList));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	public static List<AtmGroupAtmItem> getAvaliableAtmsListForAttributeGroup(ISessionHolder sessionHolder,
			String personid, int atmGroupID) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList
					.addAll(mapper.getAvaliableAtmsListForAttributeGroup(atmGroupID, AtmGroupType.USER_TO_ATM.getId()));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	public static List<AtmGroupAtmItem> getAvaliableAtmsListForAttributeGroup(ISessionHolder sessionHolder,
			String personid, int atmGroupID, List<Institute> instList) {
		List<AtmGroupAtmItem> atmGroupAtmList = new ArrayList<AtmGroupAtmItem>();
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			atmGroupAtmList.addAll(mapper.getAvaliableAtmsListForAttributeGroup(atmGroupID,
					AtmGroupType.USER_TO_ATM.getId(), instList));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupAtmList;
	}

	/**
	 * Retrieves List containing all ATMs with ATM's calendars. Currently used
	 * on Calendars form to fetch ATM list for Global Admininstrator
	 */
	public static List<CalendarAtmItem> getCalendarAtmList(ISessionHolder sessionHolder) {
		List<CalendarAtmItem> result = new ArrayList<CalendarAtmItem>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			result.addAll(mapper.getCalendarAtmList());
			return result;
		} catch (Exception e) {
			_logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

	/**
	 * Retrieves List containing all ATMs with ATM's calendars. Currently used
	 * on Calendars form to fetch ATM list for Local Administrator
	 */
	public static List<CalendarAtmItem> getCalendarAtmList(ISessionHolder sessionHolder, List<Institute> instList) {
		List<CalendarAtmItem> result = new ArrayList<CalendarAtmItem>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			result.addAll(mapper.getCalendarAtmList_Adm(instList));
			return result;
		} catch (Exception e) {
			_logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

	/**
	 * Retrieves List containing all ATMs with ATM's calendars. Currently used
	 * on Calendars form to fetch ATM list for User
	 */
	public static List<CalendarAtmItem> getCalendarAtmList(ISessionHolder sessionHolder, List<Institute> instList,
			List<AtmGroup> groupList) {
		List<CalendarAtmItem> result = new ArrayList<CalendarAtmItem>();

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			result.addAll(mapper.getCalendarAtmList_User(instList));
			return result;
		} catch (Exception e) {
			_logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

	public static void saveAtmGroupAtms(ISessionHolder sessionHolder, ru.bpc.cm.items.settings.AtmGroupItem group,
			List<AtmGroupAtmItem> selectedAtms) {
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			mapper.deleteAtmGroupAtms(group.getAtmGroupID());
			mapper.flush();

			for (AtmGroupAtmItem item : selectedAtms)
				mapper.saveAtmGroupAtms(group.getAtmGroupID(), item.getAtmID());

			mapper.flush();
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void saveAtmGroupAtms(ISessionHolder sessionHolder, ru.bpc.cm.items.settings.AtmGroupItem group,
			List<AtmGroupAtmItem> selectedAtms, List<AtmGroupAtmItem> groupAtms) {
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			mapper.deleteAtmGroupAtmsTemp(group.getAtmGroupID());
			mapper.flush();

			for (AtmGroupAtmItem item : selectedAtms)
				mapper.saveAtmGroupAtmsTemp(group.getAtmGroupID(), item.getAtmID());

			mapper.flush();
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static List<Pair> saveAttributeGroupAtms(ISessionHolder sessionHolder,
			ru.bpc.cm.items.settings.AtmGroupItem group, List<AtmGroupAtmItem> selectedAtms) {
		List<Pair> checkFailedAtms = new ArrayList<Pair>();
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			String descx = null;

			for (AtmGroupAtmItem item : selectedAtms) {
				descx = mapper.getDescx_saveAttributeGroupAtms(item.getAtmID(), group.getAtmGroupID(), group.getType());
				if (descx != null)
					checkFailedAtms.add(new Pair(String.valueOf(item.getAtmID()), descx));
			}

			if (checkFailedAtms.size() == 0) {
				mapper.delete_saveAttributeGroupAtms(group.getAtmGroupID());
				mapper.flush();

				for (AtmGroupAtmItem item : selectedAtms)
					mapper.saveAtmGroupAtms(group.getAtmGroupID(), item.getAtmID());

				mapper.flush();
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return checkFailedAtms;
	}

	public static List<Pair> saveAttributeGroupAtms(ISessionHolder sessionHolder,
			ru.bpc.cm.items.settings.AtmGroupItem group, List<AtmGroupAtmItem> selectedAtms,
			List<AtmGroupAtmItem> groupAtms) {
		List<Pair> checkFailedAtms = new ArrayList<Pair>();
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			AtmPidListMapper mapper = session.getMapper(getMapperClass());
			String descx = null;
			for (AtmGroupAtmItem item : selectedAtms) {
				descx = mapper.getDescx_saveAttributeGroupAtms(item.getAtmID(), group.getAtmGroupID(), group.getType());
				if (descx != null)
					checkFailedAtms.add(new Pair(String.valueOf(item.getAtmID()), descx));
			}

			if (checkFailedAtms.size() == 0) {
				mapper.delete_saveAttributeGroupAtmsTemp(group.getAtmGroupID());
				mapper.flush();

				for (AtmGroupAtmItem item : selectedAtms)
					mapper.saveAtmGroupAtmsTemp(group.getAtmGroupID(), item.getAtmID());

				mapper.flush();
			}
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return checkFailedAtms;
	}

}
