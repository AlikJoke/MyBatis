package ru.bpc.cm.cashmanagement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.ejb.EJBException;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.orm.AtmGroupListMapper;
import ru.bpc.cm.items.enums.AtmGroupType;
import ru.bpc.cm.items.settings.AtmGroupItem;
import ru.bpc.cm.items.settings.GroupFilter;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.Pair;
import ru.bpc.cm.utils.db.DbTextUtil;
import ru.bpc.cm.utils.db.JdbcUtils;
import ru.bpc.cm.utils.db.QueryConstructor;

public class AtmGroupListController {

	private static final Logger _logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<AtmGroupListMapper> getMapperClass() {
		return AtmGroupListMapper.class;
	}

	public static List<IFilterItem<Integer>> getFullGroupList(ISessionHolder sessionHolder) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<IFilterItem<Integer>> datalist = new ArrayList<IFilterItem<Integer>>();

		try {
			datalist.addAll(session.getMapper(getMapperClass()).getFullGroupList(AtmGroupType.USER_TO_ATM.getId()));
			return datalist;
		} catch (Exception e) {
			_logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

	public static List<IFilterItem<Integer>> getFullAttrGroupList(ISessionHolder sessionHolder) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<IFilterItem<Integer>> datalist = new ArrayList<IFilterItem<Integer>>();

		try {
			datalist.addAll(session.getMapper(getMapperClass()).getFullGroupList(AtmGroupType.USER_TO_ATM.getId()));
			return datalist;
		} catch (Exception e) {
			_logger.error("", e);
			throw new EJBException(e);
		} finally {
			session.close();
		}
	}

	public static List<AtmGroupItem> getAtmGroupList(ISessionHolder sessionHolder, String personid,
			boolean fetchSubordinatesGroups) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmGroupItem> atmGroupList = new ArrayList<AtmGroupItem>();
		try {
			atmGroupList.addAll(session.getMapper(getMapperClass()).getAtmGroupList(personid,
					AtmGroupType.USER_TO_ATM.getId(), fetchSubordinatesGroups));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupList;
	}

	public static List<AtmGroupItem> getAtmGroupList(ISessionHolder sessionHolder, GroupFilter filter) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmGroupItem> atmGroupList = new ArrayList<AtmGroupItem>();
		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			connection = session.getConnection();
			String sql = "SELECT t.ID,t.NAME, t.DESCRIPTION,t.TYPE_ID " + "FROM T_CM_ATM_GROUP t " + "WHERE t.ID in ( "
					+ "SELECT tg.ID FROM T_CM_ATM_GROUP tg  "
					+ "left outer join T_CM_ATM2ATM_GROUP tr on ( tg.ID = tr.ATM_GROUP_ID ) "
					+ "left outer join T_CM_ATM ta on (tr.ATM_ID = ta.ATM_ID) " + "where 1=1 "
					+ "AND tg.ID in (select ID from T_CM_TEMP_ATM_GROUP_LIST) ";
			QueryConstructor querConstr = new QueryConstructor();
			querConstr.setQueryBody(sql, true);
			querConstr.addElementIfNotNull("atm_id", "AND", "tr.atm_id", DbTextUtil.getOperationType(filter.getAtmID()),
					filter.getAtmID());
			querConstr.addElementIfNotNull("atm_name", "AND", "ta.name",
					DbTextUtil.getOperationType(filter.getAtmName()), filter.getAtmName());
			querConstr.addElementIfNotNull("group_name", "AND", "tg.name",
					DbTextUtil.getOperationType(filter.getGroupName()), filter.getGroupName());
			querConstr.addElementIfNotNull("group_id", "AND", "tg.ID", DbTextUtil.getOperationType(filter.getGroupID()),
					filter.getGroupID());
			querConstr.addElementIfNotNull("type2", "AND", "tg.TYPE_ID", "=", AtmGroupType.USER_TO_ATM.getId());
			querConstr.setQueryTail(" ) ORDER BY t.NAME");
			// from session
			pstmt = connection.prepareStatement(querConstr.getQuery());
			querConstr.updateQueryParameters(pstmt);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				AtmGroupItem item = new AtmGroupItem();
				item.setAtmGroupID(rs.getInt("ID"));
				item.setName(rs.getString("NAME"));
				item.setDescx(rs.getString("DESCRIPTION"));
				item.setType(rs.getInt("TYPE_ID"));
				atmGroupList.add(item);
			}
		} catch (SQLException e) {
			_logger.error("", e);
		} finally {
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
			JdbcUtils.close(connection);
			session.close();
		}
		return atmGroupList;
	}

	public static List<Integer> getAtmGroupIdList(ISessionHolder sessionHolder, String personid,
			boolean fetchSubordinatesGroups) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Integer> atmGroupList = new ArrayList<Integer>();
		try {
			atmGroupList.addAll(session.getMapper(getMapperClass()).getAtmGroupIdList(personid,
					AtmGroupType.USER_TO_ATM.getId(), fetchSubordinatesGroups));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupList;
	}

	public static List<Pair> getAtmGroupListForAtm(ISessionHolder sessionHolder, String personid, int atmID,
			boolean fetchSubordinatesGroups) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmGroupListForAtm = new ArrayList<Pair>();
		try {
			atmGroupListForAtm.addAll(session.getMapper(getMapperClass()).getAtmGroupListForAtm(personid,
					AtmGroupType.USER_TO_ATM.getId(), fetchSubordinatesGroups, atmID));
		} catch (Exception e) {
			_logger.error("", e);
		} finally {
			session.close();
		}
		return atmGroupListForAtm;
	}
}
