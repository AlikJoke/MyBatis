package ru.bpc.cm.monitoring;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.apache.ibatis.session.SqlSession;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.items.enums.EncashmentActionType;
import ru.bpc.cm.monitoring.orm.GroupActualStateMapper;
import ru.bpc.cm.utils.Pair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class GroupStateController {

	private static final Class<GroupActualStateMapper> getMapperClass() {
		return GroupActualStateMapper.class;
	}

	protected static List<Pair> getAtmEncPlanSums(ISessionHolder sessionHolder, int atmId, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());
			atmEncashmentCassList = Optional.ofNullable(mapper.getAtmEncPlanSums(JdbcUtils.getSqlDate(date), atmId))
					.orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getAtmEncPeriodSums(ISessionHolder sessionHolder, int atmId, Date dateFrom,
			Date dateTo) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());

			atmEncashmentCassList = Optional.ofNullable(
					mapper.getAtmEncPeriodSums(JdbcUtils.getSqlDate(dateFrom), JdbcUtils.getSqlDate(dateFrom), atmId))
					.orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getAtmRemainingSums(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());

			atmEncashmentCassList = Optional.ofNullable(mapper.getAtmRemainingSums(atmId))
					.orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getAtmEncStatSums(ISessionHolder sessionHolder, int atmId, Date date,
			EncashmentActionType actionType) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());

			atmEncashmentCassList = Optional
					.ofNullable(mapper.getAtmEncStatSums(JdbcUtils.getSqlDate(date), atmId, actionType.getId()))
					.orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getAtmsEncPlanSums(ISessionHolder sessionHolder, Date date) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());

			atmEncashmentCassList = Optional.ofNullable(mapper.getAtmsEncPlanSums(JdbcUtils.getSqlDate(date)))
					.orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getAtmsEncPeriodSums(ISessionHolder sessionHolder, Date dateFrom, Date dateTo) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());

			atmEncashmentCassList = Optional
					.ofNullable(
							mapper.getAtmsEncPeriodSums(JdbcUtils.getSqlDate(dateFrom), JdbcUtils.getSqlDate(dateTo)))
					.orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getAtmsRemainingSums(ISessionHolder sessionHolder) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());

			atmEncashmentCassList = Optional.ofNullable(mapper.getAtmsRemainingSums()).orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

	protected static List<Pair> getAtmsEncStatSums(ISessionHolder sessionHolder, Date date,
			EncashmentActionType actionType) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Pair> atmEncashmentCassList = new ArrayList<Pair>();
		try {
			GroupActualStateMapper mapper = sessionHolder.getMapper(session, getMapperClass());

			atmEncashmentCassList = Optional
					.ofNullable(mapper.getAtmsEncStatSums(JdbcUtils.getSqlDate(date), actionType.getId()))
					.orElse(new ArrayList<Pair>());
		} finally {
			session.close();
		}
		return atmEncashmentCassList;
	}

}
