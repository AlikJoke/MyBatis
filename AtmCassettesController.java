package ru.bpc.cm.cashmanagement;

import java.util.ArrayList;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.orm.AtmCassettesMapper;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;

public class AtmCassettesController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<AtmCassettesMapper> getMapperClass() {
		return AtmCassettesMapper.class;
	}

	public static int getAtmCassCount(ISessionHolder sessionHolder, int atmId, AtmCassetteType cassType) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Integer count = 0;
		try {
			count = session.getMapper(getMapperClass()).getAtmCassCount(atmId, cassType.getId());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return count;
	}

	public static List<AtmCassetteItem> getAtmCassettes(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<AtmCassetteItem> atmCassList = new ArrayList<AtmCassetteItem>();
		try {
			atmCassList.addAll(session.getMapper(getMapperClass()).getAtmCassettes(atmId));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return atmCassList;
	}

	public static void saveAtmCassettes(ISessionHolder sessionHolder, int atmId, List<AtmCassetteItem> atmCassList) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmCassettesMapper mapper = session.getMapper(getMapperClass());
			mapper.deleteAtmCassettes(atmId);
			mapper.flush();
			
			boolean cashInInserted = false;
			for (AtmCassetteItem cass : atmCassList) {
				if (cass.getType() == AtmCassetteType.CASH_IN_CASS) {
					if (cashInInserted) {
						continue;
					} else {
						cashInInserted = true;
					}
				} else if (!(cass.getCurr() > 0 && cass.getDenom() > 0)) {
					continue;
				}
				mapper.saveAtmCassettes(atmId, cass.getType().getId(), cass.getCurr(), cass.getNumber(),
						cass.getDenom());
			}
			mapper.flush();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

}
