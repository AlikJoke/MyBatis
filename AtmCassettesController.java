package ru.bpc.cm.cashmanagement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.orm.AtmCassettesMapper;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;
import ru.bpc.cm.utils.CmUtils;

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
		Map<Integer, String> attributes = null;
		try {
			attributes = CmCommonController.getAtmAttributes(sessionHolder, atmId);
		} catch (ForecastException e1) {
			logger.error("", e1);
		}
		try {
			atmCassList.addAll(session.getMapper(getMapperClass()).getAtmCassettes(atmId));
			for (AtmCassetteItem item : atmCassList) {
				if (item.getCapacity() != 0)
					continue;

				item.setCapacity(getDefaultCapacity(item.getType(), attributes));
			}
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
				Integer capacity = null;
				if (!cass.isCapacityResetNeeded()) {
					if (cass.getCapacity() > 0)
						capacity = cass.getCapacity();
				} else {
					cass.setCapacityResetNeeded(false);
				}
				mapper.saveAtmCassettes(atmId, cass.getType().getId(), cass.getCurr(), cass.getNumber(),
						cass.getDenom(), capacity);
			}
			mapper.flush();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void clearCapacityForCass(ISessionHolder sessionHolder, int atmId, int cassNumber) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).updateAtmCassettes(atmId, cassNumber);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static int getDefaultCapacityForCass(ISessionHolder sessionHolder, int atmId, AtmCassetteType type) {
		Map<Integer, String> attributes = null;
		try {
			attributes = CmCommonController.getAtmAttributes(sessionHolder, atmId);
		} catch (ForecastException e) {
			logger.error("", e);
		}

		return getDefaultCapacity(type, attributes);
	}

	private static int getDefaultCapacity(AtmCassetteType type, Map<Integer, String> attributes) {
		int defaultCapacity = 0;
		if (attributes != null) {
			AtmCassetteType cassType = CmUtils.getEnumValueById(AtmCassetteType.class, type.getId());
			switch (cassType) {
			case CASH_OUT_CASS:
				if (attributes.get(AtmAttribute.CASH_OUT_CASS_VOLUME.getAttrID()) != null) {
					defaultCapacity = Integer.valueOf(attributes.get(AtmAttribute.CASH_OUT_CASS_VOLUME.getAttrID()));
				}
				break;
			case CASH_IN_CASS:
				if (attributes.get(AtmAttribute.CASH_IN_CASS_VOLUME.getAttrID()) != null) {
					defaultCapacity = Integer.valueOf(attributes.get(AtmAttribute.CASH_IN_CASS_VOLUME.getAttrID()));
				}
				break;
			case CASH_IN_R_CASS:
				if (attributes.get(AtmAttribute.CASH_IN_R_CASS_VOLUME.getAttrID()) != null) {
					defaultCapacity = Integer.valueOf(attributes.get(AtmAttribute.CASH_IN_R_CASS_VOLUME.getAttrID()));
				}
				break;

			default:
				break;
			}
		}
		return defaultCapacity;
	}
}
