package ru.bpc.cm.cashmanagement.orm.handlers;

import org.apache.ibatis.session.ResultContext;
import org.apache.ibatis.session.ResultHandler;

import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;

public class AtmCassetteInResultHandler implements ResultHandler<AtmCassetteItem> {

	@Override
	public void handleResult(ResultContext<? extends AtmCassetteItem> resultContext) {
		AtmCassetteItem item = (AtmCassetteItem) resultContext.getResultObject();
		item.setType(AtmCassetteType.CASH_IN_R_CASS);
	}

}
