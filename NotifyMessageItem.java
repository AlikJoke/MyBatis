package ru.bpc.cm.items.cashmanagement;

import java.sql.Timestamp;
import java.util.Date;

import ru.bpc.cm.items.enums.NotifyMessageType;
import ru.bpc.cm.utils.CmUtils;

public class NotifyMessageItem {

	private Date date;
	private NotifyMessageType messageType;
	private boolean msgNew;
	private int messageId;
	private Object[] paramList;
	
	private String localizedTitle;
	private String localizedMsg;

	public NotifyMessageItem () {}
	
	public NotifyMessageItem(Timestamp date, Integer messageTypeId, Boolean msgNew, Integer messageId, String params) {
		this.date = date;
		this.messageType = CmUtils.getEnumValueById(NotifyMessageType.class, messageTypeId);
		this.msgNew = msgNew;
		this.messageId = messageId;
		this.paramList = CmUtils.getNVLValue(params, "").split(",");
	}
	
	public Date getDate() {
    	return date;
    }
	public void setDate(Date date) {
    	this.date = date;
    }
	public NotifyMessageType getMessage() {
    	return messageType;
    }
	public void setMessage(int messageTypeId) {
    	this.messageType = CmUtils.getEnumValueById(NotifyMessageType.class, messageTypeId);
    }
	public boolean isMsgNew() {
    	return msgNew;
    }
	public void setMsgNew(boolean msgNew) {
    	this.msgNew = msgNew;
    }
	public int getMessageId() {
    	return messageId;
    }
	public void setMessageId(int messageId) {
    	this.messageId = messageId;
    }
	public Object[] getParamList() {
    	return paramList;
    }
	public void setParamList(Object[] paramList) {
    	this.paramList = paramList;
    }
	public String getLocalizedTitle() {
		return localizedTitle;
	}
	public void setLocalizedTitle(String localizedTitle) {
		this.localizedTitle = localizedTitle;
	}
	public String getLocalizedMsg() {
		return localizedMsg;
	}
	public void setLocalizedMsg(String localizedMsg) {
		this.localizedMsg = localizedMsg;
	}

}
