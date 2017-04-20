package ru.bpc.cm.filters;

import ru.bpc.cm.items.enums.AtmTypeByOperations;
import ru.bpc.cm.utils.CmUtils;


public class UserAdditionalFilter {
	
	private String nameAndAddress;
	private String atmID;
	private String extAtmID;
	private int typeByOperations;
		
	public UserAdditionalFilter(){
		typeByOperations = -1;
	}

	public String getAtmID() {
		return atmID;
	}

	public void setAtmID(String atmID) {
		this.atmID = atmID;
	}

	public String getExtAtmID() {
		return extAtmID;
	}

	public void setExtAtmID(String extAtmID) {
		this.extAtmID = extAtmID;
	}

	public String getNameAndAddress() {
		return nameAndAddress;
	}

	public void setNameAndAddress(String nameAndAddress) {
		this.nameAndAddress = nameAndAddress;
	}

	public int getTypeByOperations() {
		return typeByOperations;
	}

	public void setTypeByOperations(int typeByOperations) {
		this.typeByOperations = typeByOperations;
	}
	
	public AtmTypeByOperations getAtmTypeByOpertaions(){
		return CmUtils.getEnumValueById(AtmTypeByOperations.class, typeByOperations);
	}
}
