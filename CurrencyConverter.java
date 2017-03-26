package ru.bpc.cm.cashmanagement;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.utils.ObjectPair;

public class CurrencyConverter {
	
	private Map<String,Map<String,ObjectPair<String, Double>>> conversionRates;
	
	public CurrencyConverter(ISessionHolder sessionHolder, Connection connection, int atmId, int... additionalCurrCodes) {
		Set<Integer> currencies = new HashSet<Integer>( 
				CmCommonController.getAtmCurrencies(sessionHolder, atmId));
		for(int i : additionalCurrCodes){
			currencies.add(Integer.valueOf(i));	
		}
		if(!currencies.isEmpty()){
			if(currencies.size() > 1){
				conversionRates = CmCommonController.getConvertionsRates(sessionHolder, atmId, new ArrayList<Integer>(currencies));
			}
		}
	}
	
	public Double convertValue(int srcCurr, int destCurr, double value){
		if (srcCurr == 0 || destCurr == 0 || srcCurr == destCurr
				||conversionRates == null || conversionRates.isEmpty()) {
			return Double.valueOf(value);
		}
		Map<String,ObjectPair<String, Double>> ratesForSrcCurr = 
				conversionRates.get(String.valueOf(srcCurr));
		if(ratesForSrcCurr == null || ratesForSrcCurr.isEmpty()){
			return Double.valueOf(value);
		}
		ObjectPair<String, Double> conversion = 
				ratesForSrcCurr.get(String.valueOf(destCurr));
		if(conversion == null || conversion.getKey() == null || conversion.getValue() == null){
			return Double.valueOf(value);
		}
		String multFlag = conversion.getKey();
		
		if (multFlag.equals("M")) {
			return value * conversion.getValue();
		}
		if (multFlag.equals("D")) {
			return value / (conversion.getValue() == 0 ? 1 : conversion.getValue());
		}
		return Double.valueOf(value);
	
	}
	

}
