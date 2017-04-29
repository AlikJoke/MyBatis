package ru.bpc.cm.reports.orm.builders;

import java.util.Map;

public class ReportDenomCurrBuilder {

	public String getReportCoDenomStatBuilder(Map<String, Object> params) {
		StringBuilder sql = new StringBuilder("SELECT * FROM ( ");
		boolean splitInCycles = (Boolean) params.get("splitInCycles");
		if (!splitInCycles)
			sql.append("SELECT  atm_id, denom_value, SUM(denom_out) AS denom_out, "
					+ "SUM(denom_in) AS denom_in, SUM(denom_trans_count) AS denom_trans_count, "
					+ "SUM(curr_trans_count) AS curr_trans_count, MIN(enc_date_from) AS enc_date_from, "
					+ "MAX(enc_date_to) as enc_date_to, denom_curr_a3, denom_curr, "
					+ "count(DENOM_VALUE) over (partition by ATM_ID) as DENOM_COUNT, "
					+ "row_number() over (partition by ATM_ID order by denom_curr desc, DENOM_VALUE) as LINE_NUBMER "
					+ "FROM ( ");
		sql.append("SELECT atm_id, denom_value, denom_out, denom_in, denom_trans_count, "
				+ "curr_trans_count, enc_date as enc_date_from, enc_date_to, code_a3 as denom_curr_a3, "
				+ "denom_curr, count(dt.DENOM_VALUE) over (partition by dt.ATM_ID,dt.enc_date) as DENOM_COUNT, "
				+ "row_number() over (partition by  dt.ATM_ID,dt.enc_date order by dt.denom_curr desc,dt.DENOM_VALUE) as LINE_NUBMER "
				+ "FROM v_cm_r_denom_stat dt JOIN t_cm_curr ci on (ci.code_n3 = dt.denom_curr) "
				+ "WHERE 1=1 AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) < #{dateTo} "
				+ "AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) > #{dateFrom} ) ");
		if (!splitInCycles)
			sql.append("GROUP BY atm_id, denom_curr_a3, denom_curr, denom_value ) ");
		sql.append(" dt , t_cm_temp_atm_list WHERE atm_id = id "
				+ "ORDER BY atm_id,enc_date_from desc,dt.denom_curr, dt.denom_value desc ");
		return sql.toString();
	}

	public String getReportCoDenomStatForAtmBuilder(Map<String, Object> params) {
		boolean splitInCycles = (Boolean) params.get("splitInCycles");
		StringBuilder sql = new StringBuilder("SELECT * FROM ( ");
		if (!splitInCycles)
			sql.append("SELECT  atm_id, denom_value, SUM(denom_out) AS denom_out, "
					+ "SUM(denom_in) AS denom_in, SUM(denom_trans_count) AS denom_trans_count, "
					+ "SUM(curr_trans_count) AS curr_trans_count, MIN(enc_date_from) AS enc_date_from, "
					+ "MAX(enc_date_to) as enc_date_to, denom_curr_a3, denom_curr, "
					+ "count(DENOM_VALUE) over (partition by ATM_ID) as DENOM_COUNT, "
					+ "row_number() over (partition by ATM_ID order by denom_curr desc, DENOM_VALUE) as LINE_NUBMER "
					+ "FROM ( ");
		sql.append("SELECT atm_id, denom_value, denom_out, denom_in, denom_trans_count, "
				+ "curr_trans_count, enc_date as enc_date_from, enc_date_to, code_a3 as denom_curr_a3, "
				+ "denom_curr, count(dt.DENOM_VALUE) over (partition by dt.ATM_ID,dt.enc_date) as DENOM_COUNT, "
				+ "row_number() over (partition by  dt.ATM_ID,dt.enc_date order by dt.denom_curr desc,dt.DENOM_VALUE) as LINE_NUBMER "
				+ "FROM v_cm_r_denom_stat dt JOIN t_cm_curr ci on (ci.code_n3 = dt.denom_curr) "
				+ "WHERE 1=1 AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) < #{dateTo} "
				+ "AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) > #{dateFrom} ) ");
		if (!splitInCycles)
			sql.append("GROUP BY atm_id, denom_curr_a3, denom_curr, denom_value ) ");
		sql.append(
				" dt WHERE atm_id = #{atmId} ORDER BY atm_id,enc_date_from desc,dt.denom_curr, dt.denom_value desc ");
		return sql.toString();
	}

	public String getReportCrDenomStatBuilder(Map<String, Object> params) {
		boolean splitInCycles = (Boolean) params.get("splitInCycles");
		StringBuilder sql = new StringBuilder("SELECT * FROM ( ");
		if (!splitInCycles)
			sql.append("SELECT  atm_id, denom_value, SUM(denom_count_out) AS denom_count_out, "
					+ "SUM(denom_count_in) AS denom_count_in, SUM(denom_trans_count_in) AS denom_trans_count_in, "
					+ "SUM(denom_trans_count_out) AS denom_trans_count_out, "
					+ "SUM(curr_trans_count_in) AS curr_trans_count_in, "
					+ "SUM(curr_trans_count_out) AS curr_trans_count_out, SUM(loaded) AS loaded, "
					+ "MIN(enc_date_from) AS enc_date_from, MAX(enc_date_to) as enc_date_to, denom_curr_a3, "
					+ "denom_curr, count(DENOM_VALUE) over (partition by ATM_ID) as DENOM_COUNT, "
					+ "row_number() over (partition by ATM_ID order by denom_curr desc, DENOM_VALUE) as LINE_NUBMER "
					+ "FROM ( ");
		sql.append("SELECT atm_id, denom_value, denom_count_out AS denom_count_out, "
				+ "denom_count_in AS denom_count_in, denom_trans_count_in AS denom_trans_count_in, "
				+ "denom_trans_count_out AS denom_trans_count_out, curr_trans_count_in AS curr_trans_count_in, "
				+ "curr_trans_count_out AS curr_trans_count_out, enc_date as enc_date_from, enc_date_to, "
				+ "loaded, code_a3 as denom_curr_a3, dt.denom_curr, "
				+ "count(dt.DENOM_VALUE) over (partition by dt.ATM_ID,dt.enc_date) as DENOM_COUNT, "
				+ "row_number() over (partition by  dt.ATM_ID,dt.enc_date order by dt.denom_curr desc,dt.DENOM_VALUE) as LINE_NUBMER "
				+ "FROM v_cm_r_cashin_r_denom_stat dt JOIN t_cm_curr ci on (ci.code_n3 = denom_curr) "
				+ "WHERE 1=1 AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) < #{dateTo} "
				+ "AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) > #{dateFrom} ) ");
		if (!splitInCycles)
			sql.append("GROUP BY atm_id, denom_curr_a3, denom_curr, denom_value ) ");
		sql.append(" dt , t_cm_temp_atm_list WHERE atm_id = id "
				+ "ORDER BY atm_id,enc_date_from desc,dt.denom_curr, dt.denom_value desc ");
		return sql.toString();
	}

	public String getReportCrDenomStatForAtmBuilder(Map<String, Object> params) {
		boolean splitInCycles = (Boolean) params.get("splitInCycles");
		StringBuilder sql = new StringBuilder("SELECT * FROM ( ");
		if (!splitInCycles)
			sql.append("SELECT  atm_id, denom_value, SUM(denom_count_out) AS denom_count_out, "
					+ "SUM(denom_count_in) AS denom_count_in, SUM(denom_trans_count_in) AS denom_trans_count_in, "
					+ "SUM(denom_trans_count_out) AS denom_trans_count_out, "
					+ "SUM(curr_trans_count_in) AS curr_trans_count_in, "
					+ "SUM(curr_trans_count_out) AS curr_trans_count_out, SUM(loaded) AS loaded, "
					+ "MIN(enc_date_from) AS enc_date_from, MAX(enc_date_to) as enc_date_to, denom_curr_a3, "
					+ "denom_curr, count(DENOM_VALUE) over (partition by ATM_ID) as DENOM_COUNT, "
					+ "row_number() over (partition by ATM_ID order by denom_curr desc, DENOM_VALUE) as LINE_NUBMER "
					+ "FROM ( ");
		sql.append("SELECT atm_id, denom_value, denom_count_out AS denom_count_out, "
				+ "denom_count_in AS denom_count_in, denom_trans_count_in AS denom_trans_count_in, "
				+ "denom_trans_count_out AS denom_trans_count_out, curr_trans_count_in AS curr_trans_count_in, "
				+ "curr_trans_count_out AS curr_trans_count_out, enc_date as enc_date_from, enc_date_to, "
				+ "loaded, code_a3 as denom_curr_a3, dt.denom_curr, "
				+ "count(dt.DENOM_VALUE) over (partition by dt.ATM_ID,dt.enc_date) as DENOM_COUNT, "
				+ "row_number() over (partition by  dt.ATM_ID,dt.enc_date order by dt.denom_curr desc,dt.DENOM_VALUE) as LINE_NUBMER "
				+ "FROM v_cm_r_cashin_r_denom_stat dt JOIN t_cm_curr ci on (ci.code_n3 = denom_curr) "
				+ "WHERE 1=1 AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) < #{dateTo} "
				+ "AND COALESCE(dt.enc_date_to,CURRENT_TIMESTAMP) > #{dateFrom} ) ");
		if (!splitInCycles)
			sql.append("GROUP BY atm_id, denom_curr_a3, denom_curr, denom_value ) ");
		sql.append(
				" dt WHERE atm_id = #{atmId} ORDER BY atm_id,enc_date_from desc,dt.denom_curr, dt.denom_value desc ");
		return sql.toString();
	}

}
