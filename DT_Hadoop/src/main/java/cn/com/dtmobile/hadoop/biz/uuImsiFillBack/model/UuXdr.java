package cn.com.dtmobile.hadoop.biz.uuImsiFillBack.model;

import cn.com.dtmobile.hadoop.util.DateUtils;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class UuXdr {
	private String produerId ;
	private Integer length;
	private String city;
	private Integer interfaces;
	private String xdrid;
	private Integer rat;
	private String imsi;
	private String imei;
	private String msisdn;

	
	private String procedureType;
	private Long procedureStartTime;
	private Long procedureEndTime;
	private String keyword;
	private String procedureStatus;
	private String plmnId;
	private String enbId;
	private String cellId;
	private String crnti;
	private String targetEnbId;
	private String targetCellId;
	private String targetCrnti;
	private String mmeUeS1apId;
	private String mmeGroupId;
	private String mmeCode;
	private String mTmsi;
	private String csfbIndication;
	private String redirectednetwork;
	private Integer epsBearerNumber;
	private String[] bearerArr;
	private Long rangeTime;
	

	public UuXdr() {
	}
	
	public UuXdr(String[] values) {
	
		//所有下标+1
		this.produerId = values[0];
		this.length = ParseUtils.parseInteger(values[1]);
		this.city = values[2];
		this.interfaces = ParseUtils.parseInteger(values[3]);
		this.xdrid = values[4];
		this.rat = ParseUtils.parseInteger(values[5]);
		this.imsi = values[6];
		this.imei = values[7];
		this.msisdn = values[8];
		
		
		
		this.procedureType = values[9];
		this.procedureStartTime = Long.parseLong(values[10]);
		this.procedureEndTime = Long.parseLong(values[11]);
		this.keyword = values[12];
		this.procedureStatus = values[14]; //13
		this.plmnId = values[15];
		this.enbId = values[16];
		this.cellId = values[17];
		this.crnti = values[18];
		this.targetEnbId = values[19];
		this.targetCellId = values[20];
		this.targetCrnti = values[21];
		this.mmeUeS1apId = values[22];
		this.mmeGroupId = values[23];
		this.mmeCode = values[24];
		this.mTmsi = values[25];
		this.csfbIndication = values[26];
//		this.redirectednetwork = values[26];
		/*this.epsBearerNumber = this.epsBearerNumber;
		bearerArr = new String[this.epsBearerNumber];
		Integer start = 26;
		Integer step = 2;
		for (Integer i = 0; i < this.epsBearerNumber; i++) {
			StringBuffer sb = new StringBuffer();
			for (Integer j = start; j <= start + step; j++) {
				sb.append(values[j]);
				sb.append(StringUtils.DELIMITER_VERTICAL);
			}
			start = start + step;
			sb.deleteCharAt(sb.length() - 1);
			bearerArr[i] = sb.toString();
		}*/
	if(values[values.length-1].contains(StringUtils.DELIMITER_INNER_ITEM2)){
		this.rangeTime = DateUtils.getDateMills(values[values.length-1])   ;
	}else{
		this.rangeTime = Long.valueOf(values[values.length-1]) ;
	}

		//	System.out.println(rangeTime);
	
	
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(produerId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(length);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(city);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(interfaces);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(xdrid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(rat);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(imsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(imei);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(msisdn);
		sb.append(StringUtils.DELIMITER_VERTICAL);
	
		sb.append(procedureType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(procedureStartTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(procedureEndTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(keyword);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append("");
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		
		sb.append(procedureStatus);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(plmnId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(enbId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(cellId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(crnti);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(targetEnbId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(targetCellId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(targetCrnti);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mmeUeS1apId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mmeGroupId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mmeCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mTmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(csfbIndication);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		/*sb.append(epsBearerNumber);
		sb.append(StringUtils.DELIMITER_VERTICAL);*/
		/*for(String val : this.bearerArr){
			sb.append(val);
			sb.append(StringUtils.DELIMITER_VERTICAL);
		}*/
		sb.append(rangeTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		return sb.toString();
	}
	
	public String toString2() {
		StringBuffer sb = new StringBuffer();
		sb.append(length);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(city);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(interfaces);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(xdrid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(rat);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(imsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(imei);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(msisdn);
		sb.append(StringUtils.DELIMITER_VERTICAL);
	
		sb.append(procedureType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(procedureStartTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(procedureEndTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(keyword);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		
		sb.append(procedureStatus);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(plmnId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(enbId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(cellId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(crnti);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(targetEnbId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(targetCellId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(targetCrnti);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mmeUeS1apId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mmeGroupId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mmeCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(mTmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		
		sb.append(csfbIndication);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		/*sb.append(epsBearerNumber);
		sb.append(StringUtils.DELIMITER_VERTICAL);*/
		/*for(String val : this.bearerArr){
			sb.append(val);
			sb.append(StringUtils.DELIMITER_VERTICAL);
		}*/
		sb.append(rangeTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		return sb.toString();
	}
	
	
	
	
	
	
	public String getRedirectednetwork() {
		return redirectednetwork;
	}
	
	public void setRedirectednetwork(String redirectednetwork) {
		this.redirectednetwork = redirectednetwork;
	}
	public Long getProcedureStartTime() {
		return procedureStartTime;
	}
	
	public void setProcedureStartTime(Long procedureStartTime) {
		this.procedureStartTime = procedureStartTime;
	}
	
	public String getProduerId() {
		return produerId;
	}

	public void setProduerId(String produerId) {
		this.produerId = produerId;
	}

	public Long getProcedureEndTime() {
		return procedureEndTime;
	}
	
	public void setProcedureEndTime(Long procedureEndTime) {
		this.procedureEndTime = procedureEndTime;
	}

	

	public Integer getLength() {
		return length;
	}

	public void setLength(Integer length) {
		this.length = length;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Integer getInterfaces() {
		return interfaces;
	}

	public void setInterfaces(Integer interfaces) {
		this.interfaces = interfaces;
	}

	public String getXdrid() {
		return xdrid;
	}

	public void setXdrid(String xdrid) {
		this.xdrid = xdrid;
	}

	public Integer getRat() {
		return rat;
	}

	public void setRat(Integer rat) {
		this.rat = rat;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	public String getMsisdn() {
		return msisdn;
	}

	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}

	public String getProcedureType() {
		return procedureType;
	}

	public void setProcedureType(String procedureType) {
		this.procedureType = procedureType;
	}


	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public String getProcedureStatus() {
		return procedureStatus;
	}

	public void setProcedureStatus(String procedureStatus) {
		this.procedureStatus = procedureStatus;
	}

	public String getPlmnId() {
		return plmnId;
	}

	public void setPlmnId(String plmnId) {
		this.plmnId = plmnId;
	}

	public String getEnbId() {
		return enbId;
	}

	public void setEnbId(String enbId) {
		this.enbId = enbId;
	}

	public String getCellId() {
		return cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}

	public String getCrnti() {
		return crnti;
	}

	public void setCrnti(String crnti) {
		this.crnti = crnti;
	}

	public String getTargetEnbId() {
		return targetEnbId;
	}

	public void setTargetEnbId(String targetEnbId) {
		this.targetEnbId = targetEnbId;
	}

	public String getTargetCellId() {
		return targetCellId;
	}

	public void setTargetCellId(String targetCellId) {
		this.targetCellId = targetCellId;
	}

	public String getTargetCrnti() {
		return targetCrnti;
	}

	public void setTargetCrnti(String targetCrnti) {
		this.targetCrnti = targetCrnti;
	}

	public String getMmeUeS1apId() {
		return mmeUeS1apId;
	}

	public void setMmeUeS1apId(String mmeUeS1apId) {
		this.mmeUeS1apId = mmeUeS1apId;
	}

	public String getMmeGroupId() {
		return mmeGroupId;
	}

	public void setMmeGroupId(String mmeGroupId) {
		this.mmeGroupId = mmeGroupId;
	}

	public String getMmeCode() {
		return mmeCode;
	}

	public void setMmeCode(String mmeCode) {
		this.mmeCode = mmeCode;
	}

	public String getmTmsi() {
		return mTmsi;
	}

	public void setmTmsi(String mTmsi) {
		this.mTmsi = mTmsi;
	}

	public String getCsfbIndication() {
		return csfbIndication;
	}

	public void setCsfbIndication(String csfbIndication) {
		this.csfbIndication = csfbIndication;
	}

	public Integer getEpsBearerNumber() {
		return epsBearerNumber;
	}

	public void setEpsBearerNumber(Integer epsBearerNumber) {
		this.epsBearerNumber = epsBearerNumber;
	}

	public String[] getBearerArr() {
		return bearerArr;
	}

	public void setBearerArr(String[] bearerArr) {
		this.bearerArr = bearerArr;
	}

	public Long getRangeTime() {
		return rangeTime;
	}

	public void setRangeTime(Long rangeTime) {
		this.rangeTime = rangeTime;
	}
	
	
}
