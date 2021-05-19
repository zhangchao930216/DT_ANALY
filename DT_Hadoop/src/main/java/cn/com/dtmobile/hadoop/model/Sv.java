package cn.com.dtmobile.hadoop.model;

import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

/**
 * SV接口
 * @author zhangchao15
 * @version 2016-11-14
 */
public class Sv {
	private CommXdr commXdr;
	private String procedureType;
	private Long procedureStartTime;
	private Long procedureEndTime;
	private String sourceNeIp;
	private String sourceNePort;
	private String destNeIp;
	private String destNePort;
	private String roamDirection;
	private String homeMcc;
	private String homeMnc;
	private String mcc;
	private String mnc;
	private String targetLac;
	private String sourceTac;
	private String sourceEci;
	private String svFlags;
	private String ulCMscIp;
	private String dlCMmeIp;
	private String ulCMscTeid;
	private String dlCMmeTeid;
	private String stnSr;
	private String targetRncId;
	private String targetCellId;
	private String arp;
	private String requestResult;
	private String result;
	private String svCause;
	private String postFailureCause;
	private String respDelay;
	private String svDelay;
	private String rangeTime;
	public Sv(String [] values) {
		this.commXdr = new CommXdr(values);
		this.procedureType = values[8];
		this.procedureStartTime = ParseUtils.parseLong(values[9]);
		this.procedureEndTime = ParseUtils.parseLong(values[10]);
//		this.sourceNeIp = values[11];
//		this.sourceNePort = values[12];
//		this.destNeIp = values[13];
//		this.destNePort = values[14];
//		this.roamDirection = values[15];
//		this.homeMcc = values[16];
//		this.homeMnc = values[17];
//		this.mcc = values[18];
//		this.mnc = values[19];
//		this.targetLac = values[20];
//		this.sourceTac = values[21];
		this.sourceEci = values[22];
//		this.svFlags = values[23];
//		this.ulCMscIp = values[24];
//		this.dlCMmeIp = values[25];
//		this.ulCMscTeid = values[26];
//		this.dlCMmeTeid = values[27];
//		this.stnSr = values[28];
//		this.targetRncId = values[29];
		this.targetCellId = values[30];
//		this.arp = values[31];
		this.requestResult = values[32];
		this.result = values[33];
		this.svCause = values[34];
//		this.postFailureCause = values[35];
//		this.respDelay = values[36];
//		this.svDelay = values[37];
//		this.rangeTime = values[values.length-1];
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(commXdr.toString());
		sb.append(procedureType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureStartTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureEndTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceNeIp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceNePort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(destNeIp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(destNePort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(roamDirection);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(homeMcc);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(homeMnc);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mcc);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mnc);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(targetLac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceTac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceEci);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(svFlags);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ulCMscIp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dlCMmeIp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ulCMscTeid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dlCMmeTeid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(stnSr);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(targetRncId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(targetCellId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(arp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(requestResult);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(result);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(svCause);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(postFailureCause);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(respDelay);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(svDelay);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(rangeTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		return sb.toString();
	}
	
	
	public String getRangeTime() {
		return rangeTime;
	}

	public void setRangeTime(String rangeTime) {
		this.rangeTime = rangeTime;
	}

	/**
	 * @return the commXdr
	 */
	public CommXdr getCommXdr() {
		return commXdr;
	}

	/**
	 * @param commXdr the commXdr to set
	 */
	public void setCommXdr(CommXdr commXdr) {
		this.commXdr = commXdr;
	}

	/**
	 * @return the procedureType
	 */
	public String getProcedureType() {
		return procedureType;
	}

	/**
	 * @param procedureType the procedureType to set
	 */
	public void setProcedureType(String procedureType) {
		this.procedureType = procedureType;
	}

	/**
	 * @return the procedureStartTime
	 */
	public Long getProcedureStartTime() {
		return procedureStartTime;
	}

	/**
	 * @param procedureStartTime the procedureStartTime to set
	 */
	public void setProcedureStartTime(Long procedureStartTime) {
		this.procedureStartTime = procedureStartTime;
	}

	/**
	 * @return the procedureEndTime
	 */
	public Long getProcedureEndTime() {
		return procedureEndTime;
	}

	/**
	 * @param procedureEndTime the procedureEndTime to set
	 */
	public void setProcedureEndTime(Long procedureEndTime) {
		this.procedureEndTime = procedureEndTime;
	}

	/**
	 * @return the sourceNeIp
	 */
	public String getSourceNeIp() {
		return sourceNeIp;
	}

	/**
	 * @param sourceNeIp the sourceNeIp to set
	 */
	public void setSourceNeIp(String sourceNeIp) {
		this.sourceNeIp = sourceNeIp;
	}

	/**
	 * @return the sourceNePort
	 */
	public String getSourceNePort() {
		return sourceNePort;
	}

	/**
	 * @param sourceNePort the sourceNePort to set
	 */
	public void setSourceNePort(String sourceNePort) {
		this.sourceNePort = sourceNePort;
	}

	/**
	 * @return the destNeIp
	 */
	public String getDestNeIp() {
		return destNeIp;
	}

	/**
	 * @param destNeIp the destNeIp to set
	 */
	public void setDestNeIp(String destNeIp) {
		this.destNeIp = destNeIp;
	}

	/**
	 * @return the destNePort
	 */
	public String getDestNePort() {
		return destNePort;
	}

	/**
	 * @param destNePort the destNePort to set
	 */
	public void setDestNePort(String destNePort) {
		this.destNePort = destNePort;
	}

	/**
	 * @return the roamDirection
	 */
	public String getRoamDirection() {
		return roamDirection;
	}

	/**
	 * @param roamDirection the roamDirection to set
	 */
	public void setRoamDirection(String roamDirection) {
		this.roamDirection = roamDirection;
	}

	/**
	 * @return the homeMcc
	 */
	public String getHomeMcc() {
		return homeMcc;
	}

	/**
	 * @param homeMcc the homeMcc to set
	 */
	public void setHomeMcc(String homeMcc) {
		this.homeMcc = homeMcc;
	}

	/**
	 * @return the homeMnc
	 */
	public String getHomeMnc() {
		return homeMnc;
	}

	/**
	 * @param homeMnc the homeMnc to set
	 */
	public void setHomeMnc(String homeMnc) {
		this.homeMnc = homeMnc;
	}

	/**
	 * @return the mcc
	 */
	public String getMcc() {
		return mcc;
	}

	/**
	 * @param mcc the mcc to set
	 */
	public void setMcc(String mcc) {
		this.mcc = mcc;
	}

	/**
	 * @return the mnc
	 */
	public String getMnc() {
		return mnc;
	}

	/**
	 * @param mnc the mnc to set
	 */
	public void setMnc(String mnc) {
		this.mnc = mnc;
	}

	/**
	 * @return the targetLac
	 */
	public String getTargetLac() {
		return targetLac;
	}

	/**
	 * @param targetLac the targetLac to set
	 */
	public void setTargetLac(String targetLac) {
		this.targetLac = targetLac;
	}

	/**
	 * @return the sourceTac
	 */
	public String getSourceTac() {
		return sourceTac;
	}

	/**
	 * @param sourceTac the sourceTac to set
	 */
	public void setSourceTac(String sourceTac) {
		this.sourceTac = sourceTac;
	}

	/**
	 * @return the sourceEci
	 */
	public String getSourceEci() {
		return sourceEci;
	}

	/**
	 * @param sourceEci the sourceEci to set
	 */
	public void setSourceEci(String sourceEci) {
		this.sourceEci = sourceEci;
	}

	/**
	 * @return the svFlags
	 */
	public String getSvFlags() {
		return svFlags;
	}

	/**
	 * @param svFlags the svFlags to set
	 */
	public void setSvFlags(String svFlags) {
		this.svFlags = svFlags;
	}

	/**
	 * @return the ulCMscIp
	 */
	public String getUlCMscIp() {
		return ulCMscIp;
	}

	/**
	 * @param ulCMscIp the ulCMscIp to set
	 */
	public void setUlCMscIp(String ulCMscIp) {
		this.ulCMscIp = ulCMscIp;
	}

	/**
	 * @return the dlCMmeIp
	 */
	public String getDlCMmeIp() {
		return dlCMmeIp;
	}

	/**
	 * @param dlCMmeIp the dlCMmeIp to set
	 */
	public void setDlCMmeIp(String dlCMmeIp) {
		this.dlCMmeIp = dlCMmeIp;
	}

	/**
	 * @return the ulCMscTeid
	 */
	public String getUlCMscTeid() {
		return ulCMscTeid;
	}

	/**
	 * @param ulCMscTeid the ulCMscTeid to set
	 */
	public void setUlCMscTeid(String ulCMscTeid) {
		this.ulCMscTeid = ulCMscTeid;
	}

	/**
	 * @return the dlCMmeTeid
	 */
	public String getDlCMmeTeid() {
		return dlCMmeTeid;
	}

	/**
	 * @param dlCMmeTeid the dlCMmeTeid to set
	 */
	public void setDlCMmeTeid(String dlCMmeTeid) {
		this.dlCMmeTeid = dlCMmeTeid;
	}

	/**
	 * @return the stnSr
	 */
	public String getStnSr() {
		return stnSr;
	}

	/**
	 * @param stnSr the stnSr to set
	 */
	public void setStnSr(String stnSr) {
		this.stnSr = stnSr;
	}

	/**
	 * @return the targetRncId
	 */
	public String getTargetRncId() {
		return targetRncId;
	}

	/**
	 * @param targetRncId the targetRncId to set
	 */
	public void setTargetRncId(String targetRncId) {
		this.targetRncId = targetRncId;
	}

	/**
	 * @return the targetCellId
	 */
	public String getTargetCellId() {
		return targetCellId;
	}

	/**
	 * @param targetCellId the targetCellId to set
	 */
	public void setTargetCellId(String targetCellId) {
		this.targetCellId = targetCellId;
	}

	/**
	 * @return the arp
	 */
	public String getArp() {
		return arp;
	}

	/**
	 * @param arp the arp to set
	 */
	public void setArp(String arp) {
		this.arp = arp;
	}

	/**
	 * @return the requestResult
	 */
	public String getRequestResult() {
		return requestResult;
	}

	/**
	 * @param requestResult the requestResult to set
	 */
	public void setRequestResult(String requestResult) {
		this.requestResult = requestResult;
	}

	/**
	 * @return the result
	 */
	public String getResult() {
		return result;
	}

	/**
	 * @param result the result to set
	 */
	public void setResult(String result) {
		this.result = result;
	}

	/**
	 * @return the svCause
	 */
	public String getSvCause() {
		return svCause;
	}

	/**
	 * @param svCause the svCause to set
	 */
	public void setSvCause(String svCause) {
		this.svCause = svCause;
	}

	/**
	 * @return the postFailureCause
	 */
	public String getPostFailureCause() {
		return postFailureCause;
	}

	/**
	 * @param postFailureCause the postFailureCause to set
	 */
	public void setPostFailureCause(String postFailureCause) {
		this.postFailureCause = postFailureCause;
	}

	/**
	 * @return the respDelay
	 */
	public String getRespDelay() {
		return respDelay;
	}

	/**
	 * @param respDelay the respDelay to set
	 */
	public void setRespDelay(String respDelay) {
		this.respDelay = respDelay;
	}

	/**
	 * @return the svDelay
	 */
	public String getSvDelay() {
		return svDelay;
	}

	/**
	 * @param svDelay the svDelay to set
	 */
	public void setSvDelay(String svDelay) {
		this.svDelay = svDelay;
	}
	
}
