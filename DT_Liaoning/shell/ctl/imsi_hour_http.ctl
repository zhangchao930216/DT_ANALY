LOAD DATA
characterset UTF8
APPEND INTO TABLE imsi_hour_http
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
imsi,
msisdn,
browsedownloadvisits,
videoservicevisits,
instantmessagevisits,
appvisits,
browsedownloadbusiness,
videoservicebusiness,
instantmessagebusiness,
appbusiness,
dnsquerysucc,
dnsqueryatt,
tcpsetupsucc,
tcpsetupreq,
bearerultcpretransmit,
bearerultcptransmit,
bearerdltcpretransmit,
bearerdltcptransmit,
bearerultcpmissequence,
bearerdltcpmissequence,
pageresp,
pagereq,
pageresptimeall,
pageshowsucc,
pageshowtimeall,
httpdownflow,
httpdowntime,
mediasucc,
mediareq,
mediadownflow,
mediadowntime,
serviceimsucc,
serviceimreq,
readvisits,
wbvisits,
navigationvisits,
musicvisits,
gamevisits,
payvisits,
Animevisits,
mailvisits,
p2pvisits,
voipvisits,
MultimediaMsgvisits,
financialvisits,
securityvisits,
shoppingvisits,
travelvisits,
cloudstoragevisits,
othervisits,
readbusiness,
wbbusiness,
navigationbusiness,
musicbusiness,
gamebusiness,
paybusiness,
Animebusiness,
mailbusiness,
p2pbusiness,
voipbusiness,
MultimediaMsgbusiness,
financialbusiness,
securitybusiness,
shoppingbusiness,
travelbusiness,
cloudstoragebusiness,
otherbusiness,
mediaRespTimeall,
mediaResp,
ServiceIMTrans,
ServiceIMFlow,
ServiceIMTime
)