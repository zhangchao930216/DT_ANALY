LOAD DATA
characterset UTF8
APPEND INTO TABLE volte_gt_cell_ana_base60
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
cellid,
voltemcsucc,
voltemcatt,
voltevdsucc,
voltevdatt,
voltetime,
voltemctime,
voltemctimey,
voltevdtime,
voltevdtimey,
voltemchandover,
volteanswer,
voltevdhandover,
voltevdanswer,
srvccsucc,
srvccatt,
srvcctime,
lteswsucc,
lteswatt,
srqatt,
srqsucc,
tauatt,
tausucc,
rrcrebuild,
rrcsucc,
rrcreq,
imsiregatt,
imsiregsucc,
wirelessdrop,
wireless,
eabdrop,
eab,
eabs1swx,
eabs1swy,
s1tox2swx,
s1tox2swy,
enbx2swx,
enbx2swy,
uuenbswx,
uuenbswy,
uuenbinx,
uuenbiny,
swx,
swy,
attachx,
attachy,
voltesucc,
srvccsuccS1,
s1ContextBuild,
enbrelese,
nenbrelese,
remaincontext,
srvccsucc_Sv,
srvccatt_S1,
erabBuildReq,
erabBuildSucc,
s1ContextBuildSucc,
sm_adebReq_qci1,
sm_adebSucc_qci1,
sm_adebReq_qci2,
sm_adebSucc_qci2,
nbrReqRelenb_qci1_erab,
nbrReqRelenb_qci1,
s1hooutsucc,
s1hoout,
s1hoinsucc,
s1hoin,
voltecallingmcsucc,
voltecallingmcatt,
voltecalledmcsucc,
voltecalledmcatt,
voltecallingvdsucc,
voltecallingvdatt,
voltecalledvdsucc,
voltecalledvdatt,
voltemcnetsucc,
voltemcnetatt,
voltevdnetsucc,
voltevdnetatt,
voltecallingmctime,
voltecallingvdtime,
srvccsucc_s1,
enbx2insucc,
enbx2inatt,
ERAB_NbrS1HoFail_qci1,
ERAB_NbrX2HoFail_qci1,
enbreleseSucc
)