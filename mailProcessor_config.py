FPB_SQL = '''
select v.*,ab.k_ytd_gms as ytd_gms
from tam_db.asin_base ab, tam_db.visibility v
where ab.k_ytd_gms between 2000 and 3000 and ab.onsite_date>='2017-01-01'
and ab.asin=v.asin order by ab.onsite_date desc
'''
MetaAudit_SQL='''
SELECT ma.fro,ma.receive,mc.* from accesslog.mail_audit ma, accesslog.metadata_check_list mc 
where ma.msgid=mc.msgid order by ma.receive desc
'''
HIGH_SCORE='''
select high.reviews_count as '亚马逊评论数',high.average_rating as '亚马逊分',
round(di.rate_number,0) as '豆瓣评论数', round(di.score,1) as '豆瓣分', ab.* 
from tam_db.all_bands ab
left join tam_db.highscore high
on high.asin  = ab.asin
left join (select isbn, avg(rate_number) as rate_number, avg(score) as score 
from hera.db_info group by isbn) di
on di.isbn = ab.isbn
where our_price is not null
and (ab.isbn in (
select isbn from hera.db_info group by isbn having avg(score) >=8)
or ab.asin in( select asin from tam_db.highscore)
) order by 1 desc
'''
VENDOR_GOAL='''
select gmsg.digital_pubcode,gmsg.goal,
round(pubdata.ytd_gms, 0) as act_gms,
concat(round(pubdata.ytd_gms*100/gmsg.goal,1),'%') as progress from 
(SELECT g.digital_pubcode, sum(total_gms) as goal
FROM goal.gms_goal g
where book_type = 'YTD Onsite'
group by g.digital_pubcode)  gmsg
inner join  (
select ab.pubcode, sum(ttm_gms) as ytd_gms from tam_db.all_bands ab
where ab.onsite_date >= '20170101'
and ab.pubcode like '%CN'
group by ab.pubcode
) pubdata
on pubdata.pubcode = gmsg.digital_pubcode
order by goal desc
limit 100;
'''
HIASIN_SQL = u"select tt.asin as KASIN, ab.title_name as '书名', \
    ab.band as 'BAND', ab.onsite_date as '上线日期', ab.pubcode as 'PUBCODE', \
    tt.SNAPSHOT_DAY as '销售日',round(tt.gms/tt.pu,2) as '均价', \
    round(tt.gms,2) as '销售额', tt.pu as '销量', dgv.gv as '浏览量',concat(round(tt.pu/dgv.gv*100,1),'%%') as '转化率' , \
    kd.qb as 'KU下载量', dd.title as '促销种类' \
    from (SELECT das.asin, das.SNAPSHOT_DAY,sum(das.gms)/sum(das.pu) as asp, sum(das.gms) as gms, sum(das.pu) as pu FROM hera.daily_asin_sales das \
    where 1=1 \
    group by das.asin, das.SNAPSHOT_DAY) tt \
    left join hera.ku_daily kd \
    on kd.asin = tt.asin \
    and tt.SNAPSHOT_DAY = kd.O_DAY \
    inner join hera.all_bands ab \
    on tt.asin = ab.asin \
    left join hera.deal_daily dd \
    on dd.ASIN = tt.ASIN \
    and dd.d_date  = tt.SNAPSHOT_DAY \
    left join hera.daily_gv dgv \
    on dgv.asin = tt.asin \
    and dgv.sdate = tt.SNAPSHOT_DAY \
    order by tt.asin, tt.SNAPSHOT_DAY"

apps = \
[
    {
        'appname':'DOTD',
        'search_str':'[#DOTD]',
        'display':'y',
        'desc':'Promotion info fulfillment',
        'cmds':['process(msg, int(bytes.decode(msgid)),name)'],
        'groups':['tam'],
        'users':[]
    },
   {
        'appname':'SQL',
        'search_str':'[#SQL]',
        'display':'y',
        'desc':'Query data by using customized SQL',
        'cmds':['processSQL(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam'],
        'users':[]
    },
   {
        'appname':'HiASIN',
        'search_str':'[#HIASIN]',
        'display':'y',
        'desc':'Query multiple ASINs\' data',
        'cmds':['processHiASIN(msg, int(bytes.decode(msgid)),name)'],
        'groups':['all'],
        'users':[]
    },
   {
        'appname':'HiASIN_example',
        'search_str':'[#HIASIN_EXAMPLE]',
        'display':'y',
        'desc':'Query multiple ASINs\' data',
        'type':'SQL:attach',
        'sqls':[('data',HIASIN_SQL,[('das.asin',r'(B\w{9})')])],
        'groups':[],
        'users':['yongqis','nanmeng','chenmiao']
    },
   {
        'appname':'PubScan',
        'search_str':'[#PUBSCAN]',
        'display':'y',
        'desc':'Query vendor data',
        'cmds':['processPubScan(msg, int(bytes.decode(msgid)), name)'],
        'groups':['all'],
        'users':[]
    },
   {
        'appname':'Taurus',
        'search_str':'[#TAURUS]',
        'display':'y',
        'desc':'Query taurus data',
        'cmds':['reload(taurus)','taurus.process(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam'],
        'users':[]
    },
   {
        'appname':'Metadata',
        'search_str':'[#METADATA]',
        'display':'y',
        'desc':'Get the Metadata checklist',
        'cmds':['reload(metadataProcessor)','metadataProcessor.processMetaData(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam','tamops'],
        'users':['zmingz','zhanglun','lyrun','lzhangi','lzzho','ivyliu','mhlai']
    },
   {
        'appname':'TPOS',
        'search_str':'[#TPOS]',
        'display':'y',
        'desc':'TAM Pattern OS Part',
        'cmds':['processTPOS(msg, int(bytes.decode(msgid)), name)'],
        'groups':['all'],
        'users':[]
    },
   {
        'appname':'TPNEWOS',
        'search_str':'[#TPNEWOS]',
        'display':'y',
        'desc':'TAM Pattern New OS Part',
        'cmds':['processTPNEWOS(msg, int(bytes.decode(msgid)), name)'],
        'groups':['all'],
        'users':[]
    },
   {
        'appname':'TPNEWNOS-DEALINTENT',
        'search_str':'[#TPNEWNOS-DEALINTENT]',
        'display':'n',
        'desc':'TAM Pattern New OS Part',
        'cmds':['import cdMail','reload(cdMail)','cdMail.processTamPattern(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam','CD'],
        'users':['nanmeng']
    },
   {
        'appname':'A9',
        'search_str':'[#A9]',
        'display':'n',
        'desc':'TAM Pattern New OS Part',
        'cmds':['processA9(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam'],
        'users':[]
    },
   {
        'appname':'HIVENDOR',
        'search_str':'HIVENDOR',
        'display':'n',
        'desc':'TAM Pattern New OS Part',
        'cmds':['reload(vendorMail)','vendorMail.processHiVendor(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam','QUE'],
        'users':['nanmeng','chenmiao']
    },
   {
        'appname':'PromoBill',
        'search_str':'[#PROMOBILL]',
        'display':'y',
        'desc':'PromotionBill Data',
        'cmds':['import promotionBill','reload(promotionBill)','promotionBill.processBill(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam'],
        'users':['lyq','lichengl']
    },
   {
        'appname':'PriceLadder',
        'search_str':'[#PRICELADDER]',
        'display':'y',
        'desc':'Price Ladder today',
        'cmds':['processPriceLadder(msg, int(bytes.decode(msgid)), name)'],
        'groups':['tam'],
        'users':['nanmeng']
    },
   {
        'appname':'F+Band',
        'search_str':'[#F+BAND]',
        'display':'y',
        'desc':'Visibility + YTD GMS',
        'type':'SQL',
        'sqls':[('report',FPB_SQL)],
        'groups':['tam'],
        'users':['nanmeng','hanfei']
    },
   {
        'appname':'MetaAudit',
        'search_str':'[#METAAUDIT]',
        'display':'y',
        'desc':'Get Metadata Audit data',
        'type':'SQL',
        'sqls':[('Metadata_audit',MetaAudit_SQL)],
        'groups':['tam'],
        'users':['zmingz','zhanglun','lyrun']
    },
   {
        'appname':'HighScore',
        'search_str':'[#HIGHSCORE]',
        'display':'y',
        'desc':'Get Highscore titles',
        'type':'SQL',
        'sqls':[('highscore',HIGH_SCORE)],
        'groups':['tam','CD','QUE','LITSCE'],
        'users':['nanmeng']
    },
	{
        'appname':'VendorGoal',
        'search_str':'[#VENDORGOAL]',
        'display':'y',
        'desc':'Vendor Goal Progress',
        'type':'SQL',
        'sqls':[('vendorgoal',VENDOR_GOAL)],
        'groups':['tam'],
        'users':['nanmeng']
    },
   {
        'appname':'Feedback',
        'search_str':'[#FEEDBACK]',
        'display':'y',
        'desc':'Provide your feedback to TAM AUTO',
        'cmds':[],
        'groups':['all'],
        'users':[]
    }
]

DeadLock_SQL = '''
#Version 2
SELECT 
		vm,dr.asin as KASIN,
        ab.title_name as TITLE_NAME,
        ab.band as BAND,
        abs.digital_pubcode as CODE,
        dr.price  as recent_P,
        dr.pu as recent_Pu,
        dr.gv as  recent_GV,
        dr.days as days,
        concat(round(dr.pu*100/dr.gv,2),'%') as cr,
        is_ku_flag as ku,
        list_price as DLP,
        p_our_price as op,
        concat(round(dr.price*100/p_our_price,2),'%') as OP_ratio,
        abs.onsite_date as os_date,
        pm.competitor_id as competitor
        from tam_db.price_duration_recent dr
inner join hera.all_bands ab on ab.asin  = dr.asin
inner join (SELECT asin, min(price) as price FROM tam_db.price_duration group by asin) aa on aa.asin = dr.asin and dr.price = aa.price
inner join tam_db.asin_base abs on abs.asin = dr.asin
left  join tam_db.price_match as pm on pm.asin = dr.ASIN
where days>=60 and dr.price <10
and dr.asin in (select asin from hera.all_bands where DIGITAL_PUBCODE <> 'YWGCN' )
and dr.asin in (SELECT asin FROM tam_db.price_match where entry_day_local = (select max(entry_day_local) from tam_db.price_match))
and dr.asin in (SELECT asin FROM tam_db.price_duration group by asin having count(distinct price)>2)
and dr.price/abs.p_our_price < 0.2
and pm.entry_day_local = (select max(entry_day_local) from tam_db.price_match)
order by vm, abs.digital_pubcode
'''
pRank_SQL = '''
select ani.pasin,
left(ani.title_name,15) as title_name,left(ani.author_name,15) as author_name,
ani.digital_pubcode,ani.vm_name,
pr.rank as '当前排名', gs.source as '击中算法', ani.asin_creation_date
from tam_db.all_nos_info ani
left join (
select pasin, group_concat(source) as source from hera.tam_pattern_nos   
where snapshot_day = (select max(snapshot_day) from hera.tam_pattern_nos)
group by pasin
) gs
on gs.pasin = ani.pasin
left join hera.pbook_rank pr
on pr.pasin = ani.pasin
where ani.pasin in(
SELECT distinct(pasin) FROM hera.tam_pattern_nos where
snapshot_day = (select max(snapshot_day) from hera.tam_pattern_nos))
and isbn13 <> ''
and digital_pubcode <> ''
and isbn13 not in (select isbn from tam_db.all_bands)
order by pr.rank 
'''

pasin_creation_SQL = '''
select month(ani.asin_creation_date) as month, 
count(distinct(asin)) as tc
from tam_db.dim_pbook ani
where ani.asin_creation_date >= '20170101'
and ani.asin like 'B%'
group by month(ani.asin_creation_date) 
order by 1 desc;
'''


pasin_creation_SQL_w = '''
select week(ani.asin_creation_date) as week, 
count(distinct(asin)) as tc
from tam_db.dim_pbook ani
where ani.asin_creation_date >= '20170101'
and ani.asin like 'B%'
group by week(ani.asin_creation_date) 
order by 1 desc;
'''


schedulers = \
[
    {
        'name':'deadlock',
        'time':'13:00',
        'day_of_week':'1-5',
        'sqls':[(u'比价死锁书目如下,按照VM和PUBCODE排序,每天刷新：',DeadLock_SQL)],
        'publisher_type':'table',
        'subject':'[[TAM Pattern : PriceDeadlock]] Suspecious Titles for Competitor Price Match DeadLock',
        'groups':[],
        'users':['nanmeng']
    },
    {
        'name':'pRank',
        'time':'9:00',
        'day_of_week':'1-7',
        'sqls':[(u'TAM PATTERN NOS引擎未上线书单，基于Douban、Affinity和A9算法，数据每天刷新，按照ASIN创建日期倒序：',pRank_SQL)],
        'publisher_type':'table',
        'subject':'[TAM Pattern NOS : Daily Mail] Callout for P0 titles',
        'groups':[],
		'users':['nanmeng','chenmiao','jingamz']
    },
    {
        'name':'pasin_creation',
        'time':'9:25',
        'day_of_week':'1-7',
        'sqls':[(u'Pbook ASIN创建：',pasin_creation_SQL, False)],
        'publisher_type':'table',
        'subject':'[TAM Pattern NOS : Opportunity data] PBOOK ASIN CREATION',
        'groups':[],
		'users':['nanmeng','chenmiao']
    },
    {
        'name':'pasin_creation_w',
        'time':'9:25',
        'day_of_week':'1-7',
        'sqls':[(u'Pbook ASIN创建：',pasin_creation_SQL_w, False)],
        'publisher_type':'table',
        'subject':'[TAM Pattern NOS : Opportunity data] PBOOK ASIN CREATION Weekly',
        'groups':[],
		'users':['nanmeng','chenmiao']
    }
]


