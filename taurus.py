#!/usr/bin/python3
#coding=utf-8

import imaplib
import email
from email.parser import Parser
from email.header import decode_header
from email.utils import parseaddr
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import COMMASPACE,formatdate
from email import encoders

from bs4 import BeautifulSoup
import pymysql
import importlib
import sys
import logging
import getpass
import time
import xlrd
import os
import xlwt
import nltk
import unicodedata
import re
import datetime
import tamCommonLib

## comment variables ##########################################
## time zone
mysql_host = 'ud094661c879c59fa6e9e'
mysql_user = '***'
mysql_password = '***'
mysql_default_db = '***'
work_path = './'
## mysql configuration
importlib.reload(sys)


mail_sql = '''
       select pdr.asin,ab.band,ab.title_name,ab.pubcode,b.vm_name as vm,pdr.price,pdr.days,pdr.pu+ifnull(pdr.qb,0) as pu_qb,pdr.gv
    ,concat(round((pdr.pu+ifnull(pdr.qb,0))*100/pdr.gv,2),'%') as cr
    ,pdr.prior_gms
    ,pdr.recent_gms
    ,case when abs(pdr.highest_price-pdr.price)<=0.1 then pdr.price else pdr.highest_price end as highest_price
    ,b.p_our_price as pop
    ,concat(round(b.our_price*100/b.p_our_price,1),'%') as ep_op_ratio
    ,pm.competitor_id
  from tam_db.price_duration_recent pdr
  inner join tam_db.all_bands ab on pdr.asin=ab.asin
  inner join tam_db.asin_base b on pdr.asin=b.asin and b.kindle_suppression_state not like '%COMPL%' and b.category_name='文学' and b.digital_pubcode!='YWGCN'
  left join tam_db.price_match pm on pm.asin=pdr.asin and entry_day_local = (select max(entry_day_local) from tam_db.price_match)
  left join tam_db.vendor_LTD vl on pdr.asin=vl.asin
where pdr.days>10 and pdr.gv/pdr.days>30 and (pdr.pu+ifnull(pdr.qb,0))/pdr.gv<0.06 and pdr.price>10
and ifnull(vl.set_flag,'N')!='Y' and ab.title_name not like '%文集%'
and pdr.recent_gms<pdr.prior_gms*1.6
order by (pdr.pu+ifnull(pdr.qb,0))/pdr.gv
    '''
hist_sql = '''
 insert tam_db.taurus_hist 
       select curdate() as ss_day,'D' as dir,pdr.asin,ab.band,ab.title_name,ab.pubcode,pdr.price
    ,case when pdr.prior_units=0 then null else round(pdr.prior_gms/pdr.prior_units,2) end as prior_price
    ,pdr.days,pdr.pu+ifnull(pdr.qb,0) as pu_qb,pdr.gv,round((pdr.pu+ifnull(pdr.qb,0))/pdr.gv,4) as cr
    ,pdr.prior_gms
    ,pdr.recent_gms
    ,case when abs(pdr.highest_price-pdr.price)<=0.1 then pdr.price else pdr.highest_price end as highest_price
    ,b.p_our_price as pop
    ,round(b.our_price/b.p_our_price,3) as ep_op_ratio
    ,pm.competitor_id
  from tam_db.price_duration_recent pdr
  inner join tam_db.all_bands ab on pdr.asin=ab.asin
  inner join tam_db.asin_base b on pdr.asin=b.asin and b.kindle_suppression_state not like '%COMPL%' and b.category_name='文学' and b.digital_pubcode!='YWGCN'
  left join tam_db.price_match pm on pm.asin=pdr.asin and entry_day_local = (select max(entry_day_local) from tam_db.price_match)
  left join tam_db.vendor_LTD vl on pdr.asin=vl.asin
where pdr.days>10 and pdr.gv/pdr.days>30 and (pdr.pu+ifnull(pdr.qb,0))/pdr.gv<0.06 and pdr.price>10
and ifnull(vl.set_flag,'N')!='Y' and ab.title_name not like '%文集%'
and pdr.recent_gms<pdr.prior_gms*1.6
    '''

RAISE_SQL='''
select pdr.asin,ab.band,ab.title_name,ab.pubcode,b.vm_name as vm
    ,pdr.price as recent_price
    ,case when pdr.prior_units=0 then null else round(pdr.prior_gms/pdr.prior_units,2) end as prior_price
    ,pdr.days,pdr.pu+ifnull(pdr.qb,0) as pu_qb,pdr.gv
    ,concat(round((pdr.pu+ifnull(pdr.qb,0))*100/pdr.gv,2),'%') as cr
    ,pdr.prior_gms
    ,pdr.recent_gms
    ,case when abs(pdr.highest_price-pdr.price)<=0.1 then pdr.price else pdr.highest_price end as highest_price
    ,b.p_our_price as pop
    ,concat(round(b.our_price*100/b.p_our_price,1),'%') as ep_op_ratio
    ,pm.competitor_id
  from tam_db.price_duration_recent pdr
  inner join tam_db.all_bands ab on pdr.asin=ab.asin
  inner join tam_db.asin_base b on pdr.asin=b.asin and b.kindle_suppression_state not like '%COMPL%' and b.category_name='文学' and b.digital_pubcode!='YWGCN'
  left join tam_db.price_match pm on pm.asin=pdr.asin and entry_day_local = (select max(entry_day_local) from tam_db.price_match)
  left join tam_db.vendor_LTD vl on pdr.asin=vl.asin
where pdr.days>10 and pdr.gv/pdr.days>30 and (pdr.pu+ifnull(pdr.qb,0))/pdr.gv>=0.06
and pdr.price<=pdr.prior_gms*0.8/pdr.prior_units
and pdr.recent_gms<pdr.prior_gms
order by pdr.recent_gms/pdr.prior_gms
'''

HIST_RAISE_SQL='''
 insert tam_db.taurus_hist 
       select curdate() as ss_day,
    'R' as dir,
     pdr.asin,ab.band,ab.title_name,ab.pubcode
    ,pdr.price as recent_price
    ,case when pdr.prior_units=0 then null else round(pdr.prior_gms/pdr.prior_units,2) end as prior_price
    ,pdr.days,pdr.pu+ifnull(pdr.qb,0) as pu_qb,pdr.gv
    ,case when pdr.gv=0 then null else round((pdr.pu+ifnull(pdr.qb,0))/pdr.gv,4) end as cr
    ,pdr.prior_gms
    ,pdr.recent_gms
    ,case when abs(pdr.highest_price-pdr.price)<=0.1 then pdr.price else pdr.highest_price end as highest_price
    ,b.p_our_price as pop
    ,case when b.p_our_price=0 then null else round(b.our_price/b.p_our_price,3) end as ep_op_ratio
    ,pm.competitor_id
  from tam_db.price_duration_recent pdr
  inner join tam_db.all_bands ab on pdr.asin=ab.asin
  inner join tam_db.asin_base b on pdr.asin=b.asin and b.kindle_suppression_state not like '%COMPL%' and b.category_name='文学' and b.digital_pubcode!='YWGCN'
  left join tam_db.price_match pm on pm.asin=pdr.asin and entry_day_local = (select max(entry_day_local) from tam_db.price_match)
  left join tam_db.vendor_LTD vl on pdr.asin=vl.asin
where pdr.days>10 and pdr.gv/pdr.days>30 and pdr.gv>0 and pdr.prior_units>0 and (pdr.pu+ifnull(pdr.qb,0))/pdr.gv>=0.06
and pdr.price<=pdr.prior_gms*0.8/pdr.prior_units
and pdr.recent_gms<pdr.prior_gms
'''

# 邮件的Subject或者Email中包含的名字都是经过编码后的str，要正常显示，就必须decode
def decode_str(s):
    value, charset = decode_header(s)[0]
    # decode_header()返回一个list，因为像Cc、Bcc这样的字段可能包含多个邮件地址，所以解析出来的会有多个元素。上面的代码我们偷了个懒，只取了第一个元素。
    if charset:
        value = value.decode(charset)
    return value


# 文本邮件的内容也是str，还需要检测编码，否则，非UTF-8编码的邮件都无法正常显示
def guess_charset(msg):
    charset = msg.get_charset()
    if charset is None:
        content_type = msg.get('Content-Type', '').lower()
        pos = content_type.find('charset=')
        if pos >= 0:
            charset = content_type[pos + 8:].strip()
    return charset


def send_mail(fro, to, subject, text, files=[], mtype='plain'):
    #assert type(server) == dict
    #assert type(to) == list
    #assert type(files) == list

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['Subject'] = subject
    msg['To'] = ','.join(to)  # COMMASPACE==', '
    #msg['Date'] = formatdate(localtime=True)
    msg.attach(MIMEText(text,mtype,'utf-8'))

    for file in files:
        part = MIMEBase('application', 'octet-stream')  # 'octet-stream': binary data
        part.set_payload(open(file, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(file))
        msg.attach(part)

    import smtplib
    smtp = smtplib.SMTP('mail-relay.amazon.com')
    #smtp.login(server['user'], server['passwd'])
    smtp.sendmail(fro, to, msg.as_string())
    smtp.close()

def multilines_in_html(strs):
    temps = re.split('\r\n|\n',strs)
    if len(temps)<=1:
        return strs
    else:
        ret =""
        cnt = 0
        for line in temps:
            if len(line.strip())==0: continue
            cnt += 1
            if cnt==1:
                ret = line.strip()
            else:
                ret += "<br>" + line.strip()
        return ret


def process(msg,msgid,name):
    conn = tamCommonLib.getMysqlConnect()
    cursor = conn.cursor()
    cursor.execute(mail_sql)
    # header
    data = [[col[0] for col in cursor.description]]
    # data
    data.extend(cursor.fetchall())
    mailBody = tamCommonLib.table_html_with_rn(data, u'经过计算，建议对下列书目进行降价以促进销售额的提升：')

    #title list whose price need to be raised
    cursor.execute(RAISE_SQL)
    # header
    data = [[col[0] for col in cursor.description]]
    # data
    data.extend(cursor.fetchall())
    mailBody += '<p></p>'+tamCommonLib.table_html_with_rn(data, u'经过计算，建议对下列书目进行提价以促进销售额的提升：')

    send_mail('cn-tam-auto@amazon.com', [msg.get('From', "")],'Taurus', mailBody, [],'html')
    cursor.close()
    conn.close()

    return {'key_data': '', 'mtype': '#Taurus'}


if __name__ == '__main__':
    conn = pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password,
                           db=mysql_default_db, port=3306, local_infile=1, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    cursor.execute('delete from tam_db.taurus_hist where ss_day=curdate()')
    cursor.execute(hist_sql)
    cursor.execute(HIST_RAISE_SQL)
    conn.commit()
    cursor.close()
    conn.close()

    exit()
