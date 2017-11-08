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

import xlrd
import datetime

## comment variables ##########################################
## time zone
mysql_host = 'ud094661c879c59fa6e9e'
mysql_user = '***'
mysql_password = '****'
mysql_default_db = 'tam_db'
work_path = './'
## mysql configuration
importlib.reload(sys)

##logging configuration
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)3d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %a %H:%M:%S',
                filename=work_path+'/processMail.log',
                filemode='w')

###############################################################
months = {'jan':'01','feb':'02','mar':'03','apr':'04','may':'05','jun':'06','jul':'07','aug':'08','sep':'09','oct':'10','nov':'11','dec':'12'}
allow_addresses = ['@ama.com','@a9.com']

htmlBody = '''
<p class="MsoNormal">
      <span lang=EN-US style='font-family:Wingdings;mso-fareast-font-family:Wingdings;mso-bidi-font-family:Wingdings;color:#1F497D'><span style='mso-list:Ignore'>Ø<span style='font:7.0pt "Times New Roman"'>&nbsp; </span></span></span>
      <span style='font-size:15px;font-family:宋体;color:#1F497D'>本次书单共计%s本，请在最后六列填写信息</span><br>
      <span style='font-size:13px;font-family:宋体;color:#1F497D'>注意：最后一列可以填写（Updated,Pending,OK,Fxied),OK/Fixed表示处理完毕的书目将不在下次显示</span><br>
</p>
<div style="font-size:15px;font-family:Arial">
    <table id="metalist" style="border:1px solid #cad9ea; color:#666;font-size:14px;">
        <tbody>
        <tr>
            <td style="border:1px solid #cad9ea;  font-weight:bold"></td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">source</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">ASIN</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">title</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">pubcode</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">a_band</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">f_band</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">o_ep_dis</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">l_ep_dis</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">tam_ops</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">ss_day</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">age</td>  
            <td style="border:1px solid #cad9ea;  font-weight:bold">Issue_Title_Name</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">Issue_PD</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">Issue_Cover</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">Issue_Author</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">Issue_Other</td>
            <td style="border:1px solid #cad9ea;  font-weight:bold">Fix_Status</td>
        </tr>
        %s
        </tbody>
    </table>
</div>
'''

clearStatus = ['ok','fixed']

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


def processMetaData(msg,msgid,name):
    subject = ""
    for header in ["From", "To", "Subject", "Cc"]:
        value = msg.get(header, "")
        if value:
            if header == "Subject":
                value = decode_str(value)
        print("%s:%s" % (header, value))
        if header == "Subject": subject = value

    conn = pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password,
                           db=mysql_default_db, port=3306, local_infile=1, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    key_cols = ['asin','src','ss_day','issue_Title_Name','issue_PD','issue_Cover','issue_author','fix_status']
    for part in msg.walk():
        fileName = part.get_filename()
        #read data from excel
        if fileName:
            data = part.get_payload(decode=True)
            fname = decode_str(fileName)
            fEx = open("%s" % (fname), 'wb')
            fEx.write(data)
            fEx.close()
            if fname.find('.xls') >= 0:
                data = xlrd.open_workbook(fname)
                table = data.sheet_by_index(0)
                nrows = table.nrows
                ncols = table.ncols
                key_cols_idx = {}
                data = []
                for id in range(ncols):
                    temp = str(table.cell(0, id).value).strip().lower()
                    if temp in [key_cols[i].lower() for i in range(len(key_cols))]:
                        key_cols_idx[temp] = id
                for row in range(1,nrows):
                    if len(str(table.cell(row, key_cols_idx['asin']).value).strip()) > 0 and ( len(str(table.cell(row, key_cols_idx['fix_status']).value).strip()) > 0 \
                         or len(str(table.cell(row, key_cols_idx['issue_title_name']).value).strip()) > 0 or len(str(table.cell(row, key_cols_idx['issue_pd']).value).strip()) > 0 \
                         or len(str(table.cell(row, key_cols_idx['issue_cover']).value).strip()) > 0 or len(str(table.cell(row, key_cols_idx['issue_author']).value).strip()) > 0 ):
                        validData = True
                        if len(re.findall(r'(B\w{9})',str(table.cell(row, key_cols_idx['asin']).value).strip()))<1: validData=False
                        if str(table.cell(row, key_cols_idx['src']).value).strip() not in ('PTS','ABC','DOTD'):   validData=False
                        if len(re.findall(r'(\d{4}-\d{1,2}-\d{1,2})', str(table.cell(row, key_cols_idx['ss_day']).value).strip())) < 1: validData = False
                        if validData==True:
                            temp =[]
                            for key in key_cols:
                                if key=='fix_status' and len(str(table.cell(row, key_cols_idx[key.lower()]).value).strip())==0:
                                    temp.append('Y')
                                else:
                                    temp.append(str(table.cell(row, key_cols_idx[key.lower()]).value).strip())
                            data.append(temp)
                if len(data) > 0:
                    cursor.execute("create temporary table if not exists mcl_cp as select * from accesslog.metadata_check_list where 0=1")
                    cursor.execute("truncate table mcl_cp")
                    conn.commit()
                    for row in data:
                        cursor.execute("insert into mcl_cp(asin,src,ss_day,issue_TA,issue_PD,issue_Cover,issue_author,fix_status,msgid) \
                                    values ('%s','%s','%s','%s','%s','%s','%s','%s',%d)"  % ( row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], msgid ) )
                    sql = "delete a1 from mcl_cp a1, accesslog.metadata_check_list a2 where a1.asin=a2.asin and ifnull(a1.issue_TA,'')=ifnull(a2.issue_TA,'') \
 and ifnull(a1.issue_PD,'')=ifnull(a2.issue_PD,'') and ifnull(a1.issue_Cover,'')=ifnull(a2.issue_Cover,'') and ifnull(a1.issue_author,'')=ifnull(a2.issue_author,'') \
 and ifnull(a1.fix_status,'')=ifnull(a2.fix_status,'')"
                    cursor.execute(sql)
                    conn.commit()
                    cursor.execute("delete from accesslog.metadata_check_list where asin in (select asin from mcl_cp)")
                    conn.commit()
                    cursor.execute("insert accesslog.metadata_check_list(asin,src,ss_day,issue_TA,issue_PD,issue_Cover,issue_author,issue_other,fix_status,msgid) \
                                      select asin,src,ss_day,issue_TA,issue_PD,issue_Cover,issue_author,issue_other,fix_status,msgid from mcl_cp")
                    cursor.execute("drop table mcl_cp")
                    conn.commit()
            if os.path.exists(fname):
                os.remove(fname)

    fro = re.findall(r'([a-zA-Z0-9_\-\.]+@[a-zA-Z0-9]+\.[a-zA-Z0-9]+)', msg.get('From', ""))[0]
    replyMail(cursor,fro)
    cursor.close()
    conn.close()
    return {'key_data': '', 'mtype': '#metadata'}


def replyMail(cursor,fro):
    all_asins = {}

    # read data from metadata status table
    ok_str = "','".join(clearStatus)
    sql = "select src,asin,ss_day,datediff(sysdate(), ss_day) as age, \
           issue_TA,issue_PD,issue_Cover,issue_author,issue_other,fix_status from accesslog.metadata_check_list \
           where lower(fix_status) not in ('%s')" % ok_str
    cursor.execute(sql)
    for rows in cursor.fetchall():
        if all_asins.get(rows['asin']) is None:
            all_asins[rows['asin']] = [rows['src'], str(rows['ss_day']), str(rows['age']),   \
                rows['issue_TA'],rows['issue_PD'],rows['issue_Cover'],rows['issue_author'],rows['issue_other'],rows['fix_status']]

    # read data from DOTD disk file
    path = '/mnt/ant/promotion/sgx/DOTD'
    years = os.listdir(path)
    years.sort(reverse=True)
    path = path + os.sep + years[0]
    latest_file = ""
    for ff in os.listdir(path):
        #only deal with the excel files
        if ff.find(".xls")<0: continue
        if len(latest_file) == 0:
            latest_file = ff
        else:
            if os.stat(path + os.sep + ff).st_mtime > os.stat(path + os.sep + latest_file).st_mtime:
                latest_file = ff
    data = xlrd.open_workbook(path + os.sep + latest_file)
    table = data.sheet_by_index(0)
    nrows = table.nrows
    asin_col_pos = -1
    schedule_col_pos = -1
    for j in range(5):
        temp = str(table.cell(0, j).value).strip().lower()
        if temp.find('asin') >= 0:
            asin_col_pos = j
        elif temp.find('schedule') >= 0 or temp.find('time') >= 0 or temp.find('date') >= 0:
            schedule_col_pos = j
    # sometime, the first column header may be empty, assume it was the schedule column
    if schedule_col_pos < 0: schedule_col_pos = 0
    cur = datetime.datetime.now()
    if asin_col_pos >= 0 and schedule_col_pos >= 0:
        for i in range(1, nrows):
            asin = str(table.cell(i, asin_col_pos).value).strip()
            if table.cell(i, schedule_col_pos).ctype==3:
                year1, month1, day1, hour, minute, second = xlrd.xldate_as_tuple(table.cell(i, schedule_col_pos).value,data.datemode)
                ss_day = "%s-%s-%s" % (str(year1), str(month1), str(day1))
            else:
                schedule = str(table.cell(i, schedule_col_pos).value)
                month, day, year = '', '', ''
                if len(re.findall(r'^(\d+)/(\d+)$', schedule))>0:
                    month, day = re.findall(r'^(\d+)/(\d+)$', schedule)[0]
                elif len(re.findall(r'^(\d+)/(\d+)/(\d+)$', schedule))>0:
                    month, day, year = re.findall(r'^(\d+)/(\d+)/(\d+)$', schedule)[0]
                if len(year)==0:
                    if abs(cur.month - int(month)) > 6:
                        ss_day = "%s-%s-%s" % (str(cur.year + 1), month, day)
                    else:
                        ss_day = "%s-%s-%s" % (str(cur.year), month, day)
                else:
                    ss_day = "%s-%s-%s" % (year, month, day)
            age = (datetime.datetime.now().date() - datetime.datetime.strptime(ss_day,'%Y-%m-%d').date()).days
            if len(re.findall(r'(B\w{9})',asin))>0 and all_asins.get(asin) is None:
                all_asins[asin] = ['DOTD', ss_day, str(age),'','','','','','']  # age is empty for DOTD

    # read data from week_abc table
    sql = '''
    select snapshot_day,kasin as asin,datediff(sysdate(), snapshot_day) as age 
from tam_db.week_abc where snapshot_day=(select max(snapshot_day) from tam_db.week_abc) group by snapshot_day,kasin
union all
select min(snapshot_day),asin,datediff(sysdate(), min(snapshot_day)) as age 
from tam_db.io_abc ia where date_add(sysdate(),interval -7 day) and io='IN' 
 and not exists (
  select 1 from tam_db.week_abc wa where ia.asin=wa.kasin and snapshot_day=(select max(snapshot_day) from tam_db.week_abc)
)
group by asin
'''
    cursor.execute(sql)
    snapshot_day = ""
    for rows in cursor.fetchall():
        snapshot_day = str(rows['snapshot_day'])
        if all_asins.get(rows['asin']) is None:
            all_asins[rows['asin']] = ['ABC', str(rows['snapshot_day']), str(rows['age']),'','','','','','']

    #asin_cond = "','".join(all_asins.keys())
    sql = "create temporary table if not exists tt  \
            (  asin char(13),  \
               src  char(5), \
               ss_day date ) "
    cursor.execute(sql)
    for kk in all_asins:
        cursor.execute("insert tt values('%s','%s','%s')" % (kk, all_asins[kk][0], all_asins[kk][1]))

    sql = u"SELECT ab.asin,left(ab.title_name,15) as title_name,ab.digital_pubcode as pubcode,ifnull(wa.a_band,ab.band) as a_band,ifnull(nt.band,ifnull(wa.f_band,'')) as f_band  \
                     ,round(ab.our_price/ab.p_our_price,2) as o_ep_dis,round(ab.list_price/ab.p_list_price,2) as l_ep_dis,v.tamops as tam_ops \
                     from tt \
                       inner join tam_db.asin_base ab on ab.asin = tt.asin and ab.kindle_suppression_state!='COMPLETELY_SUPPRESSED' \
                      left join tam_db.kcode_metrics v on ab.digital_pubcode = v.digital_pubcode \
                      left join tam_db.nos_titles nt on ab.asin=nt.kasin or ab.pasin=nt.pasin  \
                       left join ( select kasin,max(forecasted_band) as f_band,max(actual_band) as a_band from tam_db.week_abc where snapshot_day='%s' group by kasin) wa on ab.asin = wa.kasin \
                       order by tt.src desc,tt.ss_day" % snapshot_day

    cursor.execute(sql)
    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet("metadata", cell_overwrite_ok=True)
    col_heads = ['src','asin','title_name','pubcode','a_band','f_band','ss_day','age','tam_ops','fix_status','issue_Title_Name','issue_PD','issue_Cover','issue_author','o_ep_dis','l_ep_dis']
    for idx in range(len(col_heads)):
        sheet.write(0, idx, col_heads[idx])  # 写上字段信息
    cnt = 0
    fields = cursor.description
    style = xlwt.XFStyle()
    font = xlwt.Font()
    font.colour_index = 4
    style.font = font
    for data in cursor.fetchall():
        sql = "select asin from accesslog.metadata_check_list \
                           where asin ='%s' and lower(fix_status) in ('%s')  " % (data['asin'], ok_str)
        cursor.execute(sql)
        have_data = cursor.fetchone()
        if have_data is not None:
            # skip the row
            continue
        cnt +=1
        sheet.write(cnt, 0, all_asins[data['asin']][0]) #src
        sheet.write(cnt, 1, data['asin'])
        #sheet.write(cnt, 2, data['title_name'])
        sheet.write(cnt, 2, xlwt.Formula('HYPERLINK("https://www.amazon.cn/dp/%s";"%s")'%(data['asin'],data['title_name'])),style)
        sheet.write(cnt, 3, data['pubcode'])
        sheet.write(cnt, 4, data['a_band'])
        sheet.write(cnt, 5, data['f_band'])
        sheet.write(cnt, 6, all_asins[data['asin']][1])
        sheet.write(cnt, 7, all_asins[data['asin']][2])
        sheet.write(cnt, 8, data['tam_ops'])
        sheet.write(cnt, 9, all_asins[data['asin']][8]) #fix_status
        sheet.write(cnt, 10, all_asins[data['asin']][3])
        sheet.write(cnt, 11, all_asins[data['asin']][4])
        sheet.write(cnt, 12, all_asins[data['asin']][5])
        sheet.write(cnt, 13, all_asins[data['asin']][6])
        sheet.write(cnt, 14, data['o_ep_dis'])
        sheet.write(cnt, 15, data['l_ep_dis'])
    ff_name = './metadata_check_list.xls'
    workbook.save(ff_name)
    if cnt > 0:
        body ='''
        <p>Please see the attached data.</p>
        <p>After you edit on the file, pls send it back for saving.</p>
        <p>Note: The first header row must be kept for saving purpose.</p>
        <p>Once the fix_status become ok/fixed, it will not be on the list next time.</p>
        '''
        send_mail('cn-tam-auto@amazon.com', [fro], '[#Metadata] CheckList', body, [ff_name], 'html')
        if os.path.exists(ff_name):
            os.remove(ff_name)

if __name__ == '__main__':
    conn = pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password,
                                       db=mysql_default_db, port=3306, local_infile=1, charset='utf8',
                                       cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    replyMail(cursor, 'yongqis@amazon.com')
    cursor.close()
    conn.close()
