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

## comment variables ##########################################
## time zone
mysql_host = 'ud094661c879c59fa6e9e'
mysql_user = '***'
mysql_password = '***'
mysql_default_db = 'db'
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

nosContent = '''
<p class="MsoNormal">
      <b><span lang=ZH-CN style='font-size:10.0pt;font-family:DengXian;mso-ascii-font-family:楷体;mso-fareast-font-family:DengXian;mso-fareast-theme-font:minor-fareast;mso-hansi-font-family:楷体;color:gray'>未上线书单共计%s本：</span></b><o:p></o:p></p>
</p>
<div style="font-size:15px;font-family:Arial">
    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=0 style='margin-left:.1pt;border-collapse:collapse;mso-yfti-tbllook:1184;mso-padding-alt:0in 0in 0in 0in' id="cdnoslist">
        <tbody>
        <tr>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>序号</b></td>
            <td width=130 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>ISBN</b></td>
            <td width=70 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>PASIN</b></td>
            <td width=180 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>书名</b></td>
            <td width=120 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>作者</b></td>
            <td width=100 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>Pub_day</b></td>
            <td width=80 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>纸书排名</b></td>
            <td width=80 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>击中kindle热搜词</b></td>
            <td width=80 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>Deal_Intent</b></td>
        </tr>
        %s
        </tbody>
    </table>
</div>
'''
# 20170830
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

def processTamPattern(msg,msgid,name):
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
    for part in msg.walk():
        # fileName = part.get_filename()
        contentType = part.get_content_type()
        if contentType.find('image/') != -1: continue
        if contentType == 'text/plain' or contentType == 'text/html':
            # 保存正文
            content = part.get_payload(decode=True)
            charset = guess_charset(part)  # or msg
            if charset:
                content = content.decode(charset)
            # print('%sText: %s' % ('  ' * indent, content + '...'))
            soup = BeautifulSoup(content, 'lxml')
            # save the noslist
            table = soup.find('table', id='cdnoslist')
            if table is not None:
                line_no = 0
                asin_index = -1
                data = []
                for tr in table.findAll('tr'):
                    line_no += 1
                    col_no = 0
                    asin = ""
                    for td in tr.findAll('td'):
                        col_no += 1
                        if line_no == 1:
                            if td.getText().strip().lower() == "pasin":
                                asin_index = col_no
                        else:
                            if asin_index == col_no:
                                asin = td.getText().strip()
                    comments = td.getText().strip()  # last column for comments
                    if line_no != 1 and len(comments) > 0 and len(asin) > 0:
                        data.append((asin, comments, msgid))
                if len(data) > 0:
                    # print("\nstart to load the data into mysql database.")
                    fro = re.findall(r'([a-zA-Z0-9_\-\.]+@[a-zA-Z0-9]+\.[a-zA-Z0-9]+)', msg.get('From', ""))[0]
                    rec_time = re.findall(r'\w+, (\d{1,2}) (\w+) (\d{4}) (\d{2}:\d{2}:\d{2})', msg.get("Date", ""))[0]
                    rec_str = str(datetime.datetime.strptime("%s-%s-%s %s" % (rec_time[2], months[rec_time[1].lower()], rec_time[0], rec_time[3]),
                                                             "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8))
                    #cursor.execute("select digital_pubcode from tam_db.nos_titles where pasin='%s'" % data[0][0])
                    #pubs = cursor.fetchone()
                    #pubcode = ""
                    #if pubs is not None:
                    pubcode = 'DealIntent'
                    cursor.execute("select tech_mails,biz_mails,vm_mails from accesslog.pub_mail where pubcode='%s'" % pubcode)
                    mail_row = cursor.fetchone()
                    mail_lists = []
                    for mailtype in ('tech_mails', 'biz_mails', 'vm_mails'):
                        if mail_row is None or mail_row.get(mailtype) is None or len(mail_row[mailtype].strip()) == 0: continue
                        mail_lists.extend(re.split('[,;:]', mail_row[mailtype].strip()))
                    if fro in mail_lists:
                        for line in data:
                            print(line[0] + ":" + line[1])
                            sql = "delete from accesslog.mail_asin_audit where asin='%s' and msgid=%s" % (line[0], line[2])
                            cursor.execute(sql)
                            # sql = "insert into accesslog.mail_asin_audit(asin,comments,msgid) values ('%s','%s',%s)" % line
                            # cursor.execute(sql)
                            cursor.execute("insert into accesslog.mail_asin_audit(msgid,fro,receive,subject,type,asin,comments) values (%s,'%s','%s','%s','%s','%s','%s')" \
                                             % (line[2], fro, rec_str, subject, 'TamPattern:DealIntent', line[0], line[1]))
                            conn.commit()
            # save the catelist --20170830

            #20170830
            if len(pubcode) >0:
                replyMail(cursor,pubcode)
    cursor.close()
    conn.close()
    return {'key_data': '', 'mtype': '#DealIntent'}


def replyMail(cursor,pubcode):
    # Nos list
    mailBody = ""
    print (pubcode)
    #cursor.execute("select pubcode from accesslog.pub_rights where enable='y' and rights='noslist' and pubcode='%s'" % pubcode )
    if pubcode == "DealIntent":
        sql = '''select  
isbn13, ani.pasin, left(ani.title_name,20) as title_name, 
left(ani.author_name,8) as author_name,
ani.asin_creation_date as pub_day, pr.rank as rank, 
case when fk.k is NULL then ' ' 
else left(fk.k,8) end keyword
from  tam_db.all_nos_info  ani
left join hera.pbook_rank pr
on pr.PASIN  = ani.pasin
left join (select distinct(asin) from accesslog.mail_asin_audit where type='TamPattern:DealIntent') maa
on maa.asin = ani.pasin
left join 
(select distinct(pasin) as pasin, kk.k as k from  metadata.PASIN_KEYWORD pk
inner join (
select distinct(keywords) k from tam_db.mail_A9 m where m.receive = (select max(receive) from tam_db.mail_A9)
) kk
on pk.keyword like concat('%',kk.k,'%')) fk
on fk.pasin = ani.pasin
where ani.pasin in(
SELECT distinct(pasin) FROM hera.tam_pattern_nos where
snapshot_day = (select max(snapshot_day) from hera.tam_pattern_nos))
and isbn13 <> ''
and digital_pubcode <> ''
and isbn13 not in (select isbn from tam_db.all_bands)
ORDER BY asin asc, asin_creation_date desc '''

        cursor.execute(sql)
        strs = ""
        cnt = 0
        fields = cursor.description
        for data in cursor.fetchall():
            cnt += 1
            strs += '<tr><td width=30 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % cnt
            for field in fields:
                col = field[0]
                if col == 'ttm_gvc' and data[col] is not None:
                    strs += '<td width=49 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % str(round(10000 * data[col], 4))
                elif col == 'rank':
                    strs += '<td align="right" style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % data[col]
                else:
                    strs += '<td style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % data[col]
            # get last comments
            sql = "select comments from accesslog.mail_asin_audit where asin='%s' and type='TamPattern:DealIntent' order by receive desc" % \
                  data['pasin']
            cursor.execute(sql)
            comment_row = cursor.fetchone()
            if comment_row is None:
                strs += '<td width=80 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black"></span></td></tr>'
            else:
                strs += '<td width=80 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td></tr>' % multilines_in_html(comment_row['comments'])
        mailBody += nosContent % (str(cnt), strs)

    if len(mailBody) > 0:
        cursor.execute("select tech_mails,biz_mails,vm_mails from accesslog.pub_mail where pubcode='%s'" % pubcode)
        mail_row = cursor.fetchone()
        mail_lists = []
        for mailtype in ('tech_mails', 'biz_mails', 'vm_mails'):
            if mail_row is None or mail_row.get(mailtype) is None or len(mail_row[mailtype].strip()) == 0: continue
            mail_lists.extend(re.split('[,;:]', mail_row[mailtype].strip()))
        if len(mail_lists) > 0:
            send_mail('cn-tam-auto@amazon.com', mail_lists, '[#TPNEWNOS-DealIntent]', mailBody, [], 'html')
            # setting the last_update value in pub_rights table
            sql = "update accesslog.pub_rights set last_update='%s' where pubcode='%s' and enable='y'" \
                  % ( time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), pubcode)
            cursor.execute(sql)
        #else:
        #    send_mail('cn-tam-auto@amazon.com', ['yongqis@amazon.com'], 'Empty mail lists for pubcode:' + pubcode, 'Please fill the mail address correctly', [])

if __name__ == '__main__':
    conn = pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password,
                                       db=mysql_default_db, port=3306, local_infile=1, charset='utf8',
                                       cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    replyMail(cursor,'DealIntent')
    cursor.close()
    conn.close()
