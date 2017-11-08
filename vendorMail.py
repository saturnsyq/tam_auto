#!/usr/bin/python3
# coding=utf-8

import imaplib
import email
from email.parser import Parser
from email.header import decode_header
from email.utils import parseaddr
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import COMMASPACE, formatdate
from email import encoders
from email.mime.image import MIMEImage

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
import gen_metadata_pub

## comment variables ##########################################
## time zone
mysql_host = 'ud094661c879c59fa6e9e'
mysql_user = '***'
mysql_password = '***'
mysql_default_db = '***'
work_path = './'
## mysql configuration
importlib.reload(sys)

##logging configuration
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)3d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %a %H:%M:%S',
                    filename=work_path + '/processMail.log',
                    filemode='w')

###############################################################
months = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
          'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}
allow_addresses = ['@amazon.com', '@a9.com']

nosContent = '''
<p class="MsoNormal">
      <b><span lang=ZH-CN style='font-size:10.0pt;font-family:DengXian;mso-ascii-font-family:楷体;mso-fareast-font-family:DengXian;mso-fareast-theme-font:minor-fareast;mso-hansi-font-family:楷体;color:gray'>未上线书单共计%s本，需填写状态</span></b><o:p></o:p></p>
</p>
<div style="font-size:15px;font-family:Arial">
    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=0 style='margin-left:.1pt;border-collapse:collapse;mso-yfti-tbllook:1184;mso-padding-alt:0in 0in 0in 0in' id="noslist">
        <tbody>
        <tr>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>序号</b></td>
            <td width=130 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>ISBN</b></td>
            <td width=70 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>PASIN</b></td>
            <td width=180 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>书名</b></td>
            <td width=100 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>作者</b></td>
            <td width=130 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>ttm_gvc(bps)</b><span style='font-size:9.0pt;color:black'>↓</span><span style='font-size:9.0pt;color:black'>↓</span></td>
            <td width=130 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>Pub_day</b><span style='font-size:9.0pt;color:black'>↓</span></td>
            <td width=200 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>状态</b></td>
            <td width=70 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>ETA</b></td>
        </tr>
        %s
        </tbody>
    </table>
</div>
'''
# <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>tam_pattern</b></td>
# <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>band</b></td>
# 20171101 delete
toplistContent = '''
<p class="MsoNormal">
      <b><span lang=ZH-CN style='font-size:10.0pt;font-family:DengXian;mso-ascii-font-family:楷体;mso-fareast-font-family:DengXian;mso-fareast-theme-font:minor-fareast;mso-hansi-font-family:楷体;color:gray'>头部产品列表</span></b><o:p></o:p></p>
</p>
<div style="font-size:15px;font-family:Arial">
    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=0 style='margin-left:.1pt;border-collapse:collapse;mso-yfti-tbllook:1184;mso-padding-alt:0in 0in 0in 0in' id="toplist">
        <tbody>
        <tr>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>序号</b></span></td>
            <td width=70 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>ASIN</b></span></td>
            <td width=130 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>书名</b></span></td>
            <td width=100 nowrap align="center" style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>销售额</b><span style='font-size:9.0pt;color:black'>↓</span></span></td>
            <td width=70 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>本书占比</b></span></td>
            <td width=70 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>累计占比</b></span></td>
            <td width=200 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>备注</b></span></td>
        </tr>
        %s
        </tbody>
    </table>
</div>
'''
# 20170830-add
# 20170905-change
subcategory = '''
<p class="MsoNormal"> 
      <b><span lang=ZH-CN style='font-size:10.0pt;font-family:DengXian;mso-ascii-font-family:楷体;mso-fareast-font-family:DengXian;mso-fareast-theme-font:minor-fareast;mso-hansi-font-family:楷体;color:gray'>头部类别列表</span></b><o:p></o:p></p>
</p>
<div style="font-size:15px;font-family:Arial">
    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=0 style='margin-left:.1pt;border-collapse:collapse;mso-yfti-tbllook:1184;mso-padding-alt:0in 0in 0in 0in' id="catelist">
        <tbody>
        <tr>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>序号</b></span></td>
            <td width=70 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>类别</b></span></td>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>销售额</b><span style='font-size:9.0pt;color:black'>↓</span></span></td>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>占比</b></span></td>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>KASIN</b></span></td>
            <td width=160 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>头部产品</b></span></td>
            <td width=200 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>备注</b></span></td>

        </tr>
        %s
        </tbody>
    </table>
</div>
'''
pic_tb = """    
<p class="MsoNormal"> 
      <b><span lang=ZH-CN style='font-size:10.0pt;font-family:DengXian;mso-ascii-font-family:楷体;mso-fareast-font-family:DengXian;mso-fareast-theme-font:minor-fareast;mso-hansi-font-family:楷体;color:gray'>供应商回款额</span></b><o:p></o:p></p>
</p>
<table width="" border="0" cellspacing="0" cellpadding="4">
    <tr bgcolor="" height="100" style="font-size:13px">
        <td style="margin: 1em 1em 1em 1em"><img src="cid:io" width="700"></td>
    </tr>
</table>
"""

# 20170830

# 20170909

topauthor = '''
<p class="MsoNormal"> 
      <b><span lang=ZH-CN style='font-size:10.0pt;font-family:DengXian;mso-ascii-font-family:楷体;mso-fareast-font-family:DengXian;mso-fareast-theme-font:minor-fareast;mso-hansi-font-family:楷体;color:gray'>头部作者列表</span></b><o:p></o:p></p>
</p>
<div style="font-size:15px;font-family:Arial">
    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=0 style='margin-left:.1pt;border-collapse:collapse;mso-yfti-tbllook:1184;mso-padding-alt:0in 0in 0in 0in' id="topauthor">
        <tbody>
        <tr>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>序号</b></span></td>
            <td width=90 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>作者</b></span></td>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>销售额</b><span style='font-size:9.0pt;color:black'>↓</span></span></td>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>占比</b></span></td>
            <td width=49 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>KASIN</b></span></td>
            <td width=160 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>书名</b></span></td>
            <td width=200 nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>备注</b></span></td>

        </tr>
        %s
        </tbody>
    </table>
</div>
'''


# 20170909

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


# Add sales img 20170913
def addimg(src, imgid):
    fp = open(src, 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()  # 关闭文件
    msgImage.add_header('Content-ID', imgid)
    return msgImage


def send_mail(fro, to, subject, text, files=[], mtype='plain', imgs=[]):
    # assert type(server) == dict
    # assert type(to) == list
    # assert type(files) == list

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['Subject'] = subject
    msg['To'] = ','.join(to)  # COMMASPACE==', '
    # msg['Date'] = formatdate(localtime=True)
    msg.attach(MIMEText(text, mtype, 'utf-8'))
    for img in imgs:
        msg.attach(addimg(img, "io"))

    for file in files:
        part = MIMEBase('application', 'octet-stream')  # 'octet-stream': binary data
        part.set_payload(open(file, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(file))
        msg.attach(part)

    import smtplib
    smtp = smtplib.SMTP('mail-relay.amazon.com')
    # smtp.login(server['user'], server['passwd'])
    smtp.sendmail(fro, to, msg.as_string())
    smtp.close()


def multilines_in_html(strs):
    temps = re.split('\r\n|\n', strs)
    if len(temps) <= 1:
        return strs
    else:
        ret = ""
        cnt = 0
        for line in temps:
            if len(line.strip()) == 0: continue
            cnt += 1
            if cnt == 1:
                ret = line.strip()
            else:
                ret += "<br>" + line.strip()
        return ret


def processHiVendor(msg, msgid, name):
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
            # save the toplist
            table = soup.find('table', id='toplist')
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
                            if td.getText().strip().lower() == "asin":
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
                    rec_str = str(datetime.datetime.strptime(
                        "%s-%s-%s %s" % (rec_time[2], months[rec_time[1].lower()], rec_time[0], rec_time[3]),
                        "%Y-%m-%d %H:%M:%S") \
                                  + datetime.timedelta(hours=8))
                    cursor.execute("select digital_pubcode from tam_db.asin_base where asin='%s'" % data[0][0])
                    pubs = cursor.fetchone()
                    pubcode = ""
                    if pubs is not None:
                        pubcode = pubs['digital_pubcode']
                    cursor.execute(
                        "select tech_mails,biz_mails,vm_mails from accesslog.pub_mail where pubcode='%s'" % pubcode)
                    mail_row = cursor.fetchone()
                    mail_lists = []
                    for mailtype in ('tech_mails', 'biz_mails', 'vm_mails'):
                        if mail_row is None or mail_row.get(mailtype) is None or len(
                                mail_row[mailtype].strip()) == 0: continue
                        mail_lists.extend(re.split('[,;:]', mail_row[mailtype].strip()))
                    if fro in mail_lists:
                        for line in data:
                            print(line[0] + ":" + line[1])
                            sql = "delete from accesslog.mail_asin_audit where asin='%s' and msgid=%s and type='HiVendor:toplist'" % (
                            line[0], line[2])
                            cursor.execute(sql)
                            # sql = "insert into accesslog.mail_asin_audit(asin,comments,msgid) values ('%s','%s',%s)" % line
                            # cursor.execute(sql)
                            cursor.execute(
                                "insert into accesslog.mail_asin_audit(msgid,fro,receive,subject,type,asin,comments) values (%s,'%s','%s','%s','%s','%s','%s')" \
                                % (line[2], fro, rec_str, subject, 'HiVendor:toplist', line[0], line[1]))
                            conn.commit()
                            # pargs = ["/usr/bin/python3", "./vendorMail.py", pubcode]
                            # subprocess.Popen(pargs)

            # save the noslist
            table = soup.find('table', id='noslist')
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
                            if td.getText().strip() == "状态":
                                comments_index = col_no
                        else:
                            if asin_index == col_no:
                                asin = td.getText().strip()
                            if comments_index == col_no:
                                comments = td.getText().strip()  # get comments
                    eta = td.getText().strip()  # last column for comments
                    if line_no != 1 and (len(comments) > 0 or len(eta) > 0) and len(asin) > 0:
                        data.append((asin, comments, msgid, eta))
                if len(data) > 0:
                    # print("\nstart to load the data into mysql database.")
                    fro = re.findall(r'([a-zA-Z0-9_\-\.]+@[a-zA-Z0-9]+\.[a-zA-Z0-9]+)', msg.get('From', ""))[0]
                    rec_time = re.findall(r'\w+, (\d{1,2}) (\w+) (\d{4}) (\d{2}:\d{2}:\d{2})', msg.get("Date", ""))[0]
                    rec_str = str(datetime.datetime.strptime(
                        "%s-%s-%s %s" % (rec_time[2], months[rec_time[1].lower()], rec_time[0], rec_time[3]),
                        "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8))
                    cursor.execute("select digital_pubcode from tam_db.nos_titles where pasin='%s'" % data[0][0])
                    pubs = cursor.fetchone()
                    pubcode = ""
                    if pubs is not None:
                        pubcode = pubs['digital_pubcode']
                    cursor.execute(
                        "select tech_mails,biz_mails,vm_mails from accesslog.pub_mail where pubcode='%s'" % pubcode)
                    mail_row = cursor.fetchone()
                    mail_lists = []
                    for mailtype in ('tech_mails', 'biz_mails', 'vm_mails'):
                        if mail_row is None or mail_row.get(mailtype) is None or len(
                            mail_row[mailtype].strip()) == 0: continue
                        mail_lists.extend(re.split('[,;:]', mail_row[mailtype].strip()))
                    if fro in mail_lists:
                        for line in data:
                            print(line[0] + ":" + line[1])
                            sql = "delete from accesslog.mail_asin_audit where asin='%s' and msgid=%s and type='HiVendor:noslist'" % (
                            line[0], line[2])
                            cursor.execute(sql)
                            # sql = "insert into accesslog.mail_asin_audit(asin,comments,msgid) values ('%s','%s',%s)" % line
                            # cursor.execute(sql)
                            cursor.execute(
                                "insert into accesslog.mail_asin_audit(msgid,fro,receive,subject,type,asin,comments) values (%s,'%s','%s','%s','%s','%s','%s')" \
                                % (line[2], fro, rec_str, subject, 'HiVendor:noslist', line[0], line[1]))
                            conn.commit()
                            if len(line[3]) > 0:
                                sql = "delete from accesslog.mail_asin_audit where asin='%s' and msgid=%s and type='HiVendor:noseta'" % (
                                line[0], line[2])
                                cursor.execute(sql)
                                cursor.execute(
                                    "insert into accesslog.mail_asin_audit(msgid,fro,receive,subject,type,asin,comments) values (%s,'%s','%s','%s','%s','%s','%s')" % (
                                    line[2], fro, rec_str, subject, 'HiVendor:noseta', line[0], line[3]))
                                conn.commit()
            # save the catelist --20170830
            table = soup.find('table', id='catelist')
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
                            if td.getText().strip().lower() == "kasin":
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
                    rec_str = str(datetime.datetime.strptime(
                        "%s-%s-%s %s" % (rec_time[2], months[rec_time[1].lower()], rec_time[0], rec_time[3]),
                        "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8))
                    cursor.execute("select digital_pubcode from tam_db.asin_base where asin='%s'" % data[0][0])
                    pubs = cursor.fetchone()
                    pubcode = ""
                    if pubs is not None:
                        pubcode = pubs['digital_pubcode']
                    cursor.execute(
                        "select tech_mails,biz_mails,vm_mails from accesslog.pub_mail where pubcode='%s'" % pubcode)
                    mail_row = cursor.fetchone()
                    mail_lists = []
                    for mailtype in ('tech_mails', 'biz_mails', 'vm_mails'):
                        if mail_row is None or mail_row.get(mailtype) is None or len(
                            mail_row[mailtype].strip()) == 0: continue
                        mail_lists.extend(re.split('[,;:]', mail_row[mailtype].strip()))
                    if fro in mail_lists:
                        for line in data:
                            print(line[0] + ":" + line[1])
                            sql = "delete from accesslog.mail_asin_audit where asin='%s' and msgid=%s and type = 'HiVendor:catelist'" % (
                            line[0], line[2])
                            cursor.execute(sql)
                            # sql = "insert into accesslog.mail_asin_audit(asin,comments,msgid) values ('%s','%s',%s)" % line
                            # cursor.execute(sql)
                            cursor.execute(
                                "insert into accesslog.mail_asin_audit(msgid,fro,receive,subject,type,asin,comments) values (%s,'%s','%s','%s','%s','%s','%s')" \
                                % (line[2], fro, rec_str, subject, 'HiVendor:catelist', line[0], line[1]))
                            conn.commit()
            # 20170830
            # 20170909 topauthor
            table = soup.find('table', id='topauthor')
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
                            if td.getText().strip().lower() == "kasin":
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
                    rec_str = str(datetime.datetime.strptime(
                        "%s-%s-%s %s" % (rec_time[2], months[rec_time[1].lower()], rec_time[0], rec_time[3]),
                        "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8))
                    cursor.execute("select digital_pubcode from tam_db.asin_base where asin='%s'" % data[0][0])
                    pubs = cursor.fetchone()
                    pubcode = ""
                    if pubs is not None:
                        pubcode = pubs['digital_pubcode']
                    cursor.execute(
                        "select tech_mails,biz_mails,vm_mails from accesslog.pub_mail where pubcode='%s'" % pubcode)
                    mail_row = cursor.fetchone()
                    mail_lists = []
                    for mailtype in ('tech_mails', 'biz_mails', 'vm_mails'):
                        if mail_row is None or mail_row.get(mailtype) is None or len(
                            mail_row[mailtype].strip()) == 0: continue
                        mail_lists.extend(re.split('[,;:]', mail_row[mailtype].strip()))
                    if fro in mail_lists:
                        for line in data:
                            print(line[0] + ":" + line[1])
                            sql = "delete from accesslog.mail_asin_audit where asin='%s' and msgid=%s and type='HiVendor:topauthor'" % (
                            line[0], line[2])
                            cursor.execute(sql)
                            # sql = "insert into accesslog.mail_asin_audit(asin,comments,msgid) values ('%s','%s',%s)" % line
                            # cursor.execute(sql)
                            cursor.execute(
                                "insert into accesslog.mail_asin_audit(msgid,fro,receive,subject,type,asin,comments) values (%s,'%s','%s','%s','%s','%s','%s')" \
                                % (line[2], fro, rec_str, subject, 'HiVendor:topauthor', line[0], line[1]))
                            conn.commit()
            # 20170909 topauthor
            if len(pubcode) > 0:
                # print('Pubcode is %s'%pubcode)
                replyMail(cursor, pubcode)
    cursor.close()
    conn.close()
    return {'key_data': '', 'mtype': '#HiVendorPro'}


def replyMail(cursor, pubcode):
    # Nos list
    mailBody = ""
    metaData_dir = ""
    cursor.execute(
        "select pubcode from accesslog.pub_rights where enable='y' and rights='imgsales' and pubcode='%s'" % pubcode)
    imgdir = ""
    if cursor.rowcount > 0:
        mailBody += pic_tb
        imgdir = "/mnt/wind/images/Rhea/" + pubcode + ".png"
    cursor.execute(
        "select pubcode from accesslog.pub_rights where enable='y' and rights='noslist' and pubcode='%s'" % pubcode)
    if cursor.rowcount > 0:
        sql = '''select isbn13, pasin, left(title_name,12) as title_name, left(author_name,8) as author_name, ttm_gvc,ntt.pbook_publication_day as pub_day from tam_db.nos_titles  ntt
        where ntt.tam_pattern = 'Y' 
        AND digital_pubcode =  '%s'
        and onsite_flag = 0
        AND COPYRIGHT_GET_ETA IS NULL
        and isbn13 not in(select isbn from tam_db.all_bands)
        and PBOOK_PUBLICATION_DAY >= '20170101'
        ORDER BY ntt.pbook_publication_day desc,ttm_gvc DESC ''' % pubcode
        # tam_pattern,band,
        # 20171101 delete
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
                    strs += '<td width=49 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % str(
                        round(10000 * data[col], 4))
                else:
                    strs += '<td style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % \
                            data[col]
            # get last comments
            sql = "select comments from accesslog.mail_asin_audit where asin='%s' and type='HiVendor:noslist' order by receive desc" % \
                  data['pasin']
            cursor.execute(sql)
            comment_row = cursor.fetchone()
            if comment_row is None:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black"></span></td>'
            else:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % multilines_in_html(
                    comment_row['comments'])
            sql = "select comments from accesslog.mail_asin_audit where asin='%s' and type='HiVendor:noseta' order by receive desc" % \
                  data['pasin']
            cursor.execute(sql)
            eta_row = cursor.fetchone()
            if eta_row is None:
                strs += '<td width=70 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black"></span></td>'
            else:
                strs += '<td width=70 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % multilines_in_html(
                    eta_row['comments'])
        mailBody += nosContent % (str(cnt), strs)
        # metaData_dir="/mnt/wind/LJ/metadata/data/MTMCN_20171012_091943.xlsx"
        metaData_dir = gen_metadata_pub.gen_metadata(pubcode)
    # toplist
    cursor.execute(
        "select pubcode from accesslog.pub_rights where enable='y' and rights='toplist' and pubcode='%s'" % pubcode)
    if cursor.rowcount > 0:
        sql = "SELECT r.asin,r.title_name,format(r.ttm_gms,0) as ytd_gms,round(r.ttm_gms/t1.gms,4) as dis from tam_db.all_bands r,(select sum(ttm_gms) as gms \
              from tam_db.all_bands where PUBCODE='%s') t1 where PUBCODE='%s' order by r.ttm_gms desc limit 0,10" % (
        pubcode, pubcode)
        cursor.execute(sql)
        sum_dis = 0
        strs = ""
        cnt = 0
        total_cnt = cursor.rowcount
        for data in cursor.fetchall():
            cnt += 1
            sum_dis += data['dis']
            strs += '<tr><td width=30 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % cnt
            for col in ('asin', 'title_name', 'ytd_gms', 'dis'):
                if col == 'dis':
                    strs += '<td width=49 nowrap align="center" style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s%%</td>' % str(
                        round(100 * data[col], 2))
                elif col == 'ytd_gms':
                    strs += '<td width=100 align="right" style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                            data[col]
                elif col == 'title_name':
                    strs += '<td style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                            data[col]
                else:
                    strs += '<td align="left" style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                            data[col]
            strs += '<td width=49 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s%%</td>' % str(
                round(100 * sum_dis, 2))
            # get last comments
            sql = "select comments from accesslog.mail_asin_audit where asin='%s' and type='HiVendor:toplist' order by receive desc" % \
                  data['asin']
            cursor.execute(sql)
            comment_row = cursor.fetchone()
            if comment_row is None:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black"></td></tr>'
            else:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td></tr>' % multilines_in_html(
                    comment_row['comments'])
            if sum_dis > 0.8:
                break
        if cnt >= 1:
            # mailBody += toplistContent % (str(cnt), str(total_cnt), str(round(100 * cnt / total_cnt, 0)), strs)
            mailBody += toplistContent % strs
    # category --20170830
    cursor.execute(
        "select pubcode from accesslog.pub_rights where enable='y' and rights='catelist' and pubcode='%s'" % pubcode)
    if cursor.rowcount > 0:
        sql = "select cc.sub_category_name, format(cc.gms,0) as gms,round(cc.ratio*100,0) as ratio," \
              "bb.asin,bb.title_name from (SELECT ll.sub_category_name, sum(k_ytd_gms) as gms," \
              "sum(k_ytd_gms)*100/(select sum(k_ytd_gms)*100 as ratio  from tam_db.asin_base where digital_pubcode  =  '%s') as ratio " \
              "FROM tam_db.asin_base ll " \
              "WHERE digital_pubcode  =  '%s' " \
              "group by sub_category_name " \
              "order by 2 desc " \
              "limit 10 ) cc " \
              "inner join " \
              "(select ab.sub_category_name,asin, title_name from tam_db.asin_base ab " \
              "inner join  " \
              "(select sub_category_name,max(k_ytd_gms) as k_ytd_gms FROM tam_db.asin_base " \
              "WHERE digital_pubcode  =  '%s' " \
              "group by sub_category_name) aa " \
              "on aa.sub_category_name = ab.sub_category_name " \
              "and ab.digital_pubcode = '%s' " \
              "and ab.k_ytd_gms = aa.k_ytd_gms) bb " \
              "on bb.sub_category_name = cc.sub_category_name" % (pubcode, pubcode, pubcode, pubcode)
        cursor.execute(sql)
        strs = ""
        cnt = 0
        fields = cursor.description
        for data in cursor.fetchall():
            cnt += 1
            strs += '<tr><td width=30 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % cnt
            for field in fields:
                col = field[0]
                data[col]
                if col == 'sub_category_name':
                    if len(data[col]) == 0:
                        data[col] = r' '
                        strs += '<td width=160 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                                (data[col])
                    else:
                        data[col] = data[col].replace('/', '')
                        strs += '<td width=160 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                                (re.findall(r'[^()]+', data[col]))[1]
                elif col == 'ratio':
                    strs += '<td width=49 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s%%</td>' % \
                            data[col]
                elif col == 'gms':
                    strs += '<td width=49 nowrap align="right" style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                            data[col]
                else:
                    # print (data[col],len(data[col]))
                    col_align = 'left'
                    if len(data[col]) <= 10:
                        col_w = 49
                    else:
                        col_w = 300
                    strs += '<td width=%s align=%s nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % (
                    col_w, col_align, data[col])
            # get last comments
            sql = "select comments from accesslog.mail_asin_audit where asin='%s' and type='HiVendor:catelist' order by receive desc" % \
                  data['asin']
            cursor.execute(sql)
            comment_row = cursor.fetchone()
            if comment_row is None:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black"></td></tr>'
            else:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td></tr>' % multilines_in_html(
                    comment_row['comments'])
        mailBody += subcategory % strs
    # 20170830 category
    # 20170909
    cursor.execute(
        "select pubcode from accesslog.pub_rights where enable='y' and rights='topauthor' and pubcode='%s'" % pubcode)
    if cursor.rowcount > 0:
        sql = '''select left(trim(cc.author_name),12) as author_name, format(cc.gms,0) as gms,round(cc.ratio*100,0) as ratio,bb.asin,bb.title_name from (
         SELECT ll.author_name,sum(k_ytd_gms) as gms,
         sum(k_ytd_gms)*100/(select sum(k_ytd_gms)*100 as ratio  from tam_db.asin_base where digital_pubcode  =  '%s') as ratio 
         from tam_db.asin_base ll WHERE digital_pubcode = '%s' group by ll.author_name order by 2 desc limit 10 ) cc
         inner join 
         (select ab.author_name,asin, title_name from tam_db.asin_base ab 
         inner join
         (select author_name,max(k_ytd_gms) as k_ytd_gms FROM tam_db.asin_base WHERE digital_pubcode  = '%s' group by author_name ) aa
         on aa.author_name = ab.author_name
         and ab.digital_pubcode = '%s'
         and ab.k_ytd_gms = aa.k_ytd_gms) bb
         on bb.author_name = cc.author_name''' % (pubcode, pubcode, pubcode, pubcode)
        cursor.execute(sql)
        strs = ""
        cnt = 0
        fields = cursor.description
        for data in cursor.fetchall():
            cnt += 1
            strs += '<tr><td width=30 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % cnt
            for field in fields:
                col = field[0]
                data[col]
                if col == 'author_name':
                    strs += '<td width=160 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                            data[col]
                elif col == 'ratio':
                    strs += '<td width=49 nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s%%</td>' % \
                            data[col]
                elif col == 'gms':
                    strs += '<td width=49 nowrap align="right" style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % \
                            data[col]
                else:
                    # print (data[col],len(data[col]))
                    col_align = 'left'
                    if len(data[col]) <= 10:
                        col_w = 49
                    else:
                        col_w = 300
                    strs += '<td width=%s align=%s nowrap style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td>' % (
                    col_w, col_align, data[col])
            # get last comments
            sql = "select comments from accesslog.mail_asin_audit where asin='%s' and type='HiVendor:topauthor' order by receive desc" % \
                  data['asin']
            cursor.execute(sql)
            comment_row = cursor.fetchone()
            if comment_row is None:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black"></td></tr>'
            else:
                strs += '<td width=120 nowrap style="width:30.0pt;border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</td></tr>' % multilines_in_html(
                    comment_row['comments'])
        mailBody += topauthor % strs
        # 20170909

    if len(mailBody) > 0:
        cursor.execute("select tech_mails,biz_mails,vm_mails from accesslog.pub_mail where pubcode='%s'" % pubcode)
        mail_row = cursor.fetchone()
        mail_lists = []
        for mailtype in ('tech_mails', 'biz_mails', 'vm_mails'):
            if mail_row is None or mail_row.get(mailtype) is None or len(mail_row[mailtype].strip()) == 0: continue
            mail_lists.extend(re.split('[,;:]', mail_row[mailtype].strip()))
        if len(mail_lists) > 0:
            if len(imgdir) == 0:
                send_mail('cn-tam-auto@amazon.com', mail_lists, 'HiVendor Pro-' + pubcode, mailBody, [], 'html', [])
            elif len(imgdir) != 0 and len(metaData_dir) == 0:
                send_mail('cn-tam-auto@amazon.com', mail_lists, 'HiVendor Pro-' + pubcode, mailBody, [], 'html',
                          [imgdir])
            else:
                send_mail('cn-tam-auto@amazon.com', mail_lists, 'HiVendor Pro-' + pubcode, mailBody, [metaData_dir],
                          'html', [imgdir])
            # setting the last_update value in pub_rights table
            sql = "update accesslog.pub_rights set last_update='%s' where pubcode='%s' and enable='y'" \
                  % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), pubcode)
            cursor.execute(sql)
        else:
            send_mail('cn-tam-auto@amazon.com', ['yongqis@amazon.com'], 'Empty mail lists for pubcode:' + pubcode,
                      'Please fill the mail address correctly', [])


if __name__ == '__main__':
    conn = pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password,
                           db=mysql_default_db, port=3306, local_infile=1, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    pub_one = False
    if len(sys.argv) > 1:
        pub_from_argv = sys.argv[1].strip()
        pub_one = True
        cursor.execute(
            "select distinct pubcode from accesslog.pub_rights where enable='y' and pubcode='%s'" % pub_from_argv)
    else:
        cursor.execute(
            "select distinct pubcode from accesslog.pub_rights where enable='y' and ( last_update is null or date_format(last_update,'%Y-%m-%d')!='" \
            + time.strftime("%Y-%m-%d", time.localtime()) + "')")

    for row in cursor.fetchall():
        replyMail(cursor, row['pubcode'])
    cursor.close()
    conn.close()
