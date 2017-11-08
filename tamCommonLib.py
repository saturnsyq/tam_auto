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
mysql_default_db = '***'

###############################################################

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

def getMysqlConnect():
    return pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password, db=mysql_default_db, port=3306, local_infile=1, charset='utf8mb4')

def table_html(tabledata,desc=''):
    if len(tabledata)<2: return ''
    html_str=''
    if desc is not None and len(desc)>0:
        html_str+='''
<p class="MsoNormal">
      <b><span lang=ZH-CN style='font-size:10.0pt;font-family:DengXian;mso-ascii-font-family:楷体;mso-fareast-font-family:DengXian;mso-fareast-theme-font:minor-fareast;mso-hansi-font-family:楷体;color:gray'>%s</span></b><o:p></o:p></p>
</p>
        ''' % desc
    html_str+='''
<div style="font-size:15px;font-family:Arial">
    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=0 style='margin-left:.1pt;border-collapse:collapse;mso-yfti-tbllook:1184;mso-padding-alt:0in 0in 0in 0in' id="noslist">
        <tbody>
        <tr>
    '''
    for col in tabledata[0]:
        if col is not None:
            html_str += "<td nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b>%s</b></td>" % str(col)
        else:
            html_str += "<td nowrap style='border:none;border-top:solid windowtext 1.0pt;background:#BFBFBF;padding:0in 5.4pt 0in 5.4pt;height:5.75pt'><span lang=ZH-CN style='font-size:9.0pt;font-family:宋体;color:black'><b></b></td>"
    html_str +='  </tr>'
    for row in tabledata[1:]:
        html_str +='<tr>'
        for field in row:
            if field is not None:
                html_str += '<td style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black">%s</span></td>' % field
            else:
                html_str += '<td style="border:none;border-top:solid windowtext 1.0pt;border-bottom:solid windowtext 1.0pt;padding:0in 5.4pt 0in 5.4pt;height:5.75pt"><span lang=ZH-CN style="font-size:9.0pt;font-family:宋体;color:black"></span></td>'
        html_str += '</tr>'
    html_str +='''
       </tbody>
    </table>
</div>
    '''
    return html_str

#table html with row number info
def table_html_with_rn(tabledata,desc=''):
    if len(tabledata) < 2: return ''
    new_tabledata=[]
    temp=['RN']
    temp.extend(tabledata[0])
    new_tabledata.append(temp)
    cnt = 0
    for row in tabledata[1:]:
        cnt +=1
        temp=[cnt]
        temp.extend(row)
        new_tabledata.append(temp)
    return table_html(new_tabledata,desc)

if __name__ == '__main__':

    exit()
