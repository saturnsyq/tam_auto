#!/usr/bin/python3
#coding=utf-8

import pymysql
import csv
import codecs
import chardet
import sys
import os,datetime
import time
import string
import pytz
import copy
import importlib
import re
import time
import logging

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

import subprocess

## connect to MySQL

## comment variables ##########################################
## time zone
utc = pytz.utc
tz = pytz.timezone('Asia/Shanghai')
d=datetime.datetime.now(tz)
mysql_host = 'ud094661c879c59fa6e9e'
mysql_user = '***'
mysql_password = '***'
mysql_default_db = '***'
work_path = '/mnt/wind/autoload'
## mysql configuration
importlib.reload(sys)

##logging configuration
logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)3d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %a %H:%M:%S',
                filename=work_path+'/MysqlDataLoader.log',
                filemode='w')
months = {'jan':'01','feb':'02','mar':'03','apr':'04','may':'05','jun':'06','jul':'07','aug':'08','sep':'09','oct':'10','nov':'11','dec':'12'}

#reminder mail address
reminder_mail_address = ["cn-kindle-mtam@amazon.com"]
#batchsize for loading
batchsize = 10000
###############################################################

def readFile(filePath):
    finput = open(filePath, "rb")
    str = finput.readline() + finput.readline() + finput.readline()  #在文件中多读取一行是因为，如果一行文字太少，编码检测有可能错误，按自己要求调整
    codeType = chardet.detect(str)["encoding"]  #检测编码方式
    if codeType == 'ascii': codeType = 'utf-8-sig'
    finput.close()
    content = []
    duplicate_remover = {}
    data=''
    with codecs.open(filePath, 'r', codeType, 'ignore') as fd:
        data = fd.read()
    lines = re.split('\r\n|\n', data)
    for line in lines:
        line = line.rstrip('\n').rstrip('\r')
        if len(line) > 0:
            if duplicate_remover.get(line) is None:
                content.append(line)
                duplicate_remover[line] = 1
            else:
                logging.info("remove the duplicated line data:%s" % line)
    return content

def send_mail(fro, to, subject, text, files=[], mtype='plain'):
    #assert type(server) == dict
    #assert type(to) == list
    #assert type(files) == list

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['Subject'] = subject
    msg['To'] = ','.join(to)  # COMMASPACE==', '
    #msg['Date'] = formatdate(localtime=True)
    msg.attach(MIMEText(text,mtype))

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

def ready_to_process(file):
    lag = time.time() - os.stat(file).st_mtime
    if lag > 60:
        return True
    else:
        return False

def clean_done_dir():
    for df in os.listdir(work_path+os.sep+'done'):
        full_path = work_path+os.sep+'done'+os.sep+df
        lag = time.time() - os.stat(full_path).st_mtime
        if lag > 60*60*24*14: #remove files in it is older than 2 weeks
            if os.path.exists(full_path):
                os.remove(full_path)

def fieldValidate(field,type):
    if type.find('char') >= 0 or type.find('text') >= 0:
        #char field, use \\ to replace \
        return field.replace('\\','\\\\')
    else:
        if field =='': return '\\N'
        if type.find('datetime') >= 0 or type.find('timestamp')>= 0:
            return field
        elif type=='date':
            temp = re.findall(r'^(\d{1,2})-(\w{3})-(\d{4})$',field)
            if len(temp) > 0:
                if months.get(temp[0][1].lower()) is None:
                    return '\\N'
                else:
                    return "%s-%s-%s" % (temp[0][2], months.get(temp[0][1].lower()), temp[0][0])
            temp = re.findall(r'^(\d{1,2})-(\w{3})-(\d{2})$', field)
            if len(temp) > 0:
                if months.get(temp[0][1].lower()) is None:
                    return '\\N'
                else:
                    return "20%s-%s-%s" % (temp[0][2], months.get(temp[0][1].lower()), temp[0][0])
            temp = re.findall(r'^(\d{4})/(\d{1,2})/(\d{1,2})$', field)
            if len(temp) >0:
                if int(temp[0][1])<=12 and int(temp[0][2])<=31:
                    return "%s-%s-%s" % (temp[0][0], temp[0][1], temp[0][2])
                else:
                    return '\\N'
            temp = re.findall(r'^(\d{4})-(\d{1,2})-(\d{1,2})$', field)
            if len(temp) > 0:
                if int(temp[0][1]) <= 12 and int(temp[0][2]) <= 31:
                    return "%s-%s-%s" % (temp[0][0], temp[0][1], temp[0][2])
                else:
                    return '\\N'
            temp = re.findall(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', field)
            if len(temp) >0:
                if int(temp[0][0])<=12 and int(temp[0][1])<=31:
                    return "%s-%s-%s" % (temp[0][2], temp[0][0], temp[0][1])
                else:
                    return '\\N'
            temp = re.findall(r'^(\d{4})(\d{2})(\d{2})$', field)
            if len(temp) > 0:
                if int(temp[0][1]) <= 12 and int(temp[0][2]) <= 31:
                    return "%s-%s-%s" % (temp[0][0], temp[0][1], temp[0][2])
                else:
                    return '\\N'
            #invlidate data
            return '\\N'
        elif type.find('int') >= 0:
            #integer type
            try:
                temp = int(field)
            except:
                #invalid number
                return '\\N'
            return field
        else:
            #float type
            tt = re.findall(r'^([0-9]*\.?[0-9]+|[0-9]+\.?[0-9]*)%$', field)
            if len(tt) == 0:
                try:
                    temp = round(float(field),10)
                except:
                    return '\\N'
                return str(temp)
            else:
                return str(float(tt[0]) * 0.01)

temp_file = ""
looper = 0
while True:
    try:
        if looper % (20*24) == 0:
            clean_done_dir()
        looper += 1
        logging.info('connecting to DB server - ' + mysql_host)
        conn=pymysql.connect(host= mysql_host,user= mysql_user,passwd= mysql_password,db= mysql_default_db,port=3306,local_infile=1,charset='utf8',cursorclass=pymysql.cursors.DictCursor)
        cursor=conn.cursor()
        cursor.execute('set names utf8mb4')
        logging.info('connected  to DB server - ' + mysql_host)
        #wait to load the file until night by rename the file
        for init_ff in os.listdir(work_path):
            # result = re.findall(r'(\w+)\.(\w+)\.(\d{8})\.(txt|trunc|upd)\.night$', init_ff)
            result = re.findall(r'^(\d+\.)?(\w+)\.(\w+)\.(\d{8})\.(txt|trunc|upd)\.night$', init_ff)
            if len(result) == 0: continue
            if ready_to_process(work_path + os.sep + init_ff) == False: continue
            new_ff = "%s%s.%s.%s.%s" % result[0]
            if time.localtime().tm_hour>=19 or time.localtime().tm_hour<=5:
                os.rename(work_path + os.sep + init_ff, work_path + os.sep + new_ff)

        files = os.listdir(work_path)
        for ff in files:
            #result = re.findall(r'(\w+)\.(\w+)\.(\d{8})\.(txt|trunc|upd)$',ff)
            ori_result = re.findall(r'^(\d+\.)?(\w+)\.(\w+)\.(\d{8})\.(txt|trunc|upd)$', ff)
            if len(ori_result) == 0: continue
            result = [ori_result[0][1:]]

            ss_day = result[0][2]
            full_path = work_path+os.sep+ff
            if ready_to_process(full_path) == False: continue
            logging.info("Start to process the file:"+full_path)
            logging.info("The batch size is :" + str(batchsize) )
            # read table schema from the database
            sql = "SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='%s' and table_name='%s' order by ordinal_position"
            cursor.execute(sql % (result[0][0],result[0][1]))
            if cursor.rowcount == 0:
                #table doesn't exist
                p = subprocess.Popen(['/usr/bin/python3','./DetectTableSchema.py',full_path], shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                ( stdoutdata, stderrdata ) = p.communicate()
                logging.info(result[0][0]+'.'+result[0][1]+' table does not exist in the DB server. You can create it first referring to the following schema.'+os.linesep+stdoutdata.decode())
                logging.info("Skipping the file:"+full_path)
                #os.rename(full_path, work_path+os.sep+"done"+os.sep+ff+".err")
                continue
            table_fields = []
            type_map     = {}
            for row in cursor.fetchall():
                table_fields.append(row['column_name'].lower())
                type_map[row['column_name'].lower()] = row['data_type']
            if 'snapshot_day' in table_fields:
                ss_exist = True
            else:
                ss_exist = False

            # find the unique index columns ( include the primary key )
            sql = "select COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA ='%s' and TABLE_NAME='%s' and CONSTRAINT_NAME = 'PRIMARY' order by ORDINAL_POSITION"
            cursor.execute(sql % (result[0][0], result[0][1]))
            key_columns = []
            if cursor.rowcount == 0:
                sql = "select COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA ='%s' and TABLE_NAME='%s' and CONSTRAINT_NAME in ( select max(CONSTRAINT_NAME) from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA ='%s' and TABLE_NAME='%s' and CONSTRAINT_NAME!='PRIMARY' ) order by ORDINAL_POSITION"
                cursor.execute(sql % (result[0][0], result[0][1], result[0][0], result[0][1]))
                for row in cursor.fetchall():
                    key_columns.append(row['COLUMN_NAME'])
            else:
                for row in cursor.fetchall():
                    key_columns.append(row['COLUMN_NAME'])

            if result[0][3]=='upd' and len(key_columns) <1:
                logging.info("No unique keys provided for update, so skipping it.")
                try:
                    os.rename(full_path, full_path + '.err')
                except Exception as e:
                    logging.info("Failed on rename it to err file with " + repr(e))
                continue

            #start to read file content
            content = readFile(full_path)

            header = content[0].rstrip('\n').rstrip('\r').replace('\\', '_').replace('/', '_').replace('-','_').lower()
            headers = header.split('\t')
            headers = [ headers[i].replace(' ', '_') for i in range(len(headers)) ]
            asin_pos = -1
            for i in range(len(headers)):
                if headers[i]=='asin' or headers[i]=='kasin':
                    asin_pos = i
                    break
            index_pos = []
            for index in range(len(headers)):
                if headers[index] in table_fields:
                    index_pos.append(index)
            if len(index_pos) == 0: continue
            if 'snapshot_day' in headers: ss_exist = False
            #read data in memory
            all_data_dict = {}
            batch_num = 0
            cnt_in_batch = 0
            line_no = 1
            for row in content[1:]:
                line_no += 1
                data = row.rstrip('\n').rstrip('\r')
                #while data:
                data = data.split('\t')
                if len(data)!=len(headers):
                    logging.info("skipping the %d line,its data is:%s" %(line_no,'\t'.join(data)))
                    continue
                new_line = fieldValidate(data[index_pos[0]], type_map[headers[index_pos[0]]])
                for id in range(1, len(index_pos)):
                    new_line = new_line + '\t' + fieldValidate( data[index_pos[id]], type_map[headers[index_pos[id]]])
                if ss_exist:
                    new_line = new_line + '\t' + ss_day
                if asin_pos == -1 or len(data[asin_pos])==0:
                    cnt_in_batch += 1
                    if cnt_in_batch > batchsize:
                        cnt_in_batch = 0
                        batch_num += 1
                    if all_data_dict.get("%08d" % batch_num) is None:
                        all_data_dict["%08d" % batch_num] = [new_line]
                    else:
                        all_data_dict.get("%08d" % batch_num).append(new_line)
                else:
                    if all_data_dict.get(data[asin_pos]) is None:
                        all_data_dict[data[asin_pos]] = [new_line]
                    else:
                        all_data_dict.get(data[asin_pos]).append(new_line)
                #data = fd.readline().rstrip('\n').rstrip('\r')
            #load the data into database batch by batch
            concat_fields = headers[index_pos[0]]
            for id in range(1, len(index_pos)):
                concat_fields = concat_fields + ',' + headers[index_pos[id]]
            if ss_exist:
                concat_fields = concat_fields + ',snapshot_day'
            temp_table_sql = "CREATE TEMPORARY TABLE tmp_table SELECT " + concat_fields + " FROM " + result[0][0] + "." + result[0][1] + " where 0=1"
            # create the temp table in DB
            cursor.execute(temp_table_sql)

            #truncate the table if need
            if result[0][3]=='trunc':
                cursor.execute("truncate table %s.%s" % (result[0][0], result[0][1]) )
                conn.commit()
            temp_file = work_path + os.sep + "temp." + str(time.time())
            of = open(temp_file, "wb")
            batch_num = 0
            cnt_in_batch = 0
            for key in all_data_dict:
                for row in all_data_dict[key]:
                    cnt_in_batch += 1
                    of.write(str.encode(row + '\n'))
                if cnt_in_batch >= batchsize:
                    of.close()
                    cnt_in_batch = 0
                    batch_num += 1
                    # load data into temp table
                    sql = "load data local infile '" + temp_file + "' into table tmp_table CHARACTER SET utf8mb4 fields terminated by '\t'"
                    cursor.execute(sql)
                    os.remove(temp_file)
                    # delete duplicated row from destination table
                    if len(key_columns) > 0 and result[0][3]=='txt':
                        where_clause = "a1." + key_columns[0] + "=a2." + key_columns[0]
                        for id in range(1, len(key_columns)):
                            where_clause = where_clause + " and a1." + key_columns[id] + "=a2." + key_columns[id]
                        sql = "delete a1 from " + result[0][0] + "." + result[0][1] + " a1, tmp_table a2 where " + where_clause
                        cursor.execute(sql)
                        conn.commit()
                    # update some fields if upd mode
                    if result[0][3]=='upd':
                        where_clause = "a1." + key_columns[0] + "=a2." + key_columns[0]
                        for id in range(1, len(key_columns)):
                            where_clause = where_clause + " and a1." + key_columns[id] + "=a2." + key_columns[id]
                        set_fields = [headers[index_pos[id]] for id in range(0, len(index_pos)) if headers[index_pos[id]] not in key_columns]
                        set_clause = "a1." + set_fields[0] + "=a2." + set_fields[0]
                        for id in range(1, len(set_fields)):
                            set_clause = set_clause + ",a1." + set_fields[id] + "=a2." + set_fields[id]
                        sql = "update "+ result[0][0] + "." + result[0][1] + " a1 inner join tmp_table a2 on " + where_clause + " set " + set_clause
                    else:
                        sql = "insert " + result[0][0] + "." + result[0][1] + " ( " + concat_fields + " ) select " + concat_fields + " from tmp_table"
                    cursor.execute(sql)
                    cursor.execute("truncate table tmp_table")
                    logging.info("loading batch %d" % batch_num)
                    conn.commit()
                    temp_file = work_path + os.sep + "temp." + str(time.time())
                    of = open(temp_file, "wb")
            #deal with the last batch
            of.close()
            batch_num += 1
            if cnt_in_batch>0 :
                sql = "load data local infile '" + temp_file + "' into table tmp_table CHARACTER SET utf8mb4 fields terminated by '\t'"
                cursor.execute(sql)
                # delete duplicated row from destination table
                if len(key_columns) > 0:
                    where_clause = "a1." + key_columns[0] + "=a2." + key_columns[0]
                    for id in range(1, len(key_columns)):
                        where_clause = where_clause + " and a1." + key_columns[id] + "=a2." + key_columns[id]
                    sql = "delete a1 from " + result[0][0] + "." + result[0][1] + " a1, tmp_table a2 where " + where_clause
                    cursor.execute(sql)
                    conn.commit()
                sql = "insert " + result[0][0] + "." + result[0][1] + " ( " + concat_fields + " ) select " + concat_fields + " from tmp_table"
                cursor.execute(sql)
                conn.commit()
                logging.info("loading last batch %d" % batch_num)
            cursor.execute("drop table tmp_table")
            conn.commit()
            if os.path.exists(temp_file):
                os.remove(temp_file)
            if os.path.exists(work_path+os.sep+"done"+os.sep+ff):
                os.remove(work_path+os.sep+"done"+os.sep+ff)
            os.rename(full_path, work_path+os.sep+"done"+os.sep+ff)
            logging.info("finished the processing of file:" + full_path)

    except Exception as e:
        #logging.warning("ERROR: %s" % e)
        #remove the temp file
        if os.path.exists(temp_file):
           os.remove(temp_file)
        os.rename(full_path, full_path + '.err')
        #send out mail for reminder
        msgBody = "Error: %s" % repr(e)
        #if data is not None:
        #    msgBody += "<br>Data is:" + repr(data)
        send_mail('cn-tam-auto@amazon.com', reminder_mail_address, "Error generated when processing file " + full_path, msgBody, [],'html')

    finally:
        cursor.close()
        conn.close()
        logging.info("sleep for 180 seconds......")
        time.sleep(180)
        logging.info("wake up to run again......")
