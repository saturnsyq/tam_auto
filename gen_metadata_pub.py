#!/usr/bin/python3
#coding=utf-8

import os,sys
import fnmatch, openpyxl, time, datetime, shutil
import pymysql
from openpyxl.styles import Font
from openpyxl.styles.colors import RED
import time, re

# Find excel file under indicated directory
def iterfindfiles(path, fnexp):
    for root, dirs, files in os.walk(path):
        for filename in fnmatch.filter(files, fnexp):
            yield os.path.join(root, filename)
#Copy the template as the Pubcode
def copy_pub_file(working_dir,root_dir,ops_dir,pub_code):
    for filename in iterfindfiles(working_dir, "*.xlsx"):
     base_name = os.path.basename(filename)
     new_filename=ops_dir+pub_code+time.strftime("_%Y%m%d_%H%M%S", time.localtime(time.time()))+'.xlsx'
    if os.path.exists(new_filename):
        print (new_filename +'already in the directory!')
    else:
        os.system("cp "+filename+' '+new_filename)
        #shutil.copy(filename, new_filename)
    #print base_name +'\n', new_filename +'\n',filename +'\n',ops_dir
    #print (new_filename)
    return new_filename
#generate ref_id
def gen_ref_id(ref_id,isbn13,pubcode):
    pub_ref_id = pubcode+'_'+isbn13+time.strftime("_%Y%m%d", time.localtime(time.time()))+'_'+str(ref_id).zfill(4)
    return pub_ref_id

def write_excel(excel_name,wb,num=6,a=[]):
#    wb = openpyxl.load_workbook(excel_name)
    font = Font(color=RED)
    sheet = wb.get_sheet_by_name("base")
    col_name=('A','C','D','F','H','J','K','L','M','N','R','S','U','AE','AI')
    n=0
    if len(a) != 15:
        print ('Input data list not match column,please check!',len(a))
    else:
      for i in col_name:
        sheet[i+str(num)].value=a[n]
        n = n +1
    if a[9] is not None and len(a[9])>=2000:
      sheet['N'+str(num)].font=font
    if a[5]=='E'or '97'not in a[5]:
      sheet['H'+ str(num)].font = font
      sheet['J'+ str(num)].font = font
    if a[3] is None:
      sheet['AI' + str(num)].font = font
    return

def db_connect_mysql():
    mysql_host = 'ud094661c879c59fa6e9e'
    mysql_user = '***'
    mysql_password = '***'
    mysql_default_db = 'metadata'
    conn = pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password,
                           db=mysql_default_db, port=3306,local_infile=1, charset='utf8')
    cur = conn.cursor()
    return cur,conn

def check_ref_id(pasin):
    cur=db_connect().cursor
    sql='''select count(*) from pbook_desc where pasin=:pasin'''
    cur.execute(sql,pasin)
    result=cur.fetchall
    if len(result[0][0]) != 0:
        return 1
    else:
        return 0

def update_ref_id(pasin,ref_id):
    cur = db_connect().cursor
    sql = "update pbook_desc set ref_id=:pub_ref_id where pasin=:pasin"
    cur.execute(sql, ref_id,pasin)
    db_connect().commit()
    return

def gen_metadata(pub):
    pubcode=pub
    working_dir = r'/mnt/wind/LJ/metadata/template'
    root_dir = r'/mnt/wind/LJ/metadata'
    ops_dir = root_dir + '/' + 'data/'
    excel_name = copy_pub_file(working_dir, root_dir, ops_dir, pubcode)
    wb = openpyxl.load_workbook(excel_name)
    #pubcode=raw_input('Please input pucode:')
    ref_id = 1
    sql = "select t1.brand_name imprint," \
          "t1.brand_name logo_name," \
          "t1.title_name title," \
          "t1.AUTHOR_NAME authors " \
          ",case when t1.isbn13 is NULL then 'E' ELSE t1.isbn13 end isbn13" \
          ",'第1版'" \
          ",date_format(t1.PBOOK_PUBLICATION_DAY,'%%Y%%m%%d') PBOOK_PUBLICATION_DAY" \
          ",date_format(t1.PBOOK_PUBLICATION_DAY,'%%Y%%m%%d') release_day" \
          ",concat(t2.P_DESC,t3.P_DESC_A) pbook_desc" \
          ",t4.clcc" \
          ",'CHI'" \
          ",'CNY'" \
          ",t5.keyword " \
          ",t1.pasin " \
          "from NOS_TOP10K t1 " \
          " left join PBOOK_DESC t2 " \
          " on t1.pasin=t2.pasin " \
          " left join PBOOK_DESC_A t3 " \
          " on t1.pasin=t3.pasin " \
          " left join PASIN_CLCC t4 " \
          " on t1.pasin=t4.pasin " \
          " left join PASIN_KEYWORD t5 " \
          " on t1.pasin=t5.pasin " \
          " inner join (select a.asin from accesslog.mail_asin_audit a inner join (select tt.asin,tt.type,max(tt.msgid) as msgid from accesslog.mail_asin_audit tt where tt.type='HiVendor:noslist' group by tt.asin,tt.type) b on a.asin=b.asin and a.msgid=b.msgid where a.comments='YES') t6 " \
          " on t1.pasin=t6.asin" \
          " where t1.DIGITAL_PUBCODE= '%s' " \
          " and t1.kasin is NULL " % pubcode
    row_list = []
    id = 1
    num = 6
    cur,conn = db_connect_mysql()
    #cur= conn.cursor()
    cur.execute(sql)
    results=cur.fetchall()
    for row in results:
        for i in row:
                row_list.append(i)
        #print row_list
        ref_id = gen_ref_id(id, row[4], pubcode)
        row_list.insert(4,ref_id)
        id = id + 1
        write_excel(excel_name, wb,num,row_list)
        row_list = []
        num=num+1
    try:
        wb.save(excel_name)
    except:
        print ('Some errors for save excel file!')
    print (cur.rowcount)
    if cur.rowcount == 0:
        excel_name = ""
    cur.close()
    conn.close()
    return excel_name


if __name__ == '__main__':
    if len(sys.argv)>1:
        pub = sys.argv[1].strip()
        file = gen_metadata(pub)
        print (file)
    else:
        print ('Error input parameter!',len(sys.argv))

