#!/usr/bin/python3
#coding=utf-8

import sys
import os
import codecs
import chardet
import re

def readFile(filePath):
    finput = open(filePath, "rb")
    str = finput.readline() + finput.readline() + finput.readline()  #在文件中多读取一行是因为，如果一行文字太少，编码检测有可能错误，按自己要求调整
    codeType = chardet.detect(str)["encoding"]  #检测编码方式
    if codeType == 'ascii': codeType = 'utf-8-sig'
    finput.close()
    content = []
    data=''
    with codecs.open(filePath, 'r', codeType, errors='ignore') as fd:
        data = fd.read()
    lines=re.split('\r\n|\n',data)
    for line in lines:
        line=line.rstrip('\n').rstrip('\r')
        if len(line)>0:
            content.append(line)
    return content

file_name = os.path.basename(sys.argv[1])
result = re.findall(r'^(\d+\.)?(\w+)\.(\w+)\.(\d{8})\.(txt|trunc|upd)$',file_name)
db_name, table_name = "",""
if len(result) > 0:
    db_name = result[0][1]
    table_name = result[0][2]
else:
    result = re.findall(r'(.*)\.txt$', file_name)
    if len(result) > 0:
        table_name = result[0]
    else:
        table_name = file_name

content = readFile(sys.argv[1])
    #ff = codecs.open(sys.argv[1],'r','gbk','replace')
header = content[0].rstrip('\n').rstrip('\r').replace('\\','_').replace('/','_').replace('-','_').lower()
headers = header.split('\t')
headers = [ headers[i].replace(' ','_') for i in range(len(headers)) ]

#data = ff.readline().rstrip('\n').rstrip('\r')
lengths = {}
types = {}
#print(headers)
#while data:
cnt = 1
for row in content[1:]:
    cnt += 1
    data = row.rstrip('\n').rstrip('\r')
    fields = data.split('\t')
    if len(fields) != len(headers):
        print("%d line has mismatched columns,its data is:%s" % (cnt,row))
        print("fields " + str(len(fields)) + " vs header cols " + str(len(headers)))
        print(fields)
    for i in range(len(headers)):
        if fields[i] is None or len(fields[i]) == 0:
            if lengths.get(headers[i]) is None: lengths[headers[i]] = len(fields[i])
            continue
        if lengths.get(headers[i]) is None:
            lengths[headers[i]] = len(fields[i])
        else:
            if lengths[headers[i]] < len(fields[i]):
                lengths[headers[i]] = len(fields[i])

        if len(re.findall(r'^[-+]?[0-9]+$',fields[i])) == 1:
            if types.get(headers[i]) is None:
                types[headers[i]] = "int"
        elif len(re.findall(r'^[-+]?([0-9]*\.?[0-9]+|[0-9]+\.?[0-9]*)$',fields[i])) == 1:
            if types.get(headers[i]) is None or types.get(headers[i])=='int':
                types[headers[i]] = "float"
        elif len(re.findall(r'^\d{2}-\w{3}-\d{2}$',fields[i])) == 1 \
                or len(re.findall(r'^\d{4}/\d{1,2}/\d{1,2}$',fields[i])) == 1 \
                or len(re.findall(r'^\d{4}-\d{1,2}-\d{1,2}$', fields[i])) == 1 \
                or len(re.findall(r'^\d{1,2}/\d{1,2}/\d{4}$', fields[i])) == 1:
            if types.get(headers[i]) is None:
                types[headers[i]] = "date"
        else:
            types[headers[i]] = "varchar"
        if headers[i]=='asin' or headers[i]=='kasin'or headers[i]=='pasin':
            types[headers[i]] = "char"
            lengths[headers[i]] = 11
    #data = ff.readline().rstrip('\n').rstrip('\r')

guessedKey = ""
if "asin" in headers:
    guessedKey = "asin"
elif "kasin" in headers:
    guessedKey = "kasin"
elif "pasin" in headers:
    guessedKey = "pasin"
elif "pubcode" in headers:
    guessedKey = "pubcode"
elif "digital_pubcode" in headers:
    guessedKey = "digital_pubcode"

if len(db_name) > 0:
    print("use %s;" % db_name)
print("create table %s (" % table_name)
for i in range(len(headers)):
    last_line=','
    #if i == len(headers) - 1: last_line=''
    if types.get(headers[i]) is None: types[headers[i]] = "varchar"
    if types[headers[i]] == "varchar" or types[headers[i]] == "char":
        #print(headers[i] + " "* 10 + types[headers[i]] + "(%s)" % lengths[headers[i]])
        if lengths[headers[i]] >5:
            print("%-50s%s(%s)%s" % (headers[i].lower(),types[headers[i]],lengths[headers[i]]+2,last_line))
        else:
            print("%-50s%s(%s)%s" % (headers[i].lower(), types[headers[i]], lengths[headers[i]], last_line))
    else:
        print("%-50s%s%s" % (headers[i].lower(), types[headers[i]],last_line))
key_clause = "primary key ( %s )" % guessedKey
print("%-30s" % key_clause )
print(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
