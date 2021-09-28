#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import time
import datetime


def process_audit_data(file_name):    
    start_time = time.time()    
   
    PATTERN_ROW = r'((?im)(?P<data>^I\d+)\s+(?P<time>\d{2}:\d{2}:\d{2}.\d+)\s+\d+\s+sql/exec_log.go:\d+\s+\[n1,client=(?P<client>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{0,4}),(?P<connections>[a-z][A-Z]+),user=(?P<user>[a-z]+)\]\s+\d+\s+exec\s+\"+\s+\{\"(?P<table>[a-z][A-Z]+).+?}\s+\"(?P<command>[a-z][A-Z]+))'    
    
    
    txt_content = open(file_name,'r', encoding='utf-8').read()
        
    
    with open(file_name + '.csv','w', encoding='utf-8') as wr:
        
        # Process each row in the table
        row_iter = re.finditer(PATTERN_ROW, txt_content)
        
        for row_match in row_iter:
            rowData = []
            rowData.append(row_match.group('date'))
            rowData.append(row_match.group('time'))
            rowData.append(row_match.group('client'))
            rowData.append(row_match.group('connections'))
            rowData.append(row_match.group('user'))
            rowData.append(row_match.group('table'))
            rowData.append(row_match.group('command'))
            wr.write(','.join(rowData))
            wr.write('\n')
                
    print ('Elapsed Time : {0:.2f}s'.format(time.time()-start_time))


# In[3]:


files =[r"C:\Data Recovery\Data Science\CockroachDB\DatabaseAuditing"]
for file_name in files:
    print('****{0}'.format(file_name))
    process_audit_data(file_name)


# In[ ]:




