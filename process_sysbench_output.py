#!/usr/bin/env python2

import os, string

base_path = '/home/fernando/Percona/Tests/hp'
results_path = '%s/results' % base_path
csv_path = '%s/csv' % results_path
dbs = ['mysql', 'postgresql']

for db in dbs:

  results_path_db = "%s/%s" % (results_path, db)
  csv_path_db = "%s/%s" % (csv_path, db)
  if not os.path.exists(csv_path_db):
    os.mkdir(csv_path_db)
  list = os.listdir(results_path_db)

  for filename in list:

    if filename.endswith("-run.out"):
    
      file = open('%s/%s' % (results_path_db, filename))
      lines = file.readlines()
      file.close()

      rows = []
      TPS = 0
      transactions = 0

      for line in lines:
        if line.startswith('    transactions:'):
          line_s = line.split()
          transactions = line_s[1]
          TPS = line_s[2].strip('(') 
        elif line.startswith('['):
          line_s = line.split()
          tps = line_s[6]
          qps = line_s[8]
          aux = line_s[10].split('/')
          r = aux[0]
          w = aux[1]
          o = aux[2].strip(')')
          row = string.join([tps,qps,r,w,o], ';')
          rows.append(row)  
        
      csv_filename = string.join([filename.split('.')[0], 'csv'], '.')    
      file = open('%s/%s' % (csv_path_db,csv_filename), 'w')
      for row in rows:
        file.write('%s\n' % row)
      file.close()
      
      summary_filename = string.join([string.join([filename.split('.')[0], 'summary'], '-'), 'csv'], '.')
      file = open('%s/%s' % (csv_path_db,summary_filename), 'w')
      f_s = summary_filename.split("-")
      if f_s[2]=='56':
        f_s[2] = '056'
      file.write('%s;%s;%s;%s;%s;%s\n' % (f_s[0], f_s[1], f_s[2], f_s[3], transactions, TPS))
      file.close() 
    
    
    
    
