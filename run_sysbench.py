#!/usr/bin/env python2

import os, time

# Settings

db = 'postgresql'  # FIXME: 'mysql' or 'postgresql'

db_name = 'sysbench'
db_user = 'sysbench'
db_pass = 'sysbench'

sysbench_bin = '/usr/local/bin/sysbench'
sysbench_tpcc_dir = '/home/fernando/sysbench-tpcc'
sysbench_driver = {'mysql': 'mysql', 'postgresql': 'pgsql'}
sysbench_tables = 10
sysbench_scale = 100
sysbench_time = 3600
sysbench_threads = [56, 112, 224, 448, 896]

gid = { 'mysql': 1001, 'postgresql': 121 }
user = { 'mysql': 'mysql', 'postgresql': 'postgres' }
datadir = { 'mysql': '/data/sam/mysql', 'postgresql': '/data/sam/postgresql' }
base_data = { 'mysql': '/home/fernando/base_datadir-90G', 'postgresql': '/home/fernando/base_postgresql' }
base_results = '/home/fernando/results'

comp_conf_file = {
  'mysql': '/etc/mysql/conf.d/comp.cnf',
  'postgresql': '/etc/postgresql/10/main/conf.d/comp.conf'
}

cache_sizes = ['96G', '48G']
hugepages_pool = { 
  '2M': {'96G': 51200, '48G': 25600},  # 100G-->102400M/2M=51200 | 50G-->25600 
  '1G': {'96G': 100, '48G': 50}
}
chunk_size = {
  '2M': {'96G': '1G', '48G': '1G'},
  '1G': {'96G': '4G', '48G': '6G'}
}

# Disable Transparent Huge Pages (THP):
os.system("""sudo sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/enabled'""")
os.system("""sudo sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/defrag'""")

# Allowing database OS user to use huge pages
os.system("""sudo sh -c 'echo %i > /proc/sys/vm/hugetlb_shm_group'""" % gid[db])

# Run tests with and without NUMA (in fact, with interleaved memory across nodes)
for numa_interleave in ['off']:  # ['on', 'off']:

  # Run tests with different page sizes (note the kernel must have support for a particular page size)
  for page_size in ['4K', '2M']:  #['4K', '2M', '1G']: # FIXME - ATTENTION: large page size can only be changed at boot time!

    # Run tests with different cache sizes
    for cache in cache_sizes:
      # Configure Buffer Pool / shared_buffers
      if db is 'mysql':
        os.system("""sudo sh -c 'echo "[mysqld]" > %s'""" % comp_conf_file[db] )
        os.system("""sudo sh -c 'echo "innodb_buffer_pool_size = %s" >> %s'""" % (cache, comp_conf_file[db]))
        # Configure BP chunk size for large pages
        if page_size in ['2M', '1G']:
          os.system("""sudo sh -c 'echo "innodb_buffer_pool_chunk_size = %s" >> %s'""" % (chunk_size[page_size][cache], comp_conf_file[db]))
      else:  # pg
        os.system("""sudo sh -c 'echo "shared_buffers = %sB" > %s'""" % (cache, comp_conf_file[db]))  # FIXME: PostgreSQL only understands 'GB', not 'G'
      
      # Configure NUMA interleaved
      if numa_interleave is 'on':
        if db is 'mysql':
          os.system("""sudo sh -c 'echo "innodb_numa_interleave = 1" >> %s'""" % comp_conf_file[db])  
        else:  # pg
          pass
      else:
        pass

      # Adjust Huge Pages pool size and enable use of large pages by the database
      if page_size in ['2M', '1G']:
        os.system('sudo sysctl -w vm.nr_hugepages=%i' % hugepages_pool[page_size][cache])
        if db is 'mysql':
          os.system("""sudo sh -c 'echo "large_pages = 1" >> %s'""" % comp_conf_file[db])
        else:  # pg
          os.system("""sudo sh -c 'echo "huge_pages = on" >> %s'""" % comp_conf_file[db])
      else:  # '4K'
        # clear the pool
        os.system('sudo sysctl -w vm.nr_hugepages=0')
      
      # Run tests with different numbers of concurrent clients
      for num_threads in sysbench_threads:
      
        # Set file output prefix (e.g.: '4K-100G-56')
        if numa_interleave is 'on':
          prefix = '%s-%s-%i-%s-numa_int' % (page_size, cache, num_threads, db)
        else:
          prefix = '%s-%s-%i-%s-numa_reg' % (page_size, cache, num_threads, db)

        # stop server
        os.system("sudo service %s stop" % db)

        # recycle datadir
        os.system("sudo rm -fr %s/*" % datadir[db])
        os.system("sudo cp -r %s/* %s" % (base_data[db], datadir[db]))
        os.system("sudo chown %s:%s -R %s" % (user[db], user[db], datadir[db]))

        # drop OS cache
        os.system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
        
        # start server
        os.system("sudo service %s start" % db)

        # collect status counters
        if db is 'mysql':
          os.system("mysql -e 'show global status' > %s/%s/%s-pre.out" % (base_results, db, prefix))
        else:  # pg
          os.system("""psql -U %s -c "SELECT datname, pg_size_pretty(pg_database_size(datname)), blks_read, blks_hit, temp_files, temp_bytes from pg_stat_database where datname='%s'" > %s/%s/%s-pre.out""" % (db_user, db_name, base_results, db, prefix))

        # run sysbench-tpcc
        cmd_sysbench = "cd %s && /usr/bin/env %s tpcc.lua --db-driver=%s --%s-db=%s --%s-user=%s --%s-password=%s --threads=%i --report-interval=1 --tables=%i --scale=%i --use_fk=0 --trx_level=RC --time=%i run > %s/%s/%s-run.out" % (sysbench_tpcc_dir, sysbench_bin, sysbench_driver[db], sysbench_driver[db], db_name, sysbench_driver[db], db_user, sysbench_driver[db], db_pass, num_threads, sysbench_tables, sysbench_scale, sysbench_time, base_results, db, prefix)
        print cmd_sysbench
        os.system(cmd_sysbench)

        # collect status counters
        if db is 'mysql':
          os.system("mysql -e 'show global status' > %s/%s/%s-post.out" % (base_results, db, prefix))
          os.system("""mysql -e "SELECT CONCAT(sum(ROUND(data_length / ( 1024 * 1024 * 1024 ), 2)), 'G') DATA, CONCAT(sum(ROUND(index_length / ( 1024 * 1024 * 1024 ),2)), 'G') INDEXES, CONCAT(sum(ROUND(( data_length + index_length ) / ( 1024 * 1024 * 1024 ), 2)), 'G') 'TOTAL SIZE' FROM information_schema.TABLES where table_schema='sysbench' ORDER BY data_length + index_length" > %s/%s/%s-post-sizes.out""" % (base_results, db, prefix))
        else:  # pg
          os.system("""psql -U %s -c "SELECT datname, pg_size_pretty(pg_database_size(datname)), blks_read, blks_hit, temp_files, temp_bytes from pg_stat_database where datname='%s'" > %s/%s/%s-post.out""" % (db_user, db_name, base_results, db, prefix)) 
          
        # break before next iteration
        time.sleep(300)

