from databricks import sql
import configparser
from datetime import datetime
from multiprocessing import Pool
import click
import pyodbc

class qperf(object):
  def __init__(self, host, path, token):
    self.host = host
    self.path = path
    self.token = token

  def connect(self):
    pass

  def disconnect(self):
    pass

  def execute(self, query, disable_cache=False, query_tag='', use_odbc=True, fetch_all=False):
    if use_odbc:
      start=datetime.now()
      conn = pyodbc.connect("DSN=Databricks-Spark", autocommit=True)
      cur = conn.cursor()
      if disable_cache:
        cur.execute('SET use_cached_result = false;')
      if query_tag != '':
        query = f'--query_tag: {query_tag}\n' + query
      cur.execute(query)
      end = datetime.now()
      dur = (end - start).total_seconds()
      print(f'query_time: {dur}')
      if fetch_all:
        cur.fetchall()
      return


    else:
      with sql.connect(server_hostname=self.host, http_path=self.path, access_token=self.token) as conn:
        with conn.cursor() as cur:
          start=datetime.now()
          
          if disable_cache:
            cur.execute('SET use_cached_result = false;')
          if query_tag != '':
            query = f'--query_tag: {query_tag}\n' + query
          cur.execute(query)
          
          end = datetime.now()
          dur = (end - start).total_seconds()
          print(f'query_time: {dur}')
          return cur.fetchall()

  def execute_from_file(self, filename, disable_cache=False, query_tag='', fetch_all=False):
    with open(filename, 'r') as f:
      query = f.read()
    return self.execute(query, disable_cache=disable_cache, query_tag=query_tag, fetch_all=fetch_all)

  def execute_from_files(self, filenames, disable_cache=False, query_tag=''):
    for i, f in enumerate(filenames):
      if i==0:
        fetch_all=True
      else:
        fetch_all=False
      self.execute_from_file(f, disable_cache=disable_cache, query_tag=query_tag, fetch_all=fetch_all)
  
  def _wapper_execute_from_file(self, args):
    '''
    args: {filenames, disable_cache, query_tag}
    '''
    return self.execute_from_file(args['filename'], args['disable_cache'], args['query_tag'], args['fetch_all'])


  def execute_from_files_concurrent(self, filenames, n_para=1, disable_cache=False, query_tag=''):
    params=[]
    for i, f in enumerate(filenames):
      if i == 0:
        fetch_all=True
      else:
        fetch_all=False
      params.append({ 'filename': f, 'disable_cache': disable_cache, 'query_tag': query_tag, 'fetch_all': fetch_all })

    with Pool(n_para) as p:
      p.map(self._wapper_execute_from_file, params)


@click.command()
# config, profile, para, query file list, query_tag
@click.option('-c', '--configure', 'cconfig_file', default='config.ini', type=str, show_default=True)
@click.option('--profile', default='default', type=str, show_default=True)
@click.option('-p', '--n_para', default=1, type=int, show_default=True)
@click.option('-q', '--query_list', required=True, type=str)
@click.option('-t', '--query_tag', default='', type=str)
@click.option('--disable_cache', default=True, type=bool, show_default=True)
def app(cconfig_file, profile, n_para, query_list, query_tag, disable_cache):
  
  print(f'cconfig_file: {cconfig_file}')
  print(f'profile: {profile}')
  print(f'n_para: {n_para}')
  print(f'query_list: {query_list}')
  print(f'query_tag: {query_tag}')
  print(f'disable_cache: {disable_cache}')

  config = configparser.ConfigParser()
  config.read(cconfig_file)
  host = config[profile]['host'].replace('https://', '')
  token = config[profile]['token']
  path = config[profile]['path']

  qp = qperf(host, path, token)
  with open(query_list, 'r') as f:
    files=f.readlines()

  cleaned_file_list=[]
  for f in files:
    _f = f.strip()
    if len(_f) != 0:
      cleaned_file_list.append(_f)

  print(cleaned_file_list)
  qp.execute_from_files_concurrent(cleaned_file_list, n_para, disable_cache, query_tag)


if __name__ == '__main__':
  app()
