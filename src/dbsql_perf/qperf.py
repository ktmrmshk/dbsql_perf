from databricks import sql
import configparser
from datetime import datetime
from multiprocessing import Pool

class qperf(object):
  def __init__(self, host, path, token):
    self.host = host
    self.path = path
    self.token = token

  def connect(self):
    pass

  def disconnect(self):
    pass

  def execute(self, query, disable_cache=False, query_tag=''):
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

  def execute_from_file(self, filename, disable_cache=False, query_tag=''):
    with open(filename, 'r') as f:
      query = f.read()
    return self.execute(query, disable_cache=disable_cache, query_tag=query_tag)

  def execute_from_files(self, filenames, disable_cache=False, query_tag=''):
    for f in filenames:
      self.execute_from_file(f, disable_cache=disable_cache, query_tag=query_tag)
  
  def _wapper_execute_from_file(self, args):
    '''
    args: {filenames, disable_cache, query_tag}
    '''
    return self.execute_from_file(args['filename'], args['disable_cache'], args['query_tag'])


  def execute_from_files_concurrent(self, filenames, n_para=1, disable_cache=False, query_tag=''):
    params=[]
    for f in filenames:
      params.append({ 'filename': f, 'disable_cache': disable_cache, 'query_tag': query_tag })

    with Pool(n_para) as p:
      p.map(self._wapper_execute_from_file, params)

def app():
  config = configparser.ConfigParser()
  config.read('config.ini')
  host = config['default']['host'].replace('https://', '')
  token = config['default']['token']
  path = config['default']['path']

  print('>>>', host, token, path)

  qp = qperf(host, path, token)
  ret = qp.execute('select * from diamonds limit 4;', disable_cache=True)
  for r in ret:
    print(r)

if __name__ == '__main__':
  app()
