import requests
import json, urllib.parse
import datetime

class qhistory(object):
  def __init__(self, ws_url, token):
    self.ws_url = ws_url
    self.token = token

  def get_query_history(self, statuses=['FINISHED'], uers_ids=None, endpoint_ids=None, end_time=None, duration=300, max_results=100, include_metrics=True):
    '''
    start_time: iso8601 format. e.g. '2022-12-31T01:32:30+09:00'
    '''
    url = urllib.parse.urljoin(self.ws_url, '/api/2.0/sql/history/queries')
    print('>>>', self.ws_url, url)
    headers = {'Authorization' : f'Bearer {self.token}'}
    body = {}
    body['max_results'] = max_results
    body['include_metrics'] = include_metrics

    filter_by={}
    filter_by['statuses']=statuses
    if uers_ids is not None:
      filter_by['uers_ids'] = uers_ids
    if endpoint_ids is not None:
      filter_by['endpoint_ids'] = endpoint_ids
    
    if end_time is None:
      end_time_ds = datetime.datetime.now()
    else:
      end_time_ds = datetime.datetime.fromisoformat(start_time)
    end_time_ms = int( end_time_ds.timestamp()*1000 )
    start_time_ms = end_time_ms - duration * 1000
    query_start_time_range = {'start_time_ms': start_time_ms, 'end_time_ms': end_time_ms}
    filter_by['query_start_time_range'] = query_start_time_range
    body['filter_by'] = filter_by


    self.res=[]
    has_next_page = True
    next_page_token = None
    
    cnt_pulled = 0
    while has_next_page and cnt_pulled < max_results:
 
      if next_page_token is not None:
        body['page_token'] = next_page_token
        if 'filter_by' in body:
          del body['filter_by']

      print(body)
      
      ret = requests.get(url, headers=headers, json=body)
      if ret.status_code != 200:
        raise Exception(f'error response {ret.status_code}: {ret.text}')
      res_obj = ret.json()
      self.res.extend(res_obj['res'])

      has_next_page = res_obj['has_next_page']
      if has_next_page:
        next_page_token = res_obj['next_page_token']
    
      cnt_pulled += len( res_obj['res'] )


  def export(self, filename, format='json', mode='a'):
    with open(filename, mode=mode) as f:
      for r in self.res:
        f.write( f'{json.dumps(r)}\n' )
    

import configparser
def app():
  
  config = configparser.ConfigParser()
  config.read('config.ini')
  host = config['default']['host']
  token = config['default']['token']

  qh = qhistory(host, token)
  qh.get_query_history(duration=(3600*24), max_results=100 )
  qh.export('test.json', mode='w')

if __name__ == '__main__':
  app()
