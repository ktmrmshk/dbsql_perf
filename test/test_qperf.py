import unittest
import configparser
import qperf

class Test_qparf(unittest.TestCase):
  def setUp(self):
    config = configparser.ConfigParser()
    config.read('config.ini')
    host = config['default']['host'].replace('https://', '')
    token = config['default']['token']
    path = config['default']['path']
    self.qp = qperf.qperf(host, path, token)
  def tearDown(self):
    pass


  def test_execute(self):
    ret = self.qp.execute('select * from diamonds limit 4;', disable_cache=True)
    for r in ret:
      print(r)

  def test_execute_with_tag(self):
    ret = self.qp.execute('select * from diamonds limit 4;', disable_cache=True, query_tag='foobar123')


  def test_execute_from_file(self):
    self.qp.execute_from_file('test.sql')

  def test_execute_from_files(self):
    files=['test.sql'] * 3
    self.qp.execute_from_files(files, disable_cache=True)

  def test_execute_from_files_concurrent(self):
    files=['test.sql'] * 3
    self.qp.execute_from_files_concurrent(files, 2, disable_cache=True)
  
  def test_execute_from_files_concurrent_with_tag(self):
    files=['test.sql'] * 3
    self.qp.execute_from_files_concurrent(files, 2, disable_cache=True, query_tag='foobar987')


if __name__ == '__main__':
  unittest.main()
