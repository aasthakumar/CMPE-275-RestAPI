from cassandra.cluster import Cluster
import configparser

class ConnectToCassandra():

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.cfg')
        self.cluster = Cluster([config.get('ClusterConn','cluster')])
        self.session = self.cluster.connect(config.get('ClusterConn','keyspace'))
   
    def get_val(self,todate):
        ret = {}
        data = []
        query = "select * from data where timestamp > totimestamp('{}') ALLOW FILTERING;".format(todate)
        #print(query)
        rows = self.session.execute(query)
        for row in rows:
            ret['timestamp'] = str(row[0])
            ret['alti']= str(row[1])
            ret['drct'] = str(row[2]) 
            ret['dwpf'] = str(row[3]) 
            ret['gust'] = str(row[4])
            ret['id'] = str(row[5])
            ret['mnet'] = str(row[6])
            ret['p24i'] = str(row[7])
            ret['pmsl'] = str(row[8])
            ret['relh'] = str(row[9]) 
            ret['selv'] = str(row[10]) 
            ret['sknt'] = str(row[11])
            ret['slat'] = str(row[12]) 
            ret['slon'] = str(row[13])
            ret['stn'] = str(row[14])
            ret['tmpf'] = str(row[15]) 
            ret['wthr'] = str(row[16]) 
            data.append(ret)
        return data
        
    def get_id(self):
        data = []
        query = 'select distinct todate(timestamp) from data;'       
        rows = self.session.execute(query)
        count = 0
        for row in rows:
            data.append(row[0]) 
            count = count + 1
        return count,list(set(data))
    
    
'''    
def test():
    cc = ConnectToCassandra()
    cc.get_val()

if __name__ == "__main__":
    test()
'''
