from flask import Flask
from flask import request, url_for
import json
import datetime
from datetime import date
from flask import Response
from connection import ConnectToCassandra
from werkzeug.contrib.cache import SimpleCache
from createbloomfilter import CreateBloomFilter
#import OrderedDict

#now = datetime.datetime.now()
app = Flask(__name__)
bl = ConnectToCassandra()
cbf = CreateBloomFilter()

class HTTPMethodOverrideMiddleware(object):
    allowed_methods = frozenset([
        'GET',
        'HEAD',
        'POST',
        'DELETE',
        'PUT',
        'PATCH',
        'OPTIONS'
    ])
    bodyless_methods = frozenset(['GET', 'HEAD', 'OPTIONS', 'DELETE'])

# convert date in string format to date format
def string_to_date(todate):
    year,month, day = todate.split("-")
    return date(int(year), int(month), int(day))

# convert date to string format, required for returning data in json format
def date_to_string(o):
    if isinstance(o, datetime.datetime):
        return "{}-{}-{} {}:{}:{}".format(o.year, o.month, o.day,o.hour,o.minute,o.second)

# gets data from cassandra and dumps that in json format and caches data for awhile
def get_my_item(todate):
    cache = SimpleCache()
    cache1 = SimpleCache()
    rv = cache.get('my-item')
    rv1 = cache1.get('todate')
    if rv1 != todate:
        print("not equal")   
    if rv is None and rv1 != todate:
        data = bl.get_val(todate)
        #print(data)
        js = json.dumps(data,default = date_to_string, indent=4)
        rv = js
        rv1 = todate
        cache.set('my-item', rv, timeout=5 * 60)
        cache1.set('todate', rv1, timeout=5 * 60)
        #print("in" + rv1)
    return rv



@app.route('/')
def index():
    return 'OK'

# TODO add all routes here!
@app.route('/getdata/<todate>', methods=['GET'])
def getallitems(todate): 
    #todate = 
    flag = cbf.testdate(string_to_date(todate))
    print(flag)
    if flag == 1:
        js = get_my_item(todate)
    else :
        js = 'Data not present'
    return Response(js,status=200,mimetype='text/json')

# returns csv file

def generate_text_data(todate):
    data = bl.get_val(todate)
    header =[('timestamp','alti','drct','dwpf','gust','id','mnet','p24i','pmsl','relh','selv','sknt','slat','slon','stn','tmpf','wthr')]
    data = header + data
    #,'drct','dwpf','gust','id','mnet','p24i','pmsl','relh','selv','sknt','slat','slon','stn','tmpf','wthr') + '\n'
    def generate():
        for row in data:
            d1 = str(row[0]) + '\t' \
                 + str(row[1]) + '\t' \
                 + str(row[2]) + '\t' \
                 + str(row[3]) + '\t' \
                 + str(row[4]) + '\t' \
                 + str(row[5]) + '\t' \
                 + str(row[6]) + '\t' \
                 + str(row[7]) + '\t' \
                 + str(row[8]) + '\t' \
                 + str(row[9]) + '\t' \
                 + str(row[10]) + '\t' \
                 + str(row[11]) + '\t' \
                 + str(row[12]) + '\t' \
                 + str(row[13]) + '\t' \
                 + str(row[14]) + '\t' \
                 + str(row[15]) + '\t' \
                 + str(row[16])
            yield ''.join(d1) + '\n'
    return generate()

@app.route('/getcsvdata/<todate>', methods=['GET'])
def get_text_data(todate):
    flag = cbf.testdate(string_to_date(todate))
    if flag == 1:
        cache = SimpleCache()
        rv = cache.get('textdata')
        if rv is None:
            data = generate_text_data(todate)
            #cache.set('textdata', data, timeout=5 * 60)
    else :
        data = 'Data not present'
    return Response(data,status=200,mimetype='text/text')

'''
@app.route('/insertdata', methods=['POST'])
def run():
  # TODO add concurency
  # parse using specific reader
''' 
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)