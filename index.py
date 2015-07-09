import sys
import urllib2
import httplib
import json

from elasticutils import get_es

id_field = 'DocumentID'
es = get_es(urls=sys.argv[1])

#Index Settings

settings = {"analysis" : {
                "tokenizer" : {
                    "semicolon_token" : {
                        "type" : "pattern",
                        "pattern" : ";"
                    }, 
                },
                "analyzer" : {
                    "semicolonanalyzer" : {
                        "type" : "custom",
                        "tokenizer" : "semicolon_token",
                    },
            }
        }
    }

#ElasticSearch schema mapping
mapping = {'properties': {
                'DocumentID': {'type': 'string', 'index': 'not_analyzed'},
                'JobTitle': {
                    "type": "string",
                    "fields": {
                        "raw" : {
                          "type": "string",
                          "index": "not_analyzed"
                        }
                    }
                },  #
                'OrganizationName': {                    
                    "type": "string",
                    "fields": {
                        "raw" : {
                          "type": "string",
                          "index": "not_analyzed"
                        }
                    }
                },
                'AgencySubElement': {'type': 'string', 'index': 'not_analyzed'},
                'SalaryMin': {'type': 'string', 'index': 'not_analyzed'},
                'SalaryMax ': {'type': 'string', 'index': 'not_analyzed'}, #
                'SalaryBasis': {'type': 'string', 'index': 'not_analyzed'}, #
                'StartDate': {'type': 'string', 'index': 'not_analyzed'},
                'EndDate': {'type': 'string', 'index': 'not_analyzed'},
                'WhoMayApplyText': {'type': 'string', 'index': 'analyzed'}, #
                'PayPlan': {'type': 'string', 'index': 'not_analyzed'},
                'Series': {'type': 'string', 'index': 'not_analyzed'},
                'Grade': {'type': 'string', 'index': "not_analyzed"},
                'WorkType': {'type': 'string', 'index': 'analyzed'},
                'WorkSchedule': {'type': 'string', 'index': 'not_analyzed'},
                'Locations': {'type': 'string', 'index': 'analyzed', 'index_analyzer': 'semicolonanalyzer'},
                'AnnouncementNumber': {'type': 'string', 'index': 'not_analyzed'},
                'JobSummary': {'type': 'string', 'index': 'analyzed'},
                'ApplyOnlineURL': {'type': 'string', 'index': 'not_analyzed'},
            }
        }

def create_index(name, mapping):
    """
    create index with mapping,
    for simplicity, here we assume the index has only one type of document
    and the name of the doc_type is identical to the index 
    """
    es.indices.create(index=name, body={"settings":settings})

    es.indices.put_mapping(index=name, doc_type=name, body={name: mapping})

index_name = 'jobs'

if es.indices.exists(index=index_name): 
    print "index %s exists, removing ..." % index_name
    es.indices.delete(index=index_name)
    print "now re-creating index ..."
else:
    print "creating index ..."
create_index(index_name, mapping)
print "now indexing data ..."
base_url = 'https://data.usajobs.gov/api/jobs?&Series='
with open(sys.argv[2]) as data_file:    
    seriesJson = json.load(data_file)
for series in seriesJson:
    code=series['Code']
    page_url = base_url+code
    response = urllib2.urlopen(page_url)
    data = json.load(response)
    pages = data['Pages']
    page_int = int(pages)
    if page_int > 1:
        for i in range(1, page_int+1):
            url = page_url+'&Page='+str(i)
            print url
            final_response = urllib2.urlopen(url)
            data = json.load(final_response)
            jobData = data['JobData']
            for job in jobData:
                es.index(index=index_name, doc_type=index_name, body=job, id=job[id_field], request_timeout=30)
    else:
        jobData = data['JobData']
        for job in jobData:
            es.index(index=index_name, doc_type=index_name, body=job, id=job[id_field], request_timeout=30)