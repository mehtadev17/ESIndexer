import sys
import urllib2
import json

from elasticutils import get_es

id_field = 'DocumentID'

# mapping is kinda like schema for ElasticSearch
mapping = {'properties': {
                'DocumentID': {'type': 'string', 'index': 'not_analyzed'},
                'JobTitle': {'type': 'string', 'index': 'analyzed'},  #
                'OrganizationName': {'type': 'string', 'index': 'analyzed'},
                'AgencySubElement': {'type': 'string', 'index': 'not_analyzed'},
                'SalaryMin': {'type': 'string', 'index': 'not_analyzed'},
                'SalaryMax ': {'type': 'string', 'index': 'not_analyzed'}, #
                'SalaryBasis': {'type': 'string', 'index': 'not_analyzed'}, #
                'StartDate': {'type': 'string', 'index': 'not_analyzed'},
                'EndDate': {'type': 'string', 'index': 'not_analyzed'},
                'WhoMayApplyText': {'type': 'string', 'index': 'analyzed'}, #
                'PayPlan': {'type': 'string', 'index': 'not_analyzed'},
                'Series': {'type': 'string', 'index': 'not_analyzed'},
                'Grade': {'type': 'string', 'index': 'not_analyzed'},
                'WorkType': {'type': 'string', 'index': 'analyzed'},
                'WorkSchedule': {'type': 'string', 'index': 'not_analyzed'},
                'Locations': {'type': 'string', 'index': 'not_analyzed'},
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
    es = get_es(urls=['localhost:9200'])
    es.indices.create(index=name)

    es.indices.put_mapping(index=name, doc_type=name, body={name: mapping })

index_name = sys.argv[1]

es = get_es(urls=['localhost:9200'])

if es.indices.exists(index=index_name): 
    print "index %s exists, removing ..." % index_name
    es.indices.delete(index=index_name)
    print "now re-creating index ..."
else:
    print "creating index ..."
create_index(index_name, mapping)
print "now indexing data ..."

for i in range(1, 201):
    url = 'https://data.usajobs.gov/api/jobs?page='+str(i)
    response = urllib2.urlopen(url)
    data = json.load(response)
    jobData = data['JobData']
    for job in jobData:
        es.index(index=index_name, doc_type=index_name, body=job, id=job[id_field])







