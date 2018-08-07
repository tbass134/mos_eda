import requests
import json
import csv
import sys

def get_rows(hdap_ids=None, size=500, csvFile="data.csv"):

	url = "http://hadoop-slave-23.kewr0.m.vonagenetworks.net:9200/mobile_telemetry_calls-%2A/_search"
	payload = {
	  "version": "true",
	  "size": size,
	  "sort": [
	    {
	      "@timestamp": {
	        "order": "desc",
	        "unmapped_type": "boolean"
	      }
	    }
	  ],
	  "_source": {
	    "excludes": []
	  },
	  "aggs": {
	    "2": {
	      "date_histogram": {
	        "field": "@timestamp",
	        "interval": "30s",
	        "time_zone": "UTC",
	        "min_doc_count": 1
	      }
	    }
	  },
	  "stored_fields": [
	    "*"
	  ],
	  "script_fields": {
	    "TRANSPORT_LAYER_WITH_DEF": {
	      "script": {
	        "inline": "if (doc.containsKey('TRANSPORT_LAYER'))  { return (doc['TRANSPORT_LAYER'].value + \"\") }\nelse { return \"null\" }",
	        "lang": "painless"
	      }
	    },
	    "AUDIO_JITTER_AVG_NUM": {
	      "script": {
	        "inline": "if (doc.containsKey('AUDIO_JITTER_AVG')) { return 1 } else { return 0 }",
	        "lang": "painless"
	      }
	    }
	  },
	  "docvalue_fields": [
	    "@timestamp"
	  ],
	  "query": {
   	 	"terms" : {
        	"HDAP_TRACE_ID" : hdap_ids
    	}
   	}

	}


	headers = {
		'Content-Type': "application/json"
	}

	response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
	jsonResponse = response.json()['hits']['hits']

	if len(jsonResponse) == 0:
		print("No Results Found")
		return

	from operator import itemgetter, attrgetter
	keys = sorted(jsonResponse[0]['_source'], key=itemgetter(0))

	outputFile = open(csvFile, 'w', newline='')
	outputWriter = csv.DictWriter(outputFile, fieldnames = keys, extrasaction='ignore')
	outputWriter.writeheader()

	for row in jsonResponse:
		outputWriter.writerow(row["_source"])
	outputFile.close()
	print("Data written to {}".format(csvFile))

if __name__ == '__main__':

	hdap_ids = input("HDAP Ids (Comma seperated list): ").split(",")
	if not any(hdap_ids):
		 sys.exit("1 or more hdap IDs are required")
	else:
		print("Loading Data..")
		get_rows(hdap_ids=hdap_ids)
