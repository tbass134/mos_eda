import requests
import json
import csv

def get_rows(hdap_ids=["67862709-5638-4ac8-af48-27940985c7c7", "6c0aeb6b-635f-40bb-9363-8b8de0b467a1"], size=500, csvFile="data.csv"):

	url = "http://hadoop-slave-23.kewr0.m.vonagenetworks.net:9200/mobile_telemetry_calls-%2A/_search"
	payload = {
  "version": "true",
  "size": 5,
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
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "range": {
            "@timestamp": {
              "gte": 1533638769653,
              "lte": 1533639669653,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "filter": [],
      "should": [],
      "must_not": []
    }
  }

}


	headers = {
		'Content-Type': "application/json"
	}

	response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
	jsonResponse = response.json()['hits']['hits']

	from operator import itemgetter, attrgetter
	keys = sorted(jsonResponse[0]['_source'], key=itemgetter(0))

	outputFile = open(csvFile, 'w', newline='')
	outputWriter = csv.DictWriter(outputFile, fieldnames = keys)
	outputWriter.writeheader()

	for row in jsonResponse:
		outputWriter.writerow(row["_source"])
	outputFile.close()

if __name__ == '__main__':
  get_rows()