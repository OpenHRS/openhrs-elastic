import os, requests, json, copy, certifi
from elasticsearch import Elasticsearch
from urllib.request import urlopen

es = Elasticsearch([os.environ["ELASTIC_URL"]], use_ssl=True, ca_certs=certifi.where(), 
    timeout=80, max_retries=10, retry_on_timeout=True)

if not es.ping():
    raise ValueError("Connection failed")
else:
    print("Successfully connected to: " + os.environ["ELASTIC_URL"] + "\n")

years    = ["hrs2002"]
#years    = ["hrs2002","hrs2003","hrs2004","hrs2005","hrs2006","hrs2007","hrs2008","hrs2009","hrs2010",
#            "hrs2011","hrs2012","hrs2013","hrs2014","hrs2015","hrs2016","hrscurrent"]
base_url = "https://raw.githubusercontent.com/OpenHRS/openhrs-data/master/"

for year in years:
    tree = requests.get(url=base_url+year+"/"+year+"_notext.json")
    payload = []

    index = {
        "index" : {
            "_index" : "hrs_statutes",
            "_type" : year[3:]
        }
    }

    print("begin load " + year)
    
    for div in tree.json():
        for title in div["titles"]:
            for chapt in title["chapters"]:

                doc = {
                    "year": year[3:],
                    "div_name": div["name"],
                    "div_num": div["number"],
                    "title_name": title["name"],
                    "title_num": title["number"],
                    "chapt_name": chapt["name"],
                    "chapt_num": chapt["number"],
                    "repealed": chapt["repealed"]
                }

                if (not chapt["repealed"]):
                    for sec in chapt["sections"]:
                        sec_path  = "../openhrs-data/" + year + "/division/"
                        sec_path += str(div["number"]) + "/title/" + str(title["number"]) + "/chapter/" 
                        sec_path += str(chapt["number"]) + "/section/" + str(chapt["number"]) + "-" 
                        sec_path += str(sec["number"]) + ".json"
                        
                        data = json.load(open(sec_path))
                        doc2 = copy.deepcopy(doc)

                        doc2["sec_name"] = data["name"]
                        doc2["sec_num"]  = data["number"]
                        doc2["sec_text"] = data["text"]

                        payload.append(index)
                        payload.append(doc2)
                else:
                    payload.append(index)
                    payload.append(doc)
    
    counter = 0
    temp = []

    for i in range(0,len(payload), 2):
        if counter > 4000:
            es.bulk(temp)
            counter = 0
            temp = []

        temp.append(payload[i])
        temp.append(payload[i+1])

        counter += 1

    ex.bulk(temp)
    
    print("finish load " + year + "\n")
