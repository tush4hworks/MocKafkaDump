import sys
import json
import random
import datetime
import threading
import time
import itertools
import commands
import requests

class record:
	def __init__(self):
		self.basejson={
		"time": None,
		"channel": None,
		"cityName": None,
		"comment":None,
		"countryIsoCode": None,
		"countryName": None,
		"isAnonymous":None,
		"isMinor":None,
		"isNew":None,
		"isRobot":None,
		"isUnpatrolled":None,
		"metroCode":None,
		"namespace":None,
		"page":None,
		"regionIsoCode":None,
		"regionName":None,
		"user":None,
		"delta":None,
		"added":None,
		"deleted":None
		}
		self.channels=open("channel.txt","r+").readlines()
		self.basedata=iter(open("wikiticker-2015-09-12-sampled.json","r+").readlines())
		self.locations=open("location.txt","r+").readlines()
		self.comments=open("comment.txt","r+").readlines()
		self.users=open("user.txt","r+").readlines()
		self.namespaces=open("namespace.txt","r+").readlines()
		self.pages=open("page.txt","r+").readlines()
		self.comments=open("comment.txt","r+").readlines()
		with open("params.json") as params:
			self.params=json.load(params)
		self.kafkaString="echo '{}' | /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list c01s02.hadoop.local:6667 --topic "+self.params["wrap"]["topicName"]


	def runSetup(self,topicName):
		self.postSpec(self.params["wrap"]["supervisor_address"],self.params["wrap"]["ingestionSpec"])
		self.runCommand("/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic {} --replication-factor 1 --partitions 1".format(topicName),True)

	def postSpec(self,supervisor_address,ingestionSpec):
		try:
			resp=requests.post(supervisor_address+"/druid/indexer/v1/supervisor",headers={"Content-Type":"application/json"},data=json.dumps(ingestionSpec))
			print resp.content
			if not(resp.status_code==200):
				raise Exception
		except Exception as e:
			print e
			print "Ingestion Task not submitted to supervisor. Will stop here.."
			sys.exit(1)

	def runCommand(self,command,printOutput=False):
		print command
		try:	
			s,o=commands.getstatusoutput(command)
			if printOutput:
				print s
				print o
		except Exception as e:
			print e
			pass

	def getRecord(self):
		while True:
			deltamillisecs=random.choice(range(self.params["wrap"]["max_wait_per_clicker"]))
			time.sleep(deltamillisecs/1000.0)
			try:
				self.runCommand(self.kafkaString.format(json.dumps(self.alterTimeStamp(json.loads(self.basedata.next())))))
			except StopIteration:
				self.runCommand(self.kafkaString.format(json.dumps(self.buildRecord())))
			except Exception:
				pass
		
	def alterTimeStamp(self,sample_json):
		sample_json["time"]=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"Z"
		print sample_json["time"]
		return sample_json

	def buildRecord(self):
		rec=dict(self.basejson)
		rec["time"]=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"Z"
		rec["channel"]=random.choice(self.channels).strip(" \n")
		rec["namespace"]=random.choice(self.namespaces).strip(" \n")
		rec["user"]=random.choice(self.users).strip(" \n")
		rec["comment"]=random.choice(self.comments).strip(" \n")
		rec["page"]=random.choice(self.pages).strip(" \n")
		rec["cityName"],rec["countryName"],rec["countryIsoCode"],rec["metroCode"],rec["regionName"],rec["regionIsoCode"]=[item.strip(" \n\r") if not(item.strip(" \n\r")=="NULL") else None for item in random.choice(self.locations).split(",")]
		rec["isNew"],rec["isRobot"],rec["isUnpatrolled"],rec["isAnonymous"],rec["isMinor"]=[random.choice([True,False]),random.choice([True,False]),random.choice([True,False]),random.choice([True,False]),random.choice([True,False])]	
		rec["added"],rec["deleted"]=[random.choice(range(1000)),random.choice(range(1000))]
		rec["delta"]=rec["added"]-rec["deleted"]
		print rec["time"]
		return rec

	def generateData(self,n_threads):
		gthreads=[]
		for i in range(n_threads):
			gthreads.append(threading.Thread(target=self.getRecord,args=()))
		for t in gthreads:
			t.start()
		for t in gthreads:
			t.join()

p=record()

if p.params["wrap"]["runSetup"].lower()=="true":
	p.runSetup(p.params["wrap"]["topicName"])

p.generateData(p.params["wrap"]["clickers"])
