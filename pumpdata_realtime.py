import sys
import json
import random
import datetime
import threading
import time
import itertools
import commands

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
		self.index=0
		self.kafkaString="echo '{}' | /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list c01s02.hadoop.local:6667 --topic wikipedia1"

	def runCommand(self,command):
		try:
			pass
			#print command
			#s,o=commands.getstatusoutput(command)
		except Exception:
			pass

	def getRecord(self):
		while True:
			deltasecs=random.choice(range(4))
			time.sleep(deltasecs)
			try:
				self.runCommand(self.kafkaString.format(json.dumps(self.buildRecord())))
			except Exception:
				pass
		
	def buildRecord(self):
		rec=dict(self.basejson)
		rec["time"]=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.200Z")
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
p.generateData(10)