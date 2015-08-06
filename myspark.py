# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

################ 数据预处理 ################ 
def extract(line):
	import time
	try:
		fromaccount, toaccount, syscode, timestamp, amount = line.strip().split("\t")
		date, slot, amount = timestamp.split(" ")[0].replace("-",""), int(timestamp.split(" ")[1].split(":")[0])*6+int(timestamp.split(" ")[1].split(":")[1])/10, float(amount)/100
		pytime = time.strptime(timestamp,"%Y-%m-%d %H:%M:%S")
		wd = {0:"WD",1:"WD",2:"WD",3:"WD",4:"WD",5:"WE",6:"WE"}[pytime.tm_wday]
		account = accounts[fromaccount]
		gender, age, grade, stype = account["gender"], account["age"], account["grade"], account["stype"]
		merchant = merchants[toaccount]
		accountname, mtype = merchant["accountname"], merchant["mtype"]
		qixiang = qixiangs[date] if date in qixiangs else {"temp":"NA","rain":"NA","wind":"NA"}
		temp, rain, wind = qixiang["temp"], qixiang["rain"], qixiang["wind"]
		return (fromaccount, toaccount, timestamp, date, slot, wd, amount, gender, age, grade, stype, accountname, mtype, temp, rain, wind)
	except:
		return ("")

global accounts, merchants, qixiang

if __name__ == "__main__":
	accounts, merchants, qixiangs = {}, {}, {}
	import fileinput
	for line in fileinput.input("data/account.txt"):
		(account, studentcode, gender, yearofbirth, grade, stype) = line.strip().split("\t")
		gender, age, grade, stype = {"男":"M","女":"F"}[gender], 2015-int(yearofbirth), 2015-int(grade), {"本科":"U","硕士":"M","博士":"P"}[stype]
		accounts[account] = {"account":account,"gender":gender,"age":age,"grade":grade,"stype":stype}
	fileinput.close()
	# for account in accounts:
	# 	print accounts[account]
	for line in fileinput.input("data/merchant.txt"):
		syscode, codename, toaccount, accountname, address, opendata, mtype = line.strip().split("\t")
		if 1 <= int(mtype) <= 7:
			mtype = {"1":"一餐","2":"二餐","3":"三餐","4":"四餐","5":"五餐","6":"六餐","7":"哈乐"}[mtype]
			merchants[toaccount] = {"toaccount":toaccount,"accountname":accountname,"mtype":mtype}
	fileinput.close()
	# for merchant in merchants:
	# 	print merchants[merchant]
	for line in fileinput.input("data/qixiang.txt"):
		date, temp, rain, wind = line.strip().split("\t")
		qixiangs[date] = {"date":date,"temp":float(temp),"rain":float(rain),"wind":float(wind)}
	fileinput.close()
	# for date in qixiangs:
	# 	print qixiangs[date]
	conf = (SparkConf()
    	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
    	.setAppName("Extract")
    	.set("spark.cores.max", "32")
    	.set("spark.driver.memory", "4g")
		.set("spark.executor.memory", "6g"))
	sc = SparkContext(conf = conf)
	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
	lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/EMC/trade.txt', 1)
	counts = lines.map(lambda x : extract(x)) \
				  .filter(lambda x : x != "") \
				  .distinct() \
				  .map(lambda x : "\t".join([str(i) for i in x]))
	output = counts.saveAsTextFile("./EMC/transaction/data")

# ################ 基本统计分析 ################ 
# def extract(line):
# 	import time
# 	try:
# 		(fromaccount, toaccount, timestamp, date, slot, wd, amount, gender, age, grade, stype, window, canteen, temp, rain, wind) = line.strip().split("\t")
# 		# return ((canteen), (1,float(amount)))
# 		# return ((canteen+"\t"+window), (1,float(amount)))
# 		# return ((canteen, date), (1,float(amount)))
# 		# return ((canteen, str(time.strptime(timestamp,"%Y-%m-%d %H:%M:%S").tm_wday)), (1,float(amount)))
# 		# return ((canteen, slot.zfill(3)), (1,float(amount)))
# 		# return ((gender, canteen), (1,float(amount)))
# 		# return ((gender, window), (1,float(amount)))
# 		# grade = stype+grade if stype == "U" and 1 <= int(grade) <= 4 else stype if stype in ["M","P"] else "W"
# 		# return ((grade, canteen), (1,float(amount)))
# 		# return ((grade, window), (1,float(amount)))
# 		# temp = (max(min(int(float(temp)),29),5)-5)/5
# 		# return ((str(temp), date), (1,float(amount)))
# 		rain = 0 if float(rain) <= 1 else 1 if float(rain) <=5 else 2 if float(rain) <=20 else 3 if float(rain) <=100 else 4
# 		if wd == "WD" and 20141008 <= int(date) <= 20141231:
# 			return ((str(rain), date), (1,float(amount)))
# 		else:
# 			return ("")
# 	except:
# 		return ("")

# if __name__ == "__main__":
# 	conf = (SparkConf()
#     	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
#     	.setAppName("Extract")
#     	.set("spark.cores.max", "32")
#     	.set("spark.driver.memory", "4g")
# 		.set("spark.executor.memory", "6g"))
# 	sc = SparkContext(conf = conf)
# 	day_len = 123
# 	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
# 	# lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/EMC/transaction/data', 1)
# 	# counts = lines.map(lambda x : extract(x)) \
# 	# 			  .filter(lambda x : x != "") \
# 	# 			  .reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1])) \
# 	# 			  .repartition(1) \
# 	# 			  .map(lambda x : x[0][0]+"\t"+x[0][1]+"\t"+str(x[1][0])+"\t"+str(x[1][1])+"\t"+str(x[1][1]/x[1][0]))
# 	counts = lines.map(lambda x : extract(x)) \
# 				  .filter(lambda x : x != "") \
# 				  .reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1])) \
# 				  .repartition(1) \
# 				  .sortByKey() \
# 				  .map(lambda x : (x[0][0],x[1])) \
# 				  .groupByKey() \
# 				  .map(lambda x : (x[0]+"\t"+str(int(sum([i[0] for i in x[1]])/len(x[1])))+"\t"+str(int(sum([i[1] for i in x[1]])/len(x[1])))))
# 	# output = counts.saveAsTextFile("./EMC/transaction/tree_canteen")
# 	# output = counts.saveAsTextFile("./EMC/transaction/tree_window")
# 	# output = counts.saveAsTextFile("./EMC/transaction/timeline_canteen_date")
# 	# output = counts.saveAsTextFile("./EMC/transaction/timeline_canteen_slot")
# 	# output = counts.saveAsTextFile("./EMC/transaction/radar_canteen_wd")
# 	# output = counts.saveAsTextFile("./EMC/transaction/loop_gender_canteen")
# 	# output = counts.saveAsTextFile("./EMC/transaction/loop_gender_window")
# 	# output = counts.saveAsTextFile("./EMC/transaction/pie_grade_canteen")
# 	# output = counts.saveAsTextFile("./EMC/transaction/pie_grade_window")
# 	# output = counts.saveAsTextFile("./EMC/transaction/mix_temp")
# 	output = counts.saveAsTextFile("./EMC/transaction/mix_rain")

# ################ 用户行为分析 ################
# def extract(line):
# 	try:
# 		(fromaccount, toaccount, timestamp, date, slot, wd, amount, gender, age, grade, stype, window, canteen, temp, rain, wind) = line.strip().split("\t")
# 		# return ((fromaccount), (timestamp, canteen))
# 		return ((fromaccount), (timestamp, window))
# 	except:
# 		return ("")

# def proc(x):
# 	try:
# 		items = sorted([{"timestamp":item[0],"canteen":item[1]} for item in x], key=lambda x:x["timestamp"])
# 		return [items[i]["canteen"]+","+items[i+1]["canteen"] for i in xrange(0,len(items)-1)]
# 	except:
# 		return ("")

# if __name__ == "__main__":
# 	conf = (SparkConf()
#     	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
#     	.setAppName("Extract")
#     	.set("spark.cores.max", "32")
#     	.set("spark.driver.memory", "4g")
# 		.set("spark.executor.memory", "6g"))
# 	sc = SparkContext(conf = conf)
# 	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
# 	lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/EMC/transaction/data', 1)
# 	def f(x): return x
# 	counts = lines.map(lambda x : extract(x)) \
# 				  .filter(lambda x : x != "") \
# 				  .groupByKey() \
# 				  .map(lambda x : (x[0], proc(x[1]))) \
# 				  .filter(lambda x : x != "") \
# 				  .flatMapValues(f) \
# 				  .map(lambda x : (x[1], 1)) \
# 				  .reduceByKey(lambda x,y : x+y) \
# 				  .sortByKey() \
# 				  .map(lambda x : x[0]+"\t"+str(x[1])) \
# 				  .repartition(1)
# 	# output = counts.saveAsTextFile("./EMC/transaction/chord_canteen_transfer")
# 	output = counts.saveAsTextFile("./EMC/transaction/chord_window_transfer")
