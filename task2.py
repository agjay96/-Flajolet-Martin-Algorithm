from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import json
import binascii
import datetime
import sys
import random

output_file = sys.argv[2]
number_of_hashes = 14


def myhashs(s):
	v=int(binascii.hexlify(s.encode('utf8')), 16)
	r=[]
	for i in range(14):
		r.append((a_list[i]*v +b_list[i])%69997)
	return r

def hashes_a_b():
	a=[]
	b=[]
	for i in range(14):
		a.append(random.randint(0,1516189))
		b.append(random.randint(0,1516189))
	return a,b



def flajolet_martin(rdd):

	f = open(output_file, "a+")

	maxs = [0]*number_of_hashes

	len_trailing_zeros = [0]*number_of_hashes

	unique = {}

	rdd = rdd.collect()

	for record in rdd:

		res=myhashs(record)
		hash_ = int(binascii.hexlify(record.encode('utf8')), 16)
		bin_strings = [bin(((a_list[i]*hash_ + b_list[i])%853211)%16384)[2:] for i in range(14)]

		for i in range(14):

			if len(bin_strings[i]) != len(bin_strings[i])-len(bin_strings[i].rstrip('0')):
				len_trailing_zeros[i] = len(bin_strings[i])-len(bin_strings[i].rstrip('0'))

		for i in range(len(len_trailing_zeros)):
			if len_trailing_zeros[i] > maxs[i]:
				maxs[i] = len_trailing_zeros[i]

		unique[record] = 1

	a = []

	for i in range(0, len(maxs)):
		a.append(int(2**maxs[i]))

	a = sorted(a)
	t=len(unique)
	dif=len(unique)
	for i in range(0, 14, 2):
		avg=(a[i]+a[i+1])/2
		if(abs(avg-t)<dif):
			dif=abs(avg-t)
			median=avg

	f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(len(unique)) + "," + str(int(median)) + "\n")

	f.close()


if __name__ == "__main__":

	f = open(output_file, "w+")
	f.write("Time,Ground Truth,Estimation\n")
	f.close()

	sc = SparkContext()
	sc.setLogLevel(logLevel="ERROR")

	a_list,b_list= hashes_a_b()
	port = int(sys.argv[1])
	scc = StreamingContext(sc, 5)
	streaming_c = scc.socketTextStream("localhost", port)

	windowed_streaming_c = streaming_c.window(30, 10)
	windowed_streaming_c.foreachRDD(flajolet_martin)

	scc.start()
	scc.awaitTermination()