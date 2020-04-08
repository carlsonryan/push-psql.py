import psycopg2
import glob

conn = psycopg2.connect("host=192.168.1.14 dbname=ttd user=postgres")
cur = conn.cursor()
for name in glob.glob('/Users/ryan.carlson/scripts/test/s3/tmp/*'):
	with open(name, 'r') as f:
		cur.copy_from(f, 'conversions', sep='\t')
		conn.commit()
		print("Pushed LogFiles To TTD.CONVERSIONS!")

		postgreSQL_select_Query = "SELECT date(logentrytime), conversionid, advertiserid, conversiontype, tdid, ipaddress, referrerurl, monetaryvalue, monetaryvaluecurrency, orderid, processedtime from conversions limit 6" 
		cur.execute(postgreSQL_select_Query)

		all = cur.fetchall()
		for row in all:
			print("   ", row[0], "   ", row[1], "   ", row[2], "   ", row[3], "   ", row[4], "   ", row[5], "   ", row[6], "   ", row[7], "   ", row[8], "   ", row[9], "   ", row[10])
