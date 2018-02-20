#!C:\Users\c.alvarez\AppData\Local\Programs\Python\Python36\python.exe
#
# Script for finding information on oozie web API regarding some workflows
# @author	Cristian Gómez Alvarez <cristianpark@gmail.com>
#
# Licensed under Creative Commons Attribution 4.0 International. https://creativecommons.org/licenses/by/4.0/
#
# Sample Oozie jobs URL: GET /oozie/v1/jobs?filter=user%3Dbansalm&offset=1&len=50&timezone=GMT
# Oozie web API reference: https://oozie.apache.org/docs/4.0.0/WebServicesAPI.html
#
# 	urlString="http://OOZIE-LOCATION/oozie/v2/jobs"
#	Useful URL's: 
#		http://OOZIE-LOCATION/oozie/v2/jobs?filter=user%3DUSER&offset=1&len=500&timezone=GMT
#		http://OOZIE-LOCATION/oozie/v2/jobs?filter=name%3DWORKFLOW-NAME_DATE&offset=1&len=500&timezone=GMT
#		http://OOZIE-LOCATION/oozie/v2/jobs?filter=user%3DUSER&status=RUNNING&offset=1&len=500&timezone=GMT
#
#	TODO: Sort results
#

import cgitb
import urllib.request, json
import cgi
cgitb.enable()


#Print HTML headers
print("""
<!DOCTYPE html>
<html>

<head>
  <title>Oozie Jobs Monitoring</title>
</head>

<style>
th {
    padding-top: 12px;
    padding-bottom: 12px;
    padding-left: 1em;
    padding-right: 1em;
    text-align: left;
    background-color: #4CAF50;
    color: white;
}

td {
	padding-left: 1em;
    padding-right: 1em;
}

#DVresults {
	padding-left: 5em;
	border-radius: 25px;
    border: 2px solid #73AD21;
    padding: 20px;
}
</style>

<body>""")

#Parameter of the workflows to search
workflowsArray=[]

#Get POST fields
form = cgi.FieldStorage()

if(form.getvalue("SLworkflows")==None and form.getvalue("ITworkflow")==None):
	print('<strong><span color="red">No parameter recieved</span></strong>')
elif(form.getvalue("ITworkflow")!=None):
	workflowsArray.append(form.getvalue("ITworkflow"))
elif(form.getvalue("SLworkflows")!=None):
	if(isinstance(form.getvalue("SLworkflows"), list)):
		workflowsArray=form.getvalue("SLworkflows")
	else:
		workflowsArray.append(form.getvalue("SLworkflows"))

statusFilter=form.getvalue("SLstatus")														#Filter for status
jobsFilter=form.getvalue("ITjobsNumber") if form.getvalue("ITjobsNumber")!=None else 500	#Filter for jobs number

#Get JSON response from Oozie API
data=""
urlString="http://OOZIE-LOCATION/oozie/v2/jobs?filter=status%3D{0}&offset=1&len={1}&timezone=GMT".format(statusFilter, jobsFilter)
results=0
responseDict={}

#Initialize the responseDict for every workflow name
for worflow in workflowsArray:
	responseDict[worflow]=[]

with urllib.request.urlopen(urlString) as url:
	data = json.loads(url.read().decode())

print("""<div id="DVresults" align="center"><h1 style="font-size:2em;">MATCHING COORDINATORS</h1></div><br/>""")

for item in data["workflows"]:
	k=item["appName"].rfind("_")
	name=item["appName"][:k]
	date=item["appName"][k+1:]

	if name in workflowsArray:
		results+=1

		fields={
				'DateInstance': date,
				'CoordInstance': item["parentId"], 
				'Status': item["status"], 
				'StartTime': item["startTime"], 
				'LastModTime': item["lastModTime"], 
				'EndTime': item["endTime"], 
				'ConsoleURL': item["consoleUrl"]
				}
		responseDict[name].append(fields)

#Show results
for key in responseDict:
	print('<h1 style="background-color:green; color:white; padding:0.5em; width: 60%">Results for {0} (Total executions {1})</h1>'.format(key, len(responseDict[key])));
	resultsKey=0;

	for data in responseDict[key]:
		resultsKey+=1

		print("""<br />
		<h2 style="padding-left: 2em;">#{0} Date instance: {1}</h2>

		<div align="center" style="padding-left: 2em;">
		<table style="border-collapse: collapse;" border="2">
			<tr>
				<th>Coord/instance</th>
				<td>{2}</td>
			</tr>
			<tr>
				<th>Status</th>
				<td>{3}</td>
			</tr>
			<tr>
				<th>Start Time</th>
				<td>{4}</td>
			</tr>
			<tr>
				<th>LastModified Time</th>
				<td>{5}</td>
			</tr>
			<tr>
				<th>End Time</th>
				<td>{6}</td>
			</tr>
			<tr>
				<th>ConsoleUrl</th>
				<td>{7}</td>
			</tr>
		</table>
		</div><br />
		""".format(resultsKey, data['DateInstance'], data['CoordInstance'], data['Status'], data['StartTime'], data['LastModTime'], data['EndTime'], data['ConsoleURL']))
	print('<h3 style="background-color:#e3e3e3; padding: 1em; padding-left:2em; width: 30%" align="center">Workflow {0} executions: {1}</h3>'.format(key, resultsKey))

print('<h2 style="background-color:#2d7dc6; color:white; padding-left:4em; padding: 1em; width: 80%" align="center">TOTAL COORDINATORS MATCHED: {0}</h2>'.format(results))

print("""</body>
	</html>""")