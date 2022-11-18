## ************************************************************************** ##
##                                                                            ##
##             main.py for 42 Istanbul                	      			      ##
##             Created on  : Nov 19 20:36:15 2022                             ##
##             Last update : Nov 19 02:04:47 2022                             ##
##             Made by : Hakan "agunes" Güneş <ahmethakangunes24@gmail.com>   ##
##                                                                            ##
## ************************************************************************** ##
import psycopg2
import requests
import time
import pandas as pd
import datetime
from datetime import datetime


username = 'doadmin'
password = 'AVNS_NU2lKTqXn97G8D4z_3J'
host = 'db-postgresql-fra1-27675-do-user-12582068-0.b.db.ondigitalocean.com'
port = '25060'
database = 'students42'
sslmode = 'require'

conn = psycopg2.connect(
    user = username,
    password = password,
    host = host,
    port = port,
    database = database,
    sslmode = sslmode 
)

part1projects = ["Libft", "get_next_line", "ft_printf", "Born2beroot", "push_swap", "Exam Rank 02", "minitalk", "so_long", "pipex", "FdF", "fract-ol"]
part2projects = ["minishell", "Exam Rank 03", "Philosophers", "NetPractice", "cub3d", "miniRT", "CPP Module 00", "CPP Module 01", "CPP Module 02", "CPP Module 03", "CPP Module 04", "CPP Module 05", "CPP Module 06", "CPP Module 07", "CPP Module 08", "Exam Rank 04"]
part3projects = ["webserv", "ft_irc", "ft_containers", "Exam Rank 05", "ft_transcendence", "Exam Rank 06", "Inception"]
id = 0

def createtable(name):
	create = conn.cursor()
	query_table = """
	CREATE TABLE IF NOT EXISTS students(
	id SERIAL PRIMARY KEY,
	login TEXT,
	fullname TEXT,
	part TEXT,
	blackhole TEXT,
	lastseen TEXT,
	mail TEXT,
	birthdate TEXT
		)"""
	create.execute(query_table)
	conn.commit()

def insert(id, login, fullname, part, blackhole, lastseen, mail, birthdate):
	print("dönüyorum...")
	insert = conn.cursor()
	query_insert = "INSERT INTO students (id, login, fullname, part, blackhole, lastseen, mail, birthdate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
	val = (id, login, fullname, part, blackhole, lastseen, mail, birthdate)
	insert.execute(query_insert, val)
	conn.commit()

def day(blackhole):
	lastday = datetime.strptime(blackhole,"%Y-%m-%dT%H:%M:%S.%fZ")
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	now = datetime.now().strptime(now, "%Y-%m-%d %H:%M:%S")
	blackhole = str(lastday - now)
	if blackhole.find("-") == 0:
		return 0
	else:
		return blackhole.split(" ")[0]

def getprivateinfo(login):
	gsheetid = "1WfPZBxW5RhMX5o5jk352f9ZWVxCpMXoe"
	sheet_name = ""
	gsheet_url = "https:##docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(gsheetid, sheet_name)
	istanbuldf = pd.read_csv(gsheet_url, index_col ="Login")
	try:
		istanbuldf = istanbuldf.loc[login]
		try:
			birthdate = datetime.strptime(istanbuldf.loc["Birth Date"], "%Y-%m-%d")
			birthdate = birthdate.strftime('%d.%m.%Y')
		except:
			birthdate = "-"
		mail = istanbuldf.loc["Email"]
		return ([birthdate, mail])
	except:
		return 0

def getpart(response):
	global part1projects, part2projects, part3projects
	part = 1
	projectcount = 0
	part1count = 0
	part2count = 0
	for i in range(len(response['projects_users'])):
		validated = response['projects_users'][i]['validated?']
		projectname = response['projects_users'][i]['project']['name']
		if (validated == True):
			if (projectname in part1projects):
				part1count += 1
			elif (projectname in part2projects):
				part2count += 1
			if (part1count >= 8 and part2count < 15):
				part = 2
			if (part1count >= 8 and part2count >= 15):
				part = 3
	return str(part)

def getuserinfo(login):
	headers = {
	'Authorization': 'Bearer ' + token,
	}
	endpoint = "/v2/users/{}".format(login)
	response = requests.get('https:##api.intra.42.fr' + "{}".format(endpoint), headers=headers).json()
	login = response['login']
	fullname = response['usual_full_name']
	part = getpart(response)
	primarykey = response['id']
	privateinfo = getprivateinfo(login)
	try:
		blackhole = day(response['cursus_users'][1]['blackholed_at'])
	except:
		return 0
	if privateinfo != 0 and blackhole != 0:
		mail = privateinfo[1]
		birthdate = privateinfo[0]
		insert(primarykey, login, fullname, part, blackhole, "lastseen", mail, birthdate)

def goupdate(token):
	page = 1
	x = 0
	headers = {
	'Authorization': 'Bearer ' + token,
		}
	params = {
	"page[size]": 100, 
	"sort":"login", 
	"filter[primary_campus_id]": 49,
	}
	while True:
		try:
			endpoint = f"/v2/cursus/21/users?page[number]={page}"
			response = requests.get('https:##api.intra.42.fr' + "{}".format(endpoint), headers=headers, params=params)
			if (response.status_code == 200):
				responsejs = response.json()
				if (len(responsejs) > 1):
					for i in range(len(responsejs)):
						user = responsejs[i]['login']
						getuserinfo(user)
				else:
					break
		except:
			print("İstek sınırı doldu veya Intra cevap vermiyor.")
			return
		page += 1



def get_access_token():
  response = requests.post(
    "https:##api.intra.42.fr/oauth/token",
    data={"grant_type": "client_credentials"},
    auth=("u-s4t2ud-05b797961e39f9ca81738308f9b2a7e2ed752549806393581cf56fc0685062bb", "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"),
  )
  return response.json()["access_token"]

createtable("users")
token = get_access_token()
goupdate(token)
