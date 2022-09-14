#!/usr/bin/env python3

# query data for NC

import pandas as pd
import numpy as np
import requests
import urllib.parse
import bs4
import re, os, hashlib, datetime, json, uuid, time, random
import pangres
from json_memoize import memoize
from loguru import logger
from sqlalchemy import create_engine
import backoff

from functools import wraps
import threading
from tqdm import tqdm

import warnings
warnings.filterwarnings(message="The default dtype for empty Series", action='ignore')

#import pymysql

chunk_size = 35000
max_thread_count = 1
max_sql_count = 1 # must be 1
random_state = 230230
sample_count = None # set to "None" to do all rows (just shuffle instead of sample)

q = urllib.parse.quote_plus

# requirements: multiprocesspandas
#from multiprocesspandas import applyparallel # unused but required
#from pandas_parallel_apply import DataFrameParallel, apply_on_df_parallel # pip install pandas-parellel-apply, pathos


# search page: https://services.wakegov.com/realestate/Search.asp

cache_dir = "cache_dir/"
if not os.path.isdir(cache_dir):
	os.mkdir(cache_dir)

@backoff.on_exception(backoff.expo, Exception, max_time=60, max_tries=10)
def call_api(url: str) -> str:
	# get result, store in cache
	req = requests.get(url)
	req.raise_for_status()

	hash_fname = hashlib.md5(url.encode('utf-8')).hexdigest()[:30] + '.txt'
	path = os.path.join(cache_dir, hash_fname)
	if os.path.exists(path) and os.path.getsize(path) > 100:
		#logger.info("cache hit")
		with open(path, 'r') as fp:
			return fp.read()
	else:
		#logger.info("cache miss")
		result = req.text
		#with open(path, 'w') as fp:
		#	fp.write(result)
		# disable because disk is too full, and it's sorta big
		
		return req.text

def handle_call(func, *args):
	try:
		return func(*args)
	except Exception as ex:
		#logger.warning(f"Error with call args: {args} | Error: {ex}")
		return {}


def move_cols_to_end(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
	for col in col_list:
		df.insert(len(df.columns)-1, col, df.pop(col))
	return df

def ree(pattern: str, string: str, group=1) -> str:
	try:
		return re.search(pattern, string).group(group)
	except:
		return None

class Memoize_Disk: 
	def __init__(self, gx, cache_dir="./disk_memo_cache/"):
		self.gx = gx # function
		self.cache_dir = cache_dir
		
		if not os.path.isdir(cache_dir):
			os.mkdir(cache_dir)

		self.gx_name = gx.__name__ # original function name

	def __call__(self, *args):
		# generate lookup hash
		hash_file = hashlib.md5(json.dumps({'__func_name': self.gx_name, 'args': args}, sort_keys=True).encode('utf-8')).hexdigest()[:25] + '_memo.json'
		fpath = os.path.join(self.cache_dir, hash_file)

		if os.path.exists(fpath) and os.path.getsize(fpath) > 10:
			#logger.debug('disk memo cache hit')
			with open(fpath) as fp:
				return json.load(fp)
		
		else:
			#logger.debug('disk memo cache miss')
			result = self.gx(*args)

			with open(fpath, 'w') as fp:
				json.dump(result, fp)

			return result


thread_share = {'num_threads': 0}
#thread_share_lock = threading.Lock()
thread_share_sem = threading.Semaphore(max_thread_count)
sql_share_sem = threading.Semaphore(max_sql_count)
out_df_list = []

# source: https://stackoverflow.com/a/45527807
def thread_limit(number):
	""" This decorator limits the number of simultaneous Threads """
	sem = threading.Semaphore(number)
	def wrapper(func):
		@wraps(func)
		def wrapped(*args):
			with sem:
				return func(*args)
		return wrapped
	return wrapper

def thread_async(f):
	""" This decorator executes a function in a Thread """
	@wraps(f)
	def wrapper(*args, **kwargs):
		thr = threading.Thread(target=f, args=args, kwargs=kwargs)
		thr.start()
	return wrapper

#@memoize(max_age=3600*24*7*2, cache_folder_path='./memo_cache_script')
@Memoize_Disk
def do_search_for_street(street_short: str, number: str) -> dict:
	html = call_api(f'https://services.wakegov.com/realestate/ValidateAddress.asp?stnum={q(number)}&stname={q(street_short)}&locidList=&spg=')
	out = {'acctlist_status': 'normal'}

	if 'Please refine your search by selecting one or more' in html:
		out['acctlist_status'] = 'error: wants a refined search'
		return out

	df1 = pd.read_html(html)
	df1 = df1[4]
	# promote headers
	df1.columns = df1.iloc[0]
	df1 = df1[1:-1] # remove header and hidden silly footer row

	df1 = df1.rename(columns=lambda x: 'acctlist_' + x)

	out['acctlist_row_count'] = len(df1)

	if len(df1) == 0:
		out['acctlist_status'] = 'error: no account results'
		return out
	
	if len(df1) > 1:
		out['acctlist_status'] = 'warning: >1 account result; using first one'

	out.update(df1.iloc[0].to_dict())

	return out

#@memoize(max_age=3600*24*7*2, cache_folder_path='./memo_cache_script')
@Memoize_Disk
def do_lookup_accountnum(account_num: str) -> dict:
	#logger.info(f"Doing Account: '{account_num}'")
	out = {'acctsearch_status': 'normal'}

	if pd.isna(account_num):
		out['acctsearch_status'] = 'error: no account number'
		return out

	html = call_api(f'https://services.wakegov.com/realestate/Account.asp?id={account_num}')
	html = re.sub(r'(&nbsp;?)+', ' ', html)
	html = re.sub(r'(\s)+', ' ', html)


	html_txt = re.sub('<[^<]+?>', ' ', html)
	html_txt = re.sub(r'(\s)+', ' ', html_txt)
	
	#logger.info(html_txt)
	try:
		out['acctsearch_Real Estate ID'] = re.search(r"Real Estate ID (\d{5,15})", html_txt).group(1)
	except:
		logger.info(f"Unknown Real Estate ID on Account '{account_num}'")
	out['acctsearch_PIN'] = re.search(r"PIN # (\d{5,15})", html_txt).group(1)
	out['acctsearch_Property Description'] = re.search(r"Property Description (.+) Pin/Parcel History New Search", html_txt).group(1).strip()
	out['acctsearch_Property Owner'] = re.search(r"Property Owner (.+) \(Use the Deeds link to", html_txt).group(1).strip()
	out['acctsearch_Property Location Address'] = re.search(r"Property Location Address (.+) Administrative Data Old Map", html_txt).group(1).strip()

	soup = bs4.BeautifulSoup(html, features='lxml')
	for tr in soup.find_all('tr'):
		td_childs = tr.find_all('td')
		if len(td_childs) == 2:
			field_name = td_childs[0].text.strip()
			if len(field_name) < 3:
				continue
			if field_name[:10] == out['acctsearch_Property Description'][:10]:
				continue
			if field_name.startswith('Real Estate ID'):
				continue
			out['acctsearchf_' + field_name] = td_childs[1].text.strip() # acctsearchf, f stands for field
	
	return out

@thread_limit(1000000)
@thread_async
#from pebble import concurrent
#@concurrent.thread
def do_some(df: pd.DataFrame, chunk_num):
	this_uuid = str(uuid.uuid4())[:10]
	#logger.debug(f'Init do_some thread (uuid {this_uuid}, chunk {chunk_num}).')

	# while 1:
	# 	#thread_share_lock.acquire()
	# 	if thread_share['num_threads'] < max_thread_count:
	# 		logger.debug(f"Init WORK IN do_some thread (uuid {this_uuid}, num_threads={thread_share['num_threads']}, chunk {chunk_num}).")
	# 		thread_share['num_threads'] += 1
	# 		#thread_share_lock.release()
		
	# 		break
	# 	else:
	# 		time.sleep(0.25+random.random()*20)
	thread_share_sem.acquire()
	logger.debug(f"Init WORK IN do_some thread (uuid {this_uuid}, chunk {chunk_num}).")
	

	# call: do_search_for_street()
	df['acctlist'] = df.apply(lambda r: handle_call(do_search_for_street, str(r.street_short), str(r.number)), axis=1)
	#df = pd.concat([df, pd.json_normalize(df['acctlist'])], axis=1)
	df = pd.concat([df, df['acctlist'].apply(pd.Series)], axis=1)

	# filter out null Accounts (which are caused by a search page being loaded, generally)
	orig_len = len(df)
	df = df[pd.notna(df['acctlist_Account'])]

	logger.debug(f"{len(df)}/{orig_len} addresses retrieved an Account value.")


	### Start Part 2
	# call: do_lookup_accountnum()
	df['acctsearch'] = df.apply(lambda r: do_lookup_accountnum(str(r['acctlist_Account'])), axis=1)
	#df = pd.concat([df, pd.json_normalize(df['acctsearch'])], axis=1)
	df = pd.concat([df, df['acctsearch'].apply(pd.Series)], axis=1)


	#########################################################
	# huge cleanup

	
	df['state'] = 'NC' # this must be the first column added
	df['property_zip5'] = df['acctsearch_Property Location Address'].apply(lambda v: ree(r"(\d{5})-\d+", v))
	df['property_street_address'] = df.apply(lambda r: f"{r['acctlist_St Num']} {r['acctlist_Street Name']} {r['acctlist_Type']}", axis=1)
	df['property_county'] = 'WAKE' # it's all the same one
	df['property_id'] = df.apply(lambda r: f"{r['acctsearch_PIN']}-{r['acctlist_Account']}", axis=1) # PIN and Account Number (TODO zero-pad Account maybe)
	df['building_year_built'] = pd.to_datetime(df['acctsearchf_Permit Date'], errors='coerce').dt.year#.astype(str).str.replace(r'(\.0)|(nan)','', regex=True, case=False)
	df['source_url'] = df['acctlist_Account'].apply(lambda v: f'https://services.wakegov.com/realestate/Account.asp?id={v}')
	df['book'] = df['acctsearchf_Book & Page'].apply(lambda v: v.split(' ')[0] if v.split(' ')[0].isnumeric() else None)
	df['page'] = df['acctsearchf_Book & Page'].apply(lambda v: ree(r"\s(\d+)", v))
	df['deed_date'] = pd.to_datetime(df['acctsearchf_Deed Date'], errors='coerce')

	df['sale_price'] = df.apply(lambda r: r['acctsearchf_Pkg Sale Price'] if pd.notna(r['acctsearchf_Pkg Sale Price']) else r['acctsearchf_Land Sale Price'], axis=1)
	df['sale_datetime'] = pd.to_datetime(df.apply(lambda r: r['acctsearchf_Pkg Sale Date'] if pd.notna(r['acctsearchf_Pkg Sale Price']) else r['acctsearchf_Land Sale Date'], axis=1), errors='coerce')

	df['sale_datetime'] = df.apply(lambda r: r['deed_date'] if pd.isna(r['sale_datetime']) else r['sale_datetime'], axis=1)

	df['building_year_built'] = df['building_year_built'].apply(lambda v: v if 1500 < v <= 2022 else None) # clean to constraint
	df['property_zip5'] = df['property_zip5'].apply(lambda v: v if v != '00000' else None) # clean to constraint

	# remove null sale_datetime rows as it's mandatory
	len_before = len(df)
	df = df[pd.notna(df['sale_datetime'])]

	logger.info(f"Kept {len(df)}/{len_before} rows by final filters (like sale_datetime).")

	# TODO building stories/beds from the buildings tab

	rename_list = {
		"acctsearchf_Land Class": "property_type",
		"acctsearch_Property Owner": "buyer_1_name",
		"acctsearchf_Township": "property_township",
		"latitude": "property_lat",
		"longitude": "property_lon",
		"acctsearchf_Heated Area": "building_area_sqft",
		"acctsearchf_Bldg. Value Assessed": "building_assessed_value",
		"acctsearchf_Land Value Assessed": "land_assessed_value",

		"acctsearchf_City": "property_city",
		"acctsearchf_Acreage": "land_area_acres",
	}
	df = df.rename(columns=rename_list)
	df = move_cols_to_end(df, rename_list.values())

	for col in ['sale_price', 'building_assessed_value', 'land_assessed_value', 'building_area_sqft', 'land_area_acres']:
		#df[col] = df[col].str.replace(r"[*,$]", '', regex=True).astype(float, errors='ignore')
		df[col] = df[col].str.replace(r"[*,$]", '', regex=True)
		df[col] = pd.to_numeric(df[col], errors='coerce')

	df['land_area_sqft'] = df['land_area_acres'].apply(lambda v: v*43560)
	#df['sale_price'] = df['sale_price'].astype(str).str.replace('^$', '0', regex=True).astype(float, errors='ignore').astype(int)
	df['sale_price'] = df['sale_price'].fillna(0)

	# delete cols to the left of 'state'
	new_cols = []
	hit_first_col_yet = False
	for col in df.columns:
		if col == 'state': hit_first_col_yet = True
		if hit_first_col_yet: new_cols.append(col)
	df = df[new_cols]

	df = df.set_index(['state', 'property_street_address', 'sale_datetime']) # set primary key, per online database

	##############
	# write out to SQL database
	#breakpoint()
	if 0: # disable SQL for now

		# make engine
		time1 = time.time()
		sql_share_sem.acquire()
		time2 = time.time()
		sqlEngine = create_engine("mysql+pymysql://root:@127.0.0.1/us_housing_prices_v2", pool_recycle=3600)
		#pd.read_sql_query("SELECT * FROM sales LIMIT 10", con=con)

		logger.debug(f"Starting a SQL transfer for chunk {chunk_num}. Time waiting_for_lock={time2-time1:.2f}")

		while 1:
			try:
				#frame = dataFrame.to_sql(tableName, dbConnection, if_exists="fail")
				pangres.upsert(sqlEngine, df, "sales", if_row_exists='update')
				break
			except Exception as ex:
				err = str(ex)
				if len(err) > 1000:
					err = err[:500] + '........' + err[-500:]
					
				logger.error(f"SQL upsert error!: {err}")

				if "Can't connect to MySQL server on" in str(ex) or "this connection is closed" in str(ex).lower():
					logger.error(f"Can't access SQL server. Retrying for a while.")
					time.sleep(10)
			finally:
				#con.close()
				sql_share_sem.release()

		time3 = time.time()
		logger.debug(f"Times to do SQL transfer: chunk_num={chunk_num}, waiting_for_lock={time2-time1:.2f}, transfer={time3-time2:.2f} (sec)")

	sql_share_sem.acquire()
	out_df_list.append(df)
	sql_share_sem.release()

	# write backup out to file
	df.to_csv('out_partial_script/out_' + datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S') + f"_chunk_{chunk_num:04}" + '.csv', index=True)

	logger.info(f'DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! do_some (uuid {this_uuid}, chunk {chunk_num}).')
	
	#thread_share_lock.acquire()
	#thread_share['num_threads'] -= 1
	#thread_share_lock.release()
	thread_share_sem.release()

	#logger.debug(f'do_some thread released (uuid {this_uuid}, chunk {chunk_num}).')

def start_scrape():
	logger.info(f"Starting start_scrape(). ")
	df = pd.read_csv('nc_wake_addrlist.csv')
	logger.info(f"Number of potential properties (based on addr list): {len(df):,}")
	#df = df.head(10) # DEBUG
	
	if sample_count:
		df = df.sample(sample_count, random_state=1230984) # shuffle
	else:
		df = df.sample(len(df), random_state=random_state) # shuffle

	df['street_type'] = df.street.apply(lambda x: x.strip().split(' ')[-1])
	df['street_short'] = df.street.apply(lambda x: ' '.join(x.strip().split(' ')[:-1]))

	num_of_chunks = round(len(df) / chunk_size)
	logger.info(f"Total number of chunks = {num_of_chunks}")
	for chunk_num, df_chunk in enumerate(tqdm(np.array_split(df, num_of_chunks), leave=False)): # n = number of chunks, not size of chunks
		do_some(df_chunk, chunk_num)
	
	logger.info("Done creating threads.")

	while 1:
		#time.sleep(60*5 + random.random()*120)
		time.sleep(10 + random.random()*600)
		sql_share_sem.acquire()
		if len(out_df_list) == num_of_chunks:
			logger.info(f"Done! Writing out the final concat.")
			# all done, merge and write out
			dfout = pd.concat(out_df_list)
			dfout.to_csv('out_partial_script/11_concat_out_' + datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S') + '.csv', index=True)
			logger.info(f"Done writing out all the partial dataframes to the concat (total {len(out_df_list)} frames).")
			break
		sql_share_sem.release()

	logger.info('Done scrape.')
			

if __name__ == '__main__':
	start_scrape()