import requests
import os
import pandas as pd
import time
from IPython.display import clear_output
from IPython.display import display
import statistics
from statistics import mean 
import sys
import numpy as np
from datetime import datetime
from joblib import Parallel, delayed
from shutil import copyfile
import json
import sys
import pickle
import math
import multiprocessing as mp


def remove_sep_from_string(a, sep = ';'):
    a = a.split(sep)
    a = ''.join(a)
    return a

def json_to_df(extract_dict):
    # print(extract_dict)
    try:
        extract_dict['country']
    except:
        return [np.nan]*14
    if extract_dict['country']['id'] == 4:
        list_users_id = str(int(extract_dict['id']))
        try:
            list_users_fn = extract_dict['first_name']
            list_users_fn = remove_sep_from_string(list_users_fn)
        except:
            list_users_fn = np.nan                
        try:
            list_users_ln = extract_dict['last_name']
            list_users_ln = remove_sep_from_string(list_users_ln)
        except:
            list_users_ln = np.nan                
        try:
            list_users_country = extract_dict['country']['title']
            list_users_country = remove_sep_from_string(list_users_country)
        except:
            list_users_country = np.nan
        try:
            list_users_sex = str(extract_dict['sex'])
            list_users_sex = remove_sep_from_string(list_users_sex)
        except:
            list_users_sex = np.nan
        try:
            list_users_photo = str(extract_dict['photo_max_orig'])
            list_users_photo = remove_sep_from_string(list_users_photo)
        except:
            list_users_photo=np.nan
        try:
            list_users_city = extract_dict['city']['title']
            list_users_city = remove_sep_from_string(list_users_city)
        except:
            list_users_city = np.nan
        try:
            list_users_bdate = extract_dict[i]['bdate']
            list_users_bdate = remove_sep_from_string(list_users_bdate)
        except:
            list_users_bdate = np.nan
        try:
            list_users_lseen = str(extract_dict['last_seen']['time'])
            real_time = datetime.utcfromtimestamp(int(extract_dict['last_seen']['time'])).strftime('%Y-%m-%d')
            list_users_lseen_real = str(real_time)
            list_users_lseen = remove_sep_from_string(list_users_lseen)
            list_users_lseen_real = remove_sep_from_string(list_users_lseen_real)
        except:
            list_users_lseen = np.nan
            list_users_lseen_real = np.nan
        try:
            list_users_univercity= str(extract_dict['univercity'])
            list_users_univercity = remove_sep_from_string(list_users_univercity)
        except:
            list_users_univercity = np.nan
        try:
            list_users_faculty = str(extract_dict['faculty'])
            list_users_faculty = remove_sep_from_string(list_users_faculty)
        except:
            list_users_faculty = np.nan
        try:
            list_users_graduation = str(extract_dict['graduation'])
            list_users_graduation = remove_sep_from_string(list_users_graduation)
        except:
            list_users_graduation = np.nan
        try:
            list_users_mobile = str(extract_dict['mobile_phone'])
            list_users_mobile = remove_sep_from_string(list_users_mobile)
        except:
            list_users_mobile = np.nan
        return [list_users_id, list_users_fn, list_users_ln,  list_users_bdate, list_users_city, list_users_country, 
               list_users_sex, list_users_mobile, list_users_photo,
               list_users_lseen, list_users_univercity,
               list_users_faculty, list_users_graduation, list_users_lseen_real]
    else:
        return [np.nan]*14 

def parse_vk(i, user_id, token, kwargs):
    time_limit = 3.0001
    step_offset = 2000
    ext = kwargs['ext']
    file_friend_data_df = kwargs['file_friend_data_df']
    api_version = '5.89'
    friend_column_list = ['friend_id', 'first_name', 'last_name', 'bdate', 'city', 'country',  
                    'sex', 'mobile_phone', 'photo', 'last_seen', 'univercities',
                    'faculty', 'graduation', 'last_seen_real']
    fields_param = 'country,sex,bdate,city,country'\
                    ',photo_max_orig, online_mobile,has_mobile,contacts'\
                    ',connections,site,education,universities,schools,'\
                    ',can_write_private_message,status,last_seen,relation,relatives'    
    url_friend = f'https://api.vk.com/method/friends.get?user_id={str(user_id)}'
    url_friend+= f'&offset=0&count={step_offset}&access_token={token[0]}&v={api_version}&fields={fields_param}'
    ts = time.time()
    try:
        req = requests.get(url_friend, timeout=None)
        req = req.json()
    except requests.exceptions.RequestException as e:
        print(f'request {e}, type: {type(e)}')
        req = {'response': {'count': 0,  'items': []}}
    try:  
        n_members = req['response']['count']
    except KeyError as e:
        req = {'response': {'count': 0,  'items': []}}
        n_members = 0
    iter_number = n_members // step_offset
    iter_number_count = 1
    duration = time.time()-ts
    if n_members >= step_offset:
        if  duration <= time_limit: time.sleep(time_limit - duration)
        for i_offset, offset in enumerate(range(step_offset, n_members+1, step_offset)):     
            url_friend = f'https://api.vk.com/method/friends.get?user_id={str(user_id)}&offset={offset}'
            url_friend += f'&count={step_offset}&access_token={token[0]}&v={api_version}&fields={fields_param}'
            ts = time.time()
            try:
                req_temp = requests.get(url_friend, timeout = None)
                req_temp = req_temp.json()
            except requests.exceptions.RequestException as e:
                print(f'request {e}, type: {type(e)}')
                req_temp = {'response': {'count': n_members,  'items': []}}
            req['response']['items'] = req['response']['items'] + req_temp['response']['items']
            duration = time.time()-ts
            if iter_number_count < iter_number:   
                if  duration <= time_limit: time.sleep(time_limit - duration)
            iter_number_count += 1    
    extracted = [] 
    for i_extract, extract_dict in enumerate(req['response']['items']): 
        extracted.append(json_to_df(extract_dict))
    friends_data = pd.DataFrame(columns = friend_column_list, data = extracted)
    friends_data['user_id'] = user_id
    friends_data = friends_data[['user_id', 'friend_id', 'first_name', 'last_name', 'bdate', 'city', 'country',  
                                 'sex', 'mobile_phone', 'photo', 'last_seen', 'univercities',
                                 'faculty', 'graduation', 'last_seen_real']]
    url_group = f'https://api.vk.com/method/users.getSubscriptions?user_id={str(user_id)}&access_token={token[1]}&v={api_version}'
    try: 
        req = requests.get(url_group, timeout=None).json()
    except requests.exceptions.RequestException as e:
        print(f'exception {e} groups occured')
    try:
        req = req['response']['users']['items']
    except KeyError as e:
        pass
        # print('Group request KeyError: ', e) 
    req = [str(x) for x in req]
    friends_data.reset_index(drop = True, inplace = True)
    friends_data['groups_id'] = np.nan
    req = "|".join(req)
    friends_data.loc[friends_data.index == 0, 'groups_id'] = req
    friends_data.to_csv(file_friend_data_df + '_' + user_id + ext, encoding = 'utf-8', sep = ';' ) 
    return (i, user_id, n_members)


def get_result(result):
    global results
    results.append(result)

def check_consist(batch_users):
    file_list = os.listdir(path_out + 'friend_data_df')
    count_files = 0
    for file_name in file_list:
        if file_name.split('.')[-1]  == 'csv':
            count_files += 1
    if batch_users == count_files:
        print('Consistancy check is succesful')
        return 0
    else:
        print('Consistancy check is failed')
        sys.exit(0)

def update_statistics(**kwargs):
    ####### update statistics##################
    statistics_df = pd.read_csv(path_stat + file_stat, encoding = 'utf-8', sep = ';', index_col = 0, dtype = {'user_id':str})
    stat_list = []                     
    for i in range(0, len(results)):
        stat_list_temp = [kwargs['batch_number'], results[i][1], results[i][2], kwargs['batch_start'], kwargs['batch_end'], 
        kwargs['batch_duration'], kwargs['average_batch_duration'], kwargs['batch_start_fmt'], kwargs['batch_end_fmt']]
        stat_list.append(stat_list_temp)
    df_stat = pd.read_csv(path_stat + file_stat, sep = ';', encoding = 'utf-8').drop(columns = ['Unnamed: 0'])  
    df_stat_temp = pd.DataFrame(columns = column_statistics, data = stat_list)
    df_stat_temp.loc[1:,['batch_start_time', 'batch_end_time', 'batch_duratin', 'average_batch_duration', 
                          'batch_start_fmt', 'batch_end_fmt']]  = np.nan    
    df_stat = df_stat.append(df_stat_temp)
    df_stat.to_csv(path_stat + file_stat, sep = ';', encoding = 'utf-8')
    return 0

def read_stat_data():
    if (file_stat in os.listdir(path_stat)) == False:
        # get a list of initial user_ids
        user_list_df = pd.read_csv(path_in+file_name_in, encoding = 'utf-8', sep = ';', 
                                index_col = 0, dtype = {'user_id':str})
        # Note to the above uesr_list_df: index_col gives warning  FutureWarning: elementwise comparison failed; returning scalar instead, but in the future will perform elementwise comparison mask |= (ar1 == a)                         
        user_list = user_list_df.user_id.values.tolist()
        statistics_df = pd.DataFrame(columns = column_statistics)
        statistics_df.to_csv(path_stat + file_stat, encoding = 'utf-8', sep = ';')
        batch_start_count = 0
        user_used_list = []
    else:
        #obtain the remaining user_is to collect data 
        # get a list of initial user_ids
        statistics_df = pd.read_csv(path_stat + file_stat, encoding = 'utf-8', sep = ';',                                                
                                    index_col = 0, dtype = {'user_id':str})
        user_used_list = statistics_df.user_id.values.tolist()
        user_list_df = pd.read_csv(path_in+file_name_in, encoding = 'utf-8', sep = ';',                                                   
                                   index_col = 0, dtype = {'user_id':str})
        user_list = user_list_df.user_id.values.tolist()

        user_list = list(set(user_list) - set(user_used_list))
        batch_start_count = max(statistics_df.batch_number.values.tolist()) + 1
        file_list = os.listdir(path_out + 'friend_data_df')
        user_id_in_file = []
        for file_name in file_list:
            if file_name.split('.')[-1]  == 'csv':
                user_id_in_file.append(file_name.split('.')[0].split('_')[-1])
        print(sum(~statistics_df.user_id.isin(user_id_in_file)))        
    total_number_users = len(user_list_df)
    n_users_completed = len(user_used_list)
    return user_list, total_number_users, n_users_completed, batch_start_count    


# https://www.machinelearningplus.com/python/parallel-processing-python/
# asynch: https://towardsdatascience.com/asynchronous-parallel-programming-in-python-with-multiprocessing-a3fc882b4023
if __name__ == '__main__':
#### GLOBAL VAR #################
    global path_in, path_out, ext, file_user_to_group_df, file_friend_data_df, file_user_to_friend_df, path_stat, file_stat, file_log
    path_in = './data_in/'
    path_out = './data_out/'
    ext = '.csv'
    file_name_in = 'unique_users'+ext
    file_user_to_group_df = path_out + 'user_to_group_df/user_to_group_data_df'
    file_friend_data_df = path_out + 'friend_data_df/friend_data_df'
    file_user_to_friend_df = path_out +  'user_to_friend_df/user_to_friend_df'
    path_stat = path_out + 'stat/'
    file_stat = 'statistics_df.csv'
    file_log = 'log.txt' 
    kwargs = {'file_friend_data_df' : file_friend_data_df, 'ext' : ext} 
    ### REMOVE FILES FOR DIRECTORIES DURING DEBUGGING ####
    list_dirs = [path_out + dir_out for dir_out in os.listdir(path_out)]
    delete_files = False          ########################******************########################
    if delete_files:         
        for list_dir in list_dirs:
            list_file_dir = os.listdir(list_dir)
            if 'desktop.ini' in list_file_dir: list_file_dir.remove('desktop.ini')
            for file_remove in list_file_dir:
                os.remove(list_dir+'/'+file_remove)
    #########################################################          
    with open('./data_in/token_dict.txt', 'rb') as handle:
        token_dict = pickle.loads(handle.read()) 
    user_to_friend_list = ['user_id','friend_id']
    user_to_group_list = ['user_id', 'group_id']
    group_data_column_list = ['group_id', 'name', 'screen_name', 'description']
    column_statistics = ['batch_number', 'user_id', 'n_friends', 'batch_start_time', 
                         'batch_end_time', 'batch_duratin', 'average_batch_duration', 'batch_start_fmt', 'batch_end_fmt']
    #### GLOBAL VAR ENDED #################     
    user_list, total_number_users, n_users_completed, batch_start_count = read_stat_data()
    batch_size = 10
    ts = time.time()
    results = []
    batch_cnt = math.ceil(len(user_list) / batch_size)
    print('batch_cnt: ', batch_cnt)
    batch_duration_list = []
    print(f'Core # {mp.cpu_count()}')
    i_token = 1
    for i in range(batch_start_count, batch_cnt):
        # print('***i=', i)
        batch_start = time.time()
        batch_start_fmt = datetime.now().strftime("%Y-%m-%d @ %H:%M:%S")
        time_iter_begin = time.time()
        if batch_size*(i+1) <= len(user_list):
            i_start = batch_size * i
            i_end = batch_size * (i+1)
        else:
            i_start = batch_size * i
            i_end = len(user_list)
        pool = mp.Pool(mp.cpu_count())
        print(f'batch_cnt: {batch_cnt}     i_start: {i_start}     i_end: {i_end}')
        time_begin = time.time()
        for j in range(i_start, i_end):
            # print('j: ', j)
            pool.apply_async(parse_vk, args=(j, user_list[j], token_dict[i_token], kwargs), callback=get_result) 
        i_token += 1
        if i_token == len(token_dict) + 1: i_token = 1
        pool.close()
        pool.join()      

        print('Time in asynch threading:', time.time() - ts)    

        batch_end = time.time()
        batch_end_fmt = datetime.now().strftime("%Y-%m-%d @ %H:%M:%S")
        batch_duration = batch_end - batch_start
        batch_duration_list.append(batch_duration)
        # print('results: \n', results) 
        check_consist((i+1) * batch_size)
        update_statistics(batch_number = i, batch_start = batch_start, 
                        batch_end = batch_end, batch_duration = batch_duration, average_batch_duration = mean(batch_duration_list),
                        batch_start_fmt = batch_start_fmt, batch_end_fmt = batch_end_fmt)
        
        results = {x_tup[0]: x_tup for x_tup in results}
        results = {key: results[key] for key in sorted(results)}                  
        results = []
        print(f"{i}th loop ended!") 
        print('Elepsed time until completion', (batch_cnt - 1) * mean(batch_duration_list) / 60 /60, " hours")
        if i == batch_start_count + 1: break    
    df_stat = pd.read_csv(path_stat + file_stat, sep = ';', encoding = 'utf-8').drop(columns = ['Unnamed: 0'])
    display(df_stat)  