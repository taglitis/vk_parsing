import requests
import os
import pandas as pd
import time
from IPython.display import clear_output
import statistics
import sys
import numpy as np
from datetime import datetime
from joblib import Parallel, delayed
from shutil import copyfile
import json

def line_prepender(filename, lines_list):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(lines_list)
        f.write(content)

def creat_base(i, extract_dict):
        
    try:
        extract_dict['country']
    except:
        return [np.nan]*14
    if extract_dict['country']['id'] == 4:
        list_users_id = str(int(extract_dict['id']))
        try:
            list_users_fn = extract_dict['first_name']
        except:
            list_users_fn = np.nan                
        try:
            list_users_ln = extract_dict['last_name']
        except:
            list_users_ln = np.nan                
        try:
            list_users_country = extract_dict['country']['title']
        except:
            list_users_country = np.nan
        try:
            list_users_sex = str(extract_dict['sex'])
        except:
            list_users_sex = np.nan
        try:
            list_users_photo = str(extract_dict['photo_max_orig'])
        except:
            list_users_photo=np.nan
        try:
            list_users_city = extract_dict['city']['title']
        except:
            list_users_city = np.nan
        try:
            list_users_bdate = extract_dict[i]['bdate']
        except:
            list_users_bdate = np.nan
        try:
            list_users_lseen = str(extract_dict['last_seen']['time'])
            real_time = datetime.utcfromtimestamp(int(extract_dict['last_seen']['time'])).strftime('%Y-%m-%d')
            list_users_lseen_real = str(real_time)
        except:
            list_users_lseen = np.nan
            list_users_lseen_real = np.nan
        try:
            list_users_univercity= str(extract_dict['univercity'])
        except:
            list_users_univercity = np.nan
        try:
            list_users_faculty = str(extract_dict['faculty'])
        except:
            list_users_faculty = np.nan
        try:
            list_users_graduation = str(extract_dict['graduation'])
        except:
            list_users_graduation = np.nan
        try:
            list_users_mobile = str(extract_dict['mobile_phone'])
        except:
            list_users_mobile = np.nan
        return [list_users_id, list_users_fn, list_users_ln,  list_users_bdate, list_users_city, list_users_country, 
               list_users_sex, list_users_mobile, list_users_photo,
               list_users_lseen, list_users_univercity,
               list_users_faculty, list_users_graduation, list_users_lseen_real]
    else:
        return [np.nan]*14  


def user_to_friend(df_friend_data, user_id):
    df_user_to_friend = pd.DataFrame(data = {'user_id':[user_id]*df_friend_data.shape[0],
                                            'friend_id':df_friend_data.user_id.tolist()})
    return df_user_to_friend

def clean_df(df_friend_data, user_id):
    df_friend_data=df_friend_data[~df_friend_data.user_id.isna()]       
    # f.write(f'time of friends extraction for user {user_id} is {time.time()- time_friend_data_extract} sec'+'\n')
        
    # remove rows where entries have ";" 
    if df_friend_data.shape[0] != 0:
        df_friend_data = df_friend_data.astype(str)
        df_friend_data = df_friend_data[~df_friend_data.applymap(lambda x: len(x.split(';'))>1).max(axis = 1)]

    df_user_to_friend = user_to_friend(df_friend_data, user_id)
    
    df_user_to_friend = df_user_to_friend.astype(str)
    #df_user_to_friend = df_user_to_friend[~df_user_to_friend.applymap(lambda x: len(x.split(';'))>1).max(axis = 1)]
    # df_user_to_group = df_user_to_group[~df_user_to_group.applymap(lambda x: len(x.split(';'))>1).max(axis = 1)]

    return df_friend_data, df_user_to_friend

    

def save_df(df_friend_data, df_user_to_friend, df_user_to_group, path_fr_gr_out, file_friend_data, file_user_to_friend,   
    file_friend_to_group, ext, user_id):
    df_friend_data[friend_column_list].to_csv(path_fr_gr_out+file_friend_data+'_'+str(i+1)+'_' + user_id+ext, sep=';', 
                                                encoding='utf-8')
    df_user_to_friend[user_to_friend_list].to_csv(path_fr_gr_out+file_user_to_friend+'_'+str(i+1) + '_' + user_id+ext, 
                                                    sep=';', encoding='utf-8')
    df_user_to_group[user_to_group_list].to_csv(path_fr_gr_out+file_friend_to_group+'_'+str(i+1) + '_' + user_id+ext, 
                                                    sep=';', encoding='utf-8')


def json_to_df(res_friends, friend_column_list):
    ####### CPU #######                    
    extracted = []   
    for i_extract, extract_dict in enumerate(res_friends['response']['items']): 
        extracted.append(creat_base(i_extract, extract_dict))
    ####### GPU #######                    
#    extracted = Parallel(n_jobs=6)(delayed(creat_base)(i_offset, extract_dict) \
#                                                      for i_offset, extract_dict \
#                                                      in enumerate(res_friends.json()['response']['items']))
    return pd.DataFrame(columns = friend_column_list, data = extracted)
def update_statistics(type_p, path_stat, stat, i, user_id, n_members,  user_left, iter_time, time_stamp, column_statistics):
        ####### update statistics##################
  

    statistics_df = pd.read_csv(path_stat + stat, encoding = 'utf-8', sep = ';', index_col = 0, dtype = {'user_id':str})
    print('number of rows in STATISTICS file:', statistics_df.shape[0])
#    print('last element: ', statistics_df.user_id[:-1])
    # aaa = input('Enter value 2 ')
    stat_list = [type_p, i, user_id, n_members, user_left, iter_time, time_stamp]
    statistics_df_temp = pd.DataFrame(columns = column_statistics, data = [stat_list])
    statistics_df = statistics_df.append(statistics_df_temp).reset_index(drop=True)
    statistics_df.to_csv(path_stat + stat, encoding = 'utf-8', sep = ';')
    # if (i+1) % 1000 == 0:
    #     time_now = datetime.now().strftime(format = '%Y_%m_%d_%H_%m')
    #     path_fr_gr_out = './output_friends_and_group/'
    #     path_stat = path_fr_gr_out + 'statistics/'
    #     copyfile(path_stat+'statistics.csv', './output_friends_and_group/archive/statistics_'+time_now+'.csv')  
    return 0    

path_in = 'output_unique_users/'
path_fr_gr_out = './output_friends_and_group_json/'
path_stat = path_fr_gr_out + 'statistics/'

ext = '.csv'
file_user_to_friend = 'user_to_friend/user_to_friend' 
file_friend_data = 'friend_data/friend_data'
file_group_data = 'group_data/group_data'
file_friend_to_group = 'user_to_group/user_to_group'
dir_user_to_group_json = 'user_to_group_json/'
file_user_to_group_json = 'user_to_group_json/friend_data'
dir_friend_data_json = 'friend_data_json/'
file_friend_data_json = 'friend_data_json/friend_data'
file_name_in = 'unique_users.csv'
stat = 'statistics.csv'
file_log = 'log.txt' 

#### REMOVE FILES FOR DIRECTORIES DURING DEBUGGING ####
# list_dirs = [path_fr_gr_out+file_user_to_friend.split('/')[0]+'/', 
#                 path_fr_gr_out+file_friend_data.split('/')[0]+'/',
#                 path_fr_gr_out+file_group_data.split('/')[0]+'/',
#                 path_fr_gr_out+file_friend_to_group.split('/')[0]+'/',
#                 path_stat
#             ]
# for list_dir in list_dirs:
#     list_file_dir = os.listdir(list_dir)
#     if 'desktop.ini' in list_file_dir: list_file_dir.remove('desktop.ini')
#     for file_remove in list_file_dir:
#         os.remove(list_dir+file_remove)
##########################################################   

friend_column_list = ['user_id', 'first_name', 'last_name', 'bdate', 'city', 'country',  
                      'sex', 'mobile_phone', 'photo', 'last_seen', 'univercities',
                      'faculty', 'graduation', 'last_seen_real']

user_to_friend_list = ['user_id','friend_id']
user_to_group_list = ['user_id', 'group_id']
group_data_column_list = ['group_id', 'name', 'screen_name', 'description']
column_statistics = ['type', 'user_number', 'user_id', 'n_friends', 'left_friends', 
                     'iter_time', 'time_stamp']  



if (stat in os.listdir(path_stat)) == False:
    users_in_files = 0
    time_program_start = time.time()
    time_program_start_fmt = datetime.now().strftime("%Y-%m-%d @ %H:%M:%S")
    # get a llist of initial user_ids
    user_list_df = pd.read_csv(path_in+file_name_in, encoding = 'utf-8', sep = ';', 
                            index_col = 0, dtype = {'user_id':str})
    user_list = user_list_df.user_id.values.tolist()
    statistics_df = pd.DataFrame(columns = column_statistics)
    statistics_df.to_csv(path_stat + stat, encoding = 'utf-8', sep = ';')
    user_used_list = []

else:
    #obtain the remaining user_is to collect data 
    # get a llist of initial user_ids
    statistics_df = pd.read_csv(path_stat + stat, encoding = 'utf-8', sep = ';', 
                            index_col = 0, dtype = {'user_id':str})
    user_used_list = statistics_df.user_id.values.tolist()
    user_list_df = pd.read_csv(path_in+file_name_in, encoding = 'utf-8', sep = ';', 
                            index_col = 0, dtype = {'user_id':str})
    user_list = user_list_df.user_id.values.tolist()
    user_list = list(set(user_list) - set(user_used_list))
    
total_number_users = len(user_list_df)
n_users_completed = len(user_used_list) 
##############FRIENDS_DATA#########################
# list_file = os.listdir(path_fr_gr_out+'friend_data_json/') 
# for file in list_file:
#     start_iter = time.time()
#     user_id = file.split('_')[3]
#     n_user_id = file.split('_')[2]
#     with open(path_fr_gr_out+dir_friend_data_json+file) as f_json:  
#         data_json = json.load(f_json)
#     df_friend_data = json_to_df(data_json, friend_column_list)  
#     n_members = df_friend_data.shape[0]
#     df_friend_data, df_user_to_friend = clean_df(df_friend_data, user_id)
#     df_friend_data[friend_column_list].to_csv(path_fr_gr_out+file_friend_data+'_'+n_user_id+'_' + user_id+ext, sep=';', 
#                                                 encoding='utf-8')
#     df_user_to_friend[user_to_friend_list].to_csv(path_fr_gr_out+file_user_to_friend+'_'+n_user_id + '_' + user_id+ext, 
#                                                     sep=';', encoding='utf-8')
#     iter_time = time.time() - start_iter
#     update_statistics('friend', path_stat, stat, n_user_id, user_id, n_members,  df_friend_data.shape[0], iter_time, datetime.now().strftime("%Y-%m-%d @ %H:%M:%S"), column_statistics)
#     break

############USER_TO_GROUP##############################
list_file = os.listdir(path_fr_gr_out+'user_to_group_json/')
for file in list_file:
    user_id = file.split('_')[3]
    n_user_id = file.split('_')[2]
    start_iter = time.time()
    user_id = file.split('_')[3]
    n_user_id = file.split('_')[2]
    with open(path_fr_gr_out+dir_user_to_group_json+file) as f_json:  
        data_json = json.load(f_json) 
    # print(data_json)
    n_groups = len(data_json['items'])
    group_list = data_json['items']
    df_user_to_group = pd.DataFrame(data = {'user_id':[user_id]*n_groups,'group_id':group_list}, columns=user_to_group_list)
    df_user_to_group = df_user_to_group.astype(str)
    display(df_user_to_group.head(2))
    df_user_to_group.to_csv(path_fr_gr_out+file_friend_to_group +'_'+n_user_id + '_' + user_id+ext, 
                                                     sep=';', encoding='utf-8')
    iter_time = time.time() - start_iter                                                 
    update_statistics('group', path_stat, stat, n_user_id, user_id, n_groups,  np.nan, iter_time, 
        datetime.now().strftime ("%Y-%m-%d @ %H:%M:%S"), column_statistics)                                                 
    break     