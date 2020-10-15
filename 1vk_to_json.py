import requests
import os
import pandas as pd
import time
from IPython.display import clear_output
from IPython.display import display
import statistics
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





# process_number = input('Enter the process number: ')
# process_number = int(process_number)
# # parrallel processing : https://www.machinelearningplus.com/python/parallel-processing-python/
# ###############################
# if process_number == 1:
#     start_for_number = "start_for_number 1"
#     path_stat = path_out + 'statistics_json_df_1/'
#     file_name_in = 'unique_users_1.csv'
# elif process_number == 2:
#     start_for_number = "start_for_number 2"
#     path_stat = path_out + 'statistics_json_df_2/'
#     file_name_in = 'unique_users_2.csv'
# elif process_number == 3:
#     start_for_number = "start_for_number 3"
#     path_stat = path_out + 'statistics_json_df_3/'
#     file_name_in = 'unique_users_3.csv'
# else:
#     print("Raw input error! Try again...") 
#     sys.exit()   

#########################################

#Define global vars


def read_stat_data():
    if (file_stat in os.listdir(path_stat)) == False:
        users_in_files = 0
        time_program_start = time.time()
        time_program_start_fmt = datetime.now().strftime("%Y-%m-%d @ %H:%M:%S")
        # get a list of initial user_ids
        user_list_df = pd.read_csv(path_in+file_name_in, encoding = 'utf-8', sep = ';', 
                                index_col = 0, dtype = {'user_id':str})
        # Note to the above uesr_list_df: index_col gives warning  FutureWarning: elementwise comparison failed; returning scalar instead, but in the future will perform elementwise comparison mask |= (ar1 == a)                         
        # display(user_list_df.head())                                
        index_start = user_list_df.index.values[0]
        user_list = user_list_df.user_id.values.tolist()
        statistics_df = pd.DataFrame(columns = column_statistics)
        statistics_df.to_csv(path_stat + file_stat, encoding = 'utf-8', sep = ';')
        user_used_list = []
    else:
        #obtain the remaining user_is to collect data 
        # get a list of initial user_ids
        statistics_df = pd.read_csv(path_stat + file_stat, encoding = 'utf-8', sep = ';',                                                
                                    index_col = 0, dtype = {'user_id':str})
        # display(statistics_df.head(2))                        
        user_used_list = statistics_df.user_id.values.tolist()
        user_list_df = pd.read_csv(path_in+file_name_in, encoding = 'utf-8', sep = ';',                                                   
                                   index_col = 0, dtype = {'user_id':str})
        index_start = user_list_df.index.values[0]                        
        user_list = user_list_df.user_id.values.tolist()
        user_list = [x.split('_')[0] for x in user_list]
        user_list = list(set(user_list) - set(user_used_list))
    total_number_users = len(user_list_df)
    n_users_completed = len(user_used_list)
    return user_list, total_number_users, n_users_completed



def update_statistics(i, user_id, n_members, column_statistics, start_time, start_fmt):
    ####### update statistics##################
    end_fmt = datetime.now().strftime("%Y-%m-%d @ %H:%M:%S")

    end_time = time.time()
    statistics_df = pd.read_csv(path_stat + stat, encoding = 'utf-8', sep = ';', index_col = 0, dtype = {'user_id':str})
    print('number of rows in STATISTICS file:', statistics_df.shape[0])
    diff = end_time - start_time 
    stat_list = [(i+1), user_id, n_members, start_time, end_time, start_fmt, end_fmt, diff]
    statistics_df_temp = pd.DataFrame(columns = column_statistics, data = [stat_list])
    statistics_df = statistics_df.append(statistics_df_temp).reset_index(drop=True)
    statistics_df.to_csv(path_stat + stat, encoding = 'utf-8', sep = ';')
    # if (i+1) % 1000 == 0:
    #     time_now = datetime.now().strftime(format = '%Y_%m_%d_%H_%m')
    #     path_out = './output_friends_and_group/'
    #     path_stat = path_out + 'statistics/'
    #     copyfile(path_stat+'statistics.csv', './output_friends_and_group/archive/statistics_'+time_now+'.csv')  
    return 0

def parse_vk(i, user_id, token, api_version):
    url_friend = f'https://api.vk.com/method/friends.get?user_id={str(user_id)}&access_token={token}&v={api_version}'
    try:
        req = requests.get(url_friend, timeout=None)
        req = req.json()['response']   
    except requests.exceptions.RequestException as e:
        print(f'request {e}, type: {type(e)}')
        req = {'count': 0, 'items': []}

    n_members = req['count']
    print('n_numbers: ', n_members)

    
    # print('req:', req)
    return (i, req)


def get_result(result):
    global results
    results.append(result)


# https://www.machinelearningplus.com/python/parallel-processing-python/
# asynch: https://towardsdatascience.com/asynchronous-parallel-programming-in-python-with-multiprocessing-a3fc882b4023
if __name__ == '__main__':
#### GLOBAL VAR #################
    api_version = '5.89'
    path_in = './data_in/'
    path_out = './data_out/'
    ext = '.csv'
    file_name_in = 'unique_users'+ext
    # file_user_to_friend = 'user_to_friend/user_to_friend' 
    # file_friend_data = 'friend_data/friend_data'
    # file_group_data = 'group_data/group_data'
    # file_friend_to_group = 'user_to_group/user_to_group'
    file_user_to_group_json = path_out + 'user_to_group_json/user_to_group_data_json'
    file_friend_data_json = path_out + 'friend_data_json/friend_data_json'
    file_user_to_friend = path_out +  'user_to_friend_json/user_to_friend_json'
    path_stat = path_out + 'stat/'
    file_stat = 'statistics_json.csv'
    file_log = 'log.txt' 
    ### REMOVE FILES FOR DIRECTORIES DURING DEBUGGING ####
    # list_dirs = [path_out+file_user_to_friend.split('/')[0]+'/', 
    #                 path_out+file_friend_data.split('/')[0]+'/',
    #                 path_out+file_group_data.split('/')[0]+'/',
    #                 path_out+file_friend_to_group.split('/')[0]+'/',
    #                 path_stat
    #             ]
    # for list_dir in list_dirs:
    #     list_file_dir = os.listdir(list_dir)
    #     if 'desktop.ini' in list_file_dir: list_file_dir.remove('desktop.ini')
    #     for file_remove in list_file_dir:
    #         os.remove(list_dir+file_remove)
    #########################################################  
        
    with open('./data_in/token_dict.txt', 'rb') as handle:
        token_dict = pickle.loads(handle.read()) 
    print(token_dict)

    i_token = 1
    fields_param = 'country,sex,bdate,city,country'\
                    ',photo_max_orig, online_mobile,has_mobile,contacts'\
                    ',connections,site,education,universities,schools,'\
                    ',can_write_private_message,status,last_seen,relation,relatives'
    friend_column_list = ['user_id', 'first_name', 'last_name', 'bdate', 'city', 'country',  
                        'sex', 'mobile_phone', 'photo', 'last_seen', 'univercities',
                        'faculty', 'graduation', 'last_seen_real']
    user_to_friend_list = ['user_id','friend_id']
    user_to_group_list = ['user_id', 'group_id']
    group_data_column_list = ['group_id', 'name', 'screen_name', 'description']
    column_statistics = ['user_number', 'user_id', 'n_friends', 'start_time', 
                        'end_time','start_fmt', 'end_fmt', 'time_user']

    #### GLOBAL VAR ENDED #################                    

    user_list, total_number_users, n_users_completed = read_stat_data()
    batch_size = 3

    #Serial processsing:
    # results = []
    # ts = time.time()
    # for i in range(0, batch_size):
    #     get_result(parse_vk(i))
    # print('Time in serial:', time.time() - ts)
    # print(results)

    
    ts = time.time()
    results = []
    batch_cnt = math.ceil(len(user_list) / batch_size)
    print('batch_cnt: ', batch_cnt)
    time_list = []
    
    print(f'Core # {mp.cpu_count()}')
    for i in range(batch_cnt):
        time_iter_begin = time.time()
        if batch_size*(i+1) <= len(user_list):
            i_start = batch_size * i
            i_end = batch_size * (i+1)
        else:
            i_start = batch_size * i
            i_end = len(user_list)
#            display(dataset.iloc[i_start:i_end, :]) 
        pool = mp.Pool(mp.cpu_count())
        print(f'batch_cnt: {batch_cnt}     i_start: {i_start}     i_end: {i_end}')
        time_begin = time.time()
        for j in range(i_start, i_end):
            # print('token ', token_dict[i_token])
            # pool.apply_async(my_function, args=(i, params[i, 0], params[i, 1], params[i, 2]), callback=get_result) 
            pool.apply_async(parse_vk, args=(j, user_list[j], token_dict[i_token], api_version), callback=get_result) 
        i_token += 1
        if i_token == 4: i_token = 1
        pool.close()
        pool.join()

        print('Time in parallel:', time.time() - ts)          
        # print('results: \n', results) 
        print("1 loop ended!") 
        break      
 





# for i, user_id in enumerate(user_list):
#     start_fmt = datetime.now().strftime("%Y-%m-%d @ %H:%M:%S")
#     start_time = time.time()
#     # i+=index_start

#     i+=n_users_completed
#     #### CREATE & REFRASH DATAFRAMES #####
#     df_friend_data = pd.DataFrame(columns=friend_column_list)
#     df_user_to_friend = pd.DataFrame(columns=user_to_friend_list)
#     df_user_to_group = pd.DataFrame(columns=user_to_group_list)
#     ###########################
#     print('*'*50) 
#     print('*'*50)
#     print("******************* Process number is ", process_number)
#     print(f' {i + 1 - index_start - n_users_completed} user {user_id} is in calculation process')
#     print(f'current time is {start_fmt}')
#     print('*'*50) 
#     url_friend = f'https://api.vk.com/method/friends.get?user_id={str(user_id)}&access_token={token_dict[2]}&v={api_version}'
#     #get number of users
#     try: 
#         request_time = time.time()
#         req = requests.get(url_friend, timeout=None)
#         print(req)
#         req = req.json()['response']
#         i_token+=1
#         if i_token == 5: i_token = 1
#         print(f'duration of friend number request {time.time()- request_time} sec')
#         # it was checked that when 'error_code': 30, 'error_msg': 'This profile is private' - exception occurs 
#         n_members = len(req['items'])
#         print(f'user_id # {user_id} has {n_members} friends')
#     except KeyError as e:
#         print('ATTENCTION')
#         print(f'exception {e} count_friends occured')
#         print('***********ATTENCTION')
#         n_members = 0
#         df_friend_data = pd.DataFrame(columns = friend_column_list)
#         update_statistics(path_stat, stat, i, user_id+'_exception_N_friends_request', n_members,  column_statistics, start_time, start_fmt)
#         with open(path_stat+file_log, 'a') as f_log:
#                 f_log.write(f'Unnable to write json for | {user_id} | with TypeError {e} | N friends request \n')
#         continue
#     #get groups the user in 
#     url_group = f'https://api.vk.com/method/users.getSubscriptions?user_id={str(user_id)}&access_token={token_dict[2]}&v={api_version}'
#     try: 
#         request_time = time.time()
#         req = requests.get(url_group, timeout=None).json()['response']['groups']
#         i_token+=1
#         if i_token == 5: i_token = 1
#         print(f'duration of group number request {time.time()- request_time} sec')
#     except KeyError as e:
#         print('ATTENCTION')
#         print(f'exception {e} groups occured')
#         print('***********ATTENCTION')
#         with open(path_stat+file_log, 'a') as f_log:
#                 f_log.write(f'Unnable to write json for | {user_id} | with TypeError {e} | group request \n')
#         update_statistics(path_stat, stat, i,  user_id+'_exception_group_request', n_members,  column_statistics, start_time, start_fmt)    
#         continue
#     try:
#         request_time = time.time()
#         with open(path_out+file_user_to_group_json+'_'+str(i+1)+'_'+user_id+'_json.json','w') as f_json:
#             json.dump(req, f_json)
#         print(f'time of of json file creating {time.time() - request_time} sec')  
#     except TypeError as e:
#         print(f'Unnable to write json for {str(user_id)}')    
#         with open(path_stat+file_log, 'a') as f_log:
#             f_log.write(f'Unnable to write json for | {str(user_id)} | with TypeError {e} | group request json to file \n')    
#         update_statistics(path_stat, stat, i,  user_id+'_exception_group_request', n_members,  column_statistics, start_time, start_fmt)    
#         continue          

#     request_time_start = time.time()
#     i_token = 1
#     time_friend_data_extract = time.time()
#     for i_offser, offset in enumerate(range(0, n_members+1, 1000)):     
#         url_friend = f'https://api.vk.com/method/friends.get?user_id={str(user_id)}&offset={offset}&count=1000&access_token={token_dict[i_token]}&v={api_version}&fields={fields_param}'
#         try:
#             request_time = time.time()
#             res_friends = requests.get(url_friend, timeout=None)
#             print(f'duration of friend data request {time.time() - request_time} sec')
#         except KeyError as e:
#             update_statistics(path_stat, stat, i,  user_id+'_exception_friends_request', n_members,  column_statistics, start_time,     start_fmt)
#             print('excepttion for friends request occur')
#             print('request key error: ', e)
#             with open(path_out+path_stat+file_log, 'a') as f_log:
#                 f_log.write(f'Unnable to write json for | {user_id} | with TypeError {e} | friends request \n')
#             continue 
#         try:
#             request_time = time.time()
#             with open(path_out+file_friend_data_json+'_'+str(i+1)+'_'+user_id+'_json'+str(i_offser+1)+'.json','w') as f_json:
#                 json.dump(res_friends.json(), f_json)
#             print(f'time of of json file creating {time.time() - request_time} sec')  
#         except TypeError as e:
#             print(f'Unnable to write json for {str(user_id)}')    
#             with open(path_stat+file_log, 'a') as f_log:
#                 f_log.write(f'Unnable to write json for | {str(user_id)} | with TypeError {e} | friends request json to file \n')    
#             update_statistics(path_stat, stat, i,  user_id+'_exception_friends_request', n_members,  column_statistics, start_time, start_fmt)    
#             continue        
#     ###########UPDATE_STAT#########################
#     update_statistics(path_stat, stat, i, user_id, n_members,  column_statistics, start_time, start_fmt)
#     ##############################################
#     print(f'{i+1} users were processed')
#     print(f'Execution for one user {(time.time() - start_time)} sec')
#     print(f'Total time of execution since the beginnig {statistics_df.time_user.sum()/60/60} hours')
#     print(f'{round((i+1-index_start-n_users_completed)/total_number_users*100,2)} % were processed') 
#     print(f'current time is {datetime.now().strftime("%Y-%m-%d @ %H:%M:%S")}')
#     print('*'*30)
#     print('*'*30)
#     print('*'*30)    
#     print('*'*60)
#     print('*'*60)
#     print('*'*25+" "*5+'(i+1)= '+str(i+1)+" "*5+'*'*25)  
#     print('*'*60)
#     print('*'*60)      
#     if (i+1)%(225) == 0: clear_output(wait=True)
# print('Game over'+'\n')