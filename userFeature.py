# coding=utf-8
# @author:bryan
# blog: https://blog.csdn.net/bryan__
# github: https://github.com/YouChouNoBB/2018-tencent-ad-competition-baseline
import pandas as pd
import gc
import os



def get_user_feature():
    if os.path.exists('./data/userFeature.csv'):
        user_feature=pd.read_csv('./data/userFeature.csv')
    else:
        userFeature_data = []
        num = 1
        with open('./data/userFeature.data', 'r') as f:
            for i, line in enumerate(f):
                line = line.strip().split('|')
                userFeature_dict = {}
                for each in line:
                    each_list = each.split(' ')
                    userFeature_dict[each_list[0]] = ' '.join(each_list[1:])
                userFeature_data.append(userFeature_dict)
                if i % 100000 == 0:
                    print(i)
                if (i > 0 and i % 2000000 == 0):
                    user_feature = pd.DataFrame(userFeature_data)
                    user_feature.to_csv('./data/userFeature'+str(int(num))+'.csv', index=False)
                    num += 1
                    del userFeature_data
                    del user_feature
                    gc.collect()
                    userFeature_data = []
            user_feature = pd.DataFrame(userFeature_data)
            user_feature.to_csv('./data/userFeature'+str(int(num))+'.csv', index=False)
        gc.collect()
    return user_feature


userfeature = get_user_feature()
del userfeature
gc.collect()
