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

def concat_feature(select_feature, num_files):
    file_pre = './data/userFeature';
    user_feature = pd.read_csv('./data/userFeature1.csv')
    user_feature = user_feature[select_feature]
    gc.collect()

    for i in range(2,num_files+1):
        user_feature_tmp = pd.read_csv(file_pre+str(i)+'.csv')
        user_feature = pd.concat([user_feature,user_feature_tmp[select_feature]], ignore_index = True)
        del user_feature_tmp
        gc.collect()
        print('file concat ' + str(i))
    user_feature.to_csv('./data/userFeatureCompressed.csv', index=False)
    return user_feature

# userfeature = get_user_feature()
select_feature = ['LBS', 'age', 'carrier',
       'consumptionAbility', 'ct', 'education', 'gender', 'interest1',
       'interest2', 'interest5', 'kw1', 'kw2',
       'marriageStatus', 'os', 'topic2', 'topic3', 'uid']
userfeature = concat_feature(select_feature, 6)
del userfeature
gc.collect()
