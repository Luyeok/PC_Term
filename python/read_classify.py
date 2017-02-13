#-*-coding:utf-8-*-
#2017-2-13
#本脚本用来读取股票分类信息
#并将分类信息存入本地数据库中
#以便将服务器数据库导入本地数据库后，利用分类信息进行分析
'''#######################################################'''
#导入pandas、MySQLdb以及slqalchemy、os、shutil库
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import tushare as ts
#import pdb
#设置全局变量
#DB_ENGINE为创建的engine，为连接数据库做准备；
DB_ENGINE = create_engine(r'mysql+pymysql://root:luyeonline8845@localhost:3306/finance?charset=utf8')


#读取tushare股票分类数据
def read_classify():
    #读取行业分类
    db_classify=ts.get_industry_classified()
    #读取概念分类
    db_concept=ts.get_concept_classified()
    
    #修改列名
    db_classify.columns=['code','name','class']
    db_concept.columns=['code','name','concept']
    
    #去除name、c_name列里的空格
    db_classify['name']=db_classify['name'].map(lambda x : x.replace(" ",''))
    db_classify['class']=db_classify['class'].map(lambda x : x.replace(" ",''))
    db_concept['name']=db_concept['name'].map(lambda x : x.replace(" ",''))
    db_concept['concept']=db_concept['concept'].map(lambda x : x.replace(" ",''))
    
    #写入数据库
    db=DB_ENGINE.connect()
    db_classify.to_sql('stock_classify',db,if_exists='append',index=False)
    db_concept.to_sql('stock_concept',db,if_exists='append',index=False)
    db.close()


if __name__=='__main__':
    read_classify()
    #这里，数据库使用完毕，需要dispose相应的资源；
    DB_ENGINE.dispose()
