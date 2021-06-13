import pandas as pd
import csv
import psycopg2
import numpy as np

class PREPROCESS:

    def __init__(self,dbname,ticker):
        self.path = "localhost"
        self.port = "5432"
        self.user = "dragonlook"
        self.password = ""

        self.dbname = dbname

        self.cols = ['date',
        'time',
        'timezone',
        'username',
        'tweet',
        'language',
        'replies_count',
        'retweets_count',
        'likes_count',
        'hashtags']
        self.pre_fix = ['date',
        'time',
        'timezone',
        'username',
        'language',
        'replies_count',
        'retweets_count',
        'likes_count']
        self.pre_mod = ['tweet']
        self.dtypes_pre = ['varchar(30)','varchar(30)','varchar(30)','varchar(300)','varchar','varchar(2)','integer','integer','integer','varchar(50)']
        # self.base_path = '/Users/dragonlook/Dropbox (MIT)/Private/System_Trade/Codes/MOTHER_DRAGON/NPL_dragon/lib/twint/STOCK_sentiment/'
        self.base_path = ''
        self.df_path = self.base_path + ticker +'.csv'
    def clensing(self):

        df = pd.read_csv(self.df_path,usecols=self.cols)
        # filter
        df = df[df.loc[:,'language'] == 'en'] # lang filter
        fil_ind = [ind for ind in range(len(df.loc[:,'hashtags'])) if len(df.loc[:,'hashtags'].to_list()[ind]) < 50 ]
        df = df.iloc[fil_ind,:] # filter by hashtags
        # count max vchar
        max_vals = []
        for pre_mod_col in self.pre_mod:
            max_vals.append(max([len(i) for i in df.loc[:,pre_mod_col]]))
        for mod_col, max_val in zip(self.pre_mod, max_vals):
            ind = np.where(np.array(self.cols) == mod_col)[0][0]
            self.dtypes_pre[ind] = self.dtypes_pre[ind] + '(' + str(max_val) + ')'
        df.to_csv(self.base_path + 'temp.csv')

    def create_spl_table(self,table_name,old=None):
        def create_db(cur, list_col, dname, dtypes=None, exist=None):
            drop_table = 'DROP TABLE '   + dname
            create_table = 'CREATE TABLE ' + dname + ' (' 
            for col, dtype in zip(list_col, dtypes):
                create_table = create_table + col + ' ' + dtype + ', '
                
            create_table = create_table[:-2] + ')'
            if exist:
                cur.execute(drop_table)
            cur.execute(create_table)

        if self.password:
            conText = "host={} port={} dbname={} user={} password={}"
            conText = conText.format(self.path,self.port,self.dbname,self.user,self.password)
        else:
            conText = "host={} port={} dbname={} user={}"
            conText = conText.format(self.path,self.port,self.dbname,self.user) 
        connection = psycopg2.connect(conText)
        connection.get_backend_pid()
        cur = connection.cursor()
        create_db(cur, self.cols, table_name, dtypes=self.dtypes_pre,exist=old)

        connection.commit()
        connection.rollback()

        with open(self.base_path + 'temp.csv', newline='') as csvfile:
            read = csv.reader(csvfile)
            cout = 0 
            for row in read:
                if cout == 0:
                    cout += 1
                    continue
                sql = "INSERT INTO " + self.dbname + " VALUES('{}',{})"
                sql = sql.format(str(row[0]).replace("/","-"),row[1])
                cur.execute(sql)
                connection.commit()
        #         print(row)

        cur.close()
        connection.close()