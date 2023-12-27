import psycopg2
import pandas as pd
import time

from datetime import date, timedelta, datetime

class DataConnection():
    def __init__(self, username, password, host, port, db_name):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.connection = psycopg2.connect(
            dbname=self.db_name,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.connection.cursor()

    def query_to_df(self, query):
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def get_derivative_matched_data(self, 
                 start_date, 
                 end_date, 
                 start_hour, 
                 end_hour, 
                 time_delay):
        

        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        start_hour = datetime.strptime(start_hour, '%H:%M:%S')
        end_hour = datetime.strptime(end_hour, '%H:%M:%S')
        time_delay = timedelta(seconds=time_delay)
        
        start_datas = []
        end_datas = []
        current_date = start_date
        while current_date <= end_date:
            # 
            query = f'''
                    SELECT m.datetime as start_time, m.price as start_price
                    FROM quote.matched m 
                    JOIN quote.futurecontractcode ft
                    ON m.tickersymbol = ft.tickersymbol
                    WHERE ft.futurecode='VN30F1M'
                    AND m.datetime >= '{current_date.strftime('%Y-%m-%d')} {start_hour.strftime('%H:%M:%S')}'
                    AND m.datetime <= '{current_date.strftime('%Y-%m-%d')} {end_hour.strftime('%H:%M:%S')}'
                    AND (m.datetime::time > '{start_hour}') AND (m.datetime::time < '{str(time_delay + start_hour)}')
                    ORDER BY m.datetime ASC
                    LIMIT 1
                    '''
            self.cursor.execute(query)
            retrieved_start_data = self.cursor.fetchall()

            ## Query end data
            # query = f'''
            #         SELECT m.datetime as end_time, m.price as end_price
            #         FROM quote.matched m
            #         JOIN quote.futurecontractcode ft
            #         ON m.tickersymbol = ft.tickersymbol
            #         WHERE ft.futurecode='VN30F1M'
            #         AND m.datetime >= '{current_date.strftime('%Y-%m-%d')} {start_hour.strftime('%H:%M:%S')}'
            #         AND m.datetime <= '{current_date.strftime('%Y-%m-%d')} {end_hour.strftime('%H:%M:%S')}'
            #         ORDER BY m.datetime DESC
            #         LIMIT 1
            #          '''
            query = f'''
                    SELECT m.datetime as end_time, m.price as end_price
                    FROM quote.matched m
                    JOIN quote.futurecontractcode ft
                    ON m.tickersymbol = ft.tickersymbol
                    WHERE ft.futurecode='VN30F1M'
                    AND m.datetime >= '{current_date.strftime('%Y-%m-%d')} {start_hour.strftime('%H:%M:%S')}'
                    AND m.datetime <= '{current_date.strftime('%Y-%m-%d')} 23:59:00'
                    AND m.datetime::time > '11:29:45'
                    ORDER BY m.datetime ASC
                    LIMIT 1
            '''
            self.cursor.execute(query)
            retrieved_end_data = self.cursor.fetchall()

            if len(retrieved_start_data) > 0 and len(retrieved_end_data) > 0:
                start_datas.append(retrieved_start_data[0])
                end_datas.append(retrieved_end_data[0])

            current_date += timedelta(days=1)
        
        assert len(start_datas) == len(end_datas), "Start datas and end datas must have the same length"
        start_datas = pd.DataFrame(start_datas, columns=['start_time', 'start_price'])
        end_datas = pd.DataFrame(end_datas, columns=['end_time', 'end_price'])
        datas = pd.concat([start_datas, end_datas], axis=1)
        return datas


    def close(self):
        self.cursor.close()
        self.connection.close()
    

    def __del__(self):
        self.close()

    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    
    