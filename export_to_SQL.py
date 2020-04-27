from sqlalchemy import create_engine
import urllib
import create_table


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=localhost,1433;"
                                 "DATABASE=SpartaDB;"
                                 "UID=SA;"
                                 "PWD=Passw0rd2018")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

test_inst = create_table.Academy()
data_academies = test_inst.get_academy()
print(data_academies)

data_academies.to_sql('Academies', con=engine, if_exists='append', index=False)
