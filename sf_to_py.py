import snowflake.connector
from snowflake.connector import ProgrammingError
import subprocess
from td_columns import getcolumn


con = snowflake.connector.connect(
    user='DINESHM',
    password='Govindagovinda@9',
    account='uk04596.central-india.azure',
    warehouse='COMPUTE_WH'

)

li=[]
li2=[]
cursor=con.cursor()
s=cursor.execute("select * from TB_VOC.ADUIT_SCH.config_table;")
for i in cursor:
    li.append(i)
print(li)

# for column in li:
#     for row in column:
#         TD_TABLE_NAME = row


input_table_name='NATION'
TYPE='FULL'
#print(TD_TABLE_NAME)

# TD_SCH_NAME = li[column][row]
# SF_TABLE_NAME = li[1][2]

TD_TABLE_NAME=[]
TD_SCHEMA_NAME=[]

try:
    V=cursor.execute(f"Select * from TB_VOC.ADUIT_SCH.CONFIG_TABLE WHERE TD_TABLE_NAME='{input_table_name}';")
    for j in cursor:
        li2.append(j)
    TD_TABLE_NAME=li2[0][0]
    TD_SCHEMA_NAME=li2[0][1]
    print("TPT generating")
except snowflake.connector.errors.ProgrammingError as e:
    print(e)
    print("The table does not exit . Please insert into config table ")

#CONFIG_TABLE INFO
print(TD_TABLE_NAME)
print(TD_SCHEMA_NAME)
TD_DBNAME='demo_user'
col_list=getcolumn(TD_SCHEMA_NAME,TD_TABLE_NAME)
print(col_list)



#Teradata conn details
TD_USERNAME='demo_user'
TD_PASSWORD='Ranjith@db'
TD_HOST='dev-test12-cp5b3xu7q95ova8u.env.clearscape.teradata.com'


#TPT Job info
TABLE_NAME=TD_TABLE_NAME
TPT_NAME=TABLE_NAME+'_TPT'
JOB_NAME=TABLE_NAME+'_TPT_JOB'
File_name=TABLE_NAME+'_export'
Schema_name='NATION_SCH'
#print(TABLE_NAME,JOB_NAME,File_name,Schema_name)
export_path=r"C:\Users\ranjith.subbaiya\Downloads\tpt\csv"
file_path=fr"C:\Users\ranjith.subbaiya\Downloads\tpt\{TPT_NAME}.tpt"
writef=open(file_path, "w")
writef.write(f"""DEFINE JOB {JOB_NAME}
DESCRIPTION 'This job will load data in my empty table'
(


DEFINE SCHEMA {Schema_name}
(""")
comma=''
for col in col_list:
    if col in ["DATE","INTDATE"]:
        column_name=comma+col[3]+" varchar(10) "+" \n"
    else:
        column_name = comma+col[3] + " " + col[4] + "\n"
    comma=','

    writef.write(column_name)



# N_NATIONKEY BIGINT,
# N_NAME VARCHAR(64000),
# N_REGIONKEY BIGINT,
# N_COMMENT VARCHAR(64000)
#
# );

writef.write(f"""); 
  DEFINE OPERATOR DATACONNN
  DESCRIPTION 'TPT DataConnector CONSUMER Operator'
  TYPE DATACONNECTOR CONSUMER
  SCHEMA {Schema_name}
  ATTRIBUTES
  (
    VARCHAR DIRECTORYPath = '{export_path}',
	VARCHAR FileName = '{File_name}.CSV',
    VARCHAR FileSuffix = '.csv',
    VARCHAR OpenMode = 'Write',
    VARCHAR Format = 'Delimited',
    VARCHAR TextDelimiter = ',',
    VARCHAR QuotedData = 'Yes',
    INTEGER FileSize = 52428800
  );



DEFINE OPERATOR EXPORT_OPERATORR
  DESCRIPTION 'TPT export Operator'
  TYPE EXPORT
  SCHEMA {Schema_name}
  ATTRIBUTES
  (
    VARCHAR TdpId              = '{TD_HOST}',
    VARCHAR UserName           = '{TD_USERNAME}',
    VARCHAR UserPassword       = '{TD_PASSWORD}',
	INTEGER MinSessions = 1,
	INTEGER MaxSessions = 16,
    VARCHAR Selectstmt        = 'SELECT * FROM {TD_DBNAME}.nation;',
    VARCHAR LogTable           = 'demo_user.LOG_{TABLE_NAME}',
    VARCHAR ErrorTable1 = 'demo_user.ET1_nation',
    VARCHAR ErrorTable2 = 'demo_user.ET2_nation'

  );
  
  

    APPLY TO OPERATOR (DATACONNN)
    SELECT * FROM OPERATOR (EXPORT_OPERATORR);
);"""

)
writef.close()
print('TPT generated successfully')

result=subprocess.run(f'tbuild -f {file_path} -C')
print(result)
