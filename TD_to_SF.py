import snowflake.connector
from snowflake.connector import ProgrammingError
import subprocess
from td_columns import getcolumn
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from concurrent.futures import ProcessPoolExecutor
import os





#Teradata conn details

def script_generator(TD_TABLE_NAME,TD_SCHEMA_NAME):
    #print('i')
    TD_USERNAME='demo_user'
    TD_PASSWORD='Ranjith@db'
    TD_HOST='dev-test12-cp5b3xu7q95ova8u.env.clearscape.teradata.com'

    #TPT Job info
    TABLE_NAME=TD_TABLE_NAME
    TD_DBNAME=TD_SCHEMA_NAME
    TPT_NAME=TABLE_NAME+'_TPT'
    JOB_NAME=TABLE_NAME+'_TPT_JOB'
    File_name=TABLE_NAME+'_export'
    Schema_name='NATION_SCH'
    #print(TABLE_NAME,JOB_NAME,File_name,Schema_name)
    export_path=fr"C:\Users\ranjith.subbaiya\Downloads\tpt\csv"
    file_path=fr"C:\Users\ranjith.subbaiya\Downloads\tpt\{TPT_NAME}.tpt"
    file_info.append([file_path,File_name,export_path])
    writef=open(file_path, "w")
    print('TPT file started generating')
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
    VARCHAR LogTable           = '{TD_DBNAME}.LOG_{TABLE_NAME}',
    VARCHAR ErrorTable1 = '{TD_DBNAME}.ET1_nation',
    VARCHAR ErrorTable2 = '{TD_DBNAME}.ET2_nation'

  );
  
  

    APPLY TO OPERATOR (DATACONNN)
    SELECT * FROM OPERATOR (EXPORT_OPERATORR);
);"""

)
    writef.close()
    print('TPT generated successfully')




def tptrun(file_path,file_name,export_path):
    result=subprocess.run(f'tbuild -f {file_path} -C')
    print(result)
    print(f"{file_name} file generated successfully")
    cmd = ['aws', 's3', 'cp', f'{export_path}', f's3://tdsfbucket/TDEXPORT/{file_name}/', '--recursive',
           '--exclude', '*', '--include', f'*{file_name}*']

    print(cmd)
    t = subprocess.run(cmd, capture_output=True, text=True)
    print(t.returncode)

if __name__=="__main__":

    #SF Credentials
    con = snowflake.connector.connect(
        user='DINESHM',
        password='Govindagovinda@9',
        account='uk04596.central-india.azure',
        warehouse='COMPUTE_WH'
    )
    cursor=con.cursor()


    #aws credential
    S3_LOCATION = f's3://tdsfbucket/TDEXPORT/NATION_export/'
    aws_access_key_id = 'AKIA2RP6ICJRZTVTNRTE'
    aws_secret_access_key = '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'

    input_table_name='NATION'
    TYPE='FULL'

    #Empty list for appending filepath
    file_info = []


    try:
        Value= cursor.execute(f"select * from TB_VOC.ADUIT_SCH.config_table where TD_TABLE_NAME='{input_table_name}' ;")
        result1 = cursor.fetchone()
        print(result1)
        if result1 is None:
            raise ValueError("QUERY")
        else:
            TD_TABLE_NAME = result1[0]
            TD_SCHEMA_NAME = result1[1]
            SF_TABLE_NAME=result1[2]
            SF_DATABASE_NAME=result1[3]
            SF_SCHEMA_NAME=result1[4]

    except ValueError as e:
        print("The audit entry has no values. Please log audit details into the table")
        exit(0)
    except snowflake.connector.errors.ProgrammingError as e1:
        print(e1)
        exit(0)

    #CONFIG_TABLE INFO
    print(TD_TABLE_NAME)
    print(TD_SCHEMA_NAME)

    col_list=getcolumn(TD_SCHEMA_NAME,TD_TABLE_NAME)
    print(col_list)
    #vi=script_generator(TD_TABLE_NAME,TD_SCHEMA_NAME,col_list)
    with ThreadPoolExecutor() as executor:
        status_code_tpt_scr_gen = {executor.submit(script_generator,TD_TABLE_NAME,TD_SCHEMA_NAME)}
        for return_code in as_completed(status_code_tpt_scr_gen):
            print(return_code.result(), "Return Code")

    with ProcessPoolExecutor() as executor:
        #print("Kanna")
        print(file_info)
        status_code_tpt={executor.submit(tptrun,file_info[0][0],file_info[0][1],file_info[0][2])}

    comma_sep = ''
    for col in col_list:
        comma_sep = comma_sep + col[3] + " " + col[4] + ","
    Columns = comma_sep[:-1]
    print(Columns)

    f = f"create or replace table {SF_DATABASE_NAME}.{SF_SCHEMA_NAME}.{SF_TABLE_NAME} ({Columns} );"
    print(f)
    try:
        s = cursor.execute(f)
        print('table created')
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        exit(0)

    copy = (f"""COPY INTO {SF_DATABASE_NAME}.{SF_SCHEMA_NAME}.{SF_TABLE_NAME} FROM {S3_LOCATION} credentials=(AWS_KEY_ID='{aws_access_key_id}'
           AWS_SECRET_KEY='{aws_secret_access_key}') FILE_FORMAT = (TYPE = CSV  FIELD_OPTIONALLY_ENCLOSED_BY = '"')
           ;""")
    print(copy)
    try:
        s = cursor.execute(copy)
        print('table loaded')
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        exit(0)