import os
import subprocess
import snowflake.connector
import toml
from dotenv import load_dotenv
from os import getenv
from jinja2 import Environment, FileSystemLoader

#Custom imports#################################################################

################################################################################

def get_target_file(dir, ext, multi=False):

    """
    Return the file from the specified dir with the matching file extenstion

    inputs:
    - dir: String containing the path to the directory
    - ext: String containing the file extenstion
    - multi: Boolean, if true then multiple files can be returned
    """

    files = [f for f in os.listdir(dir) if (
            os.path.isfile(os.path.join(dir, f)) and
            f.endswith(ext))]

    if files == []:
        print(f"No {ext} files found in {dir}.")
        return []

    if len(files) > 1 and not multi:
        print(files)
        print(f"Multiple {ext} files found in {dir}. Please remove unrelated files.")
        return []
    
    return files

def cleanse_source_file(ds, dir, filename, suffix=""):
    src = os.path.abspath(dir + filename)
    dst = os.path.abspath(dir + ds + "_" + suffix + ".csv")

    os.rename(src, dst)

def stage(file_path):

    """
    Stage a given file
    """

    env = Environment(loader=FileSystemLoader("./src"))
    template = env.get_template("stage_file.sql.jinja")

    params = {
        "role": getenv("ROLE"),
        "database": getenv("DATABASE"),
        "schema": getenv("SCHEMA"),
        "stage_name": "LOADING_STAGE",
        "file_path": "file://" + file_path,
        "auto_compress": "false",
        "overwrite": getenv("STAGE_OVERWRITE")
    }

    # Render the template
    rendered_sql = template.render(**params).strip()

    # Run PUT using snowsql
    subprocess.run([
        "snowsql",
        "-a", getenv("ACCOUNT"),
        "-u", getenv("USER"),
        "-d", getenv("DATABASE"),
        "-s", getenv("SCHEMA"),
        "-r", getenv("ROLE"),
        "-w", getenv("WAREHOUSE"),
        "--authenticator", getenv("AUTHENTICATOR"), 
        "-q", rendered_sql
    ], 
    stdout=subprocess.DEVNULL, #Supresses stdout for successful executions
    stderr=None,
    check=True)

def ingest_csv(file_name, destination_table, columns, 
               custom_columns=False, 
               skip_rows=1, field_delimiter=',', encoding="UTF-8"):
    """
    Ingest a given file
    """
    env = Environment(loader=FileSystemLoader("./src"))
    template = env.get_template("ingest_csv.sql.jinja")

    column_list = ", ".join(f'"{col}"' for col in columns.keys())
    column_definitions = ",\n  ".join(f'"{col}" {dtype}' for col, dtype in columns.items())

    if custom_columns:
        custom_column_list = ", " + ", ".join(f'"{col}"' for col in custom_columns.keys())
        custom_column_values = ", " + ", ".join(
            [f'{custom_columns[x]["value"]} AS {x}' for x in custom_columns.keys()])
        custom_column_definitions = ", " + ",\n  ".join(
            [f'"{col}" {custom_columns[col]["type"]}' for col in custom_columns.keys()]
        )
    else:
        custom_column_list = ""
        custom_column_values = ""
        custom_column_definitions = ""

    params = {
        "file_name": file_name,
        "staging_table": destination_table + "__STAGING",
        "destination_table": destination_table,
        "stage_name": "LOADING_STAGE",
        "skip_rows": skip_rows,
        "field_delimiter": field_delimiter,
        "encoding": encoding,
        "columns": column_list,
        "column_definitions": column_definitions,

        "custom_column_list": custom_column_list,
        "custom_column_values": custom_column_values,
        "custom_column_definitions": custom_column_definitions
    }

    # Render the template
    rendered_sql = template.render(**params)
    #print(rendered_sql)

    # --- Connect to Snowflake ---
    conn = snowflake.connector.connect(
        user=getenv("USER"),
        authenticator=getenv("AUTHENTICATOR"),
        account=getenv("ACCOUNT"),
        warehouse=getenv("WAREHOUSE"),
        database=getenv("DATABASE"),
        schema=getenv("SCHEMA"),
        role=getenv("ROLE")
    )

    # --- Execute the rendered SQL ---
    cur = conn.cursor()
    try:
        cur.execute(rendered_sql, num_statements=5)
        print("Ingestion completed.\n")
    finally:
        cur.close()
        conn.close()

#Load environment variables
load_dotenv(override=True)
config = toml.load("config.toml")

#Load dataset information
datasets = config["base"]["datasets"]

#Initilise custom columns
custom_columns = False

#Custom processing##############################################################

#Example of custom processing that adds a year and month field determined by the user

# #Prompt the user for a year and month as parsing this from the data is unreliable
# while True:
#     year_str = input("Please enter the year for the data (YYYY):").strip()
#     if year_str.isdigit() and 2000 <= int(year_str) <= 2100:
#         year = int(year_str)
#         break
#     else:
#         print("Please enter a valid year value.")

# while True:
#     month_str = input("Please enter the month for the data (MM):").strip()
#     if month_str.isdigit() and 1 <= int(month_str) <= 12:
#         month = int(month_str)
#         break
#     else:
#         print("Please enter a valid month value.")

# #For naming convention, give custom column names a prefix of "_" (i.e. _Year)
# #Note for string values, enclose them in quotation marks (i.e. "'NCL'")
# custom_columns = {
#     "_Year":{
#         "value":year,
#         "type":"NUMBER"
#     },
#     "_Month":{
#         "value":month,
#         "type":"NUMBER"
#     }
# }

################################################################################

#Process each dataset indivdually
for ds in datasets:

    print(f"\nProcessing {ds} data:\n")

    #Get the latest data file
    file_ext = config["table"][ds]["file_ext"]
    rel_path = f"./data/{config["table"][ds]["data_dir"]}"

    multi_files = getenv("MULTI_FILES") == "true"
    target_files = get_target_file(rel_path, file_ext, multi_files)

    #Get any optional fields in the toml
    if "field_delimiter" in config["table"][ds]:
        field_delimiter = config["table"][ds]["field_delimiter"]
    else:
        field_delimiter = ','

    if "encoding" in config["table"][ds]:
        encoding = config["table"][ds]["encoding"]
    else:
        encoding = "UTF-8"

    for suffix, target_file in enumerate(target_files):

        target_id = f"{ds}_{str(suffix)}"

        #Cleanse the file name as unusual filenames can mess with the staging sql
        if target_file != f"{target_id}.csv":
            cleanse_source_file(ds, rel_path, target_file, str(suffix))
            target_file = f"{target_id}.csv"

        file_path = os.path.abspath(rel_path + target_file).replace("\\", "/")

        #Stage the file
        print(f"Staging {target_id} data...")
        stage(file_path)

        #Ingest the staged file into a table
        print(f"Ingesting the {target_id} data...")
        if file_ext == "csv":
            ingest_csv(
                target_file, 
                destination_table=config["table"][ds]["table_name"], 
                columns=config["table"][ds]["columns"],
                custom_columns=custom_columns,
                skip_rows=config["table"][ds]["skip_rows"],
                field_delimiter=field_delimiter,
                encoding=encoding
            )

        if getenv("ARCHIVE_FILES") == "true":
            #Archive the source file if successful to prevent reuploading data
            archive_dir = os.path.abspath(
                f"./data/archive/{config["table"][ds]["data_dir"]}/")
            
            if os.path.exists(archive_dir) == False:
                os.makedirs(archive_dir)

            os.rename(os.path.abspath(rel_path + target_file),
                    os.path.join(archive_dir, target_file))
        