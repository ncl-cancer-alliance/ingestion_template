# NCL Cancer Alliance Ingestion Template

This is an alternative template used to create ingestion pipelines for snowflake.

Currently this template only works with csv files.

__Please update/replace this README file with one relevant to your project__

Refer to the generic project template for standard best practice: [Link](https://github.com/ncl-cancer-alliance/nclca_template/blob/main/README.md)

## Scripting Guidance

Please refer to the Internal Scripting Guide documentation for instructions on setting up coding projects including virtual environments (venv).

The Internal Scripting Guide is available here: [Internal Scripting Guide](https://nhs.sharepoint.com/:w:/r/sites/msteams_38dd8f/Shared%20Documents/Document%20Library/Documents/Git%20Integration/Internal%20Scripting%20Guide.docx?d=wc124f806fcd8401b8d8e051ce9daab87&csf=1&web=1&e=qt05xI)

## Usage

For a new ingestion pipeline, create a new repository project on git hub and select ncl-cancer-alliance/ingestion_template as the "Repository template".

You still need to set up the project as usual as depicted in the [Internal Scripting Guide](https://nhs.sharepoint.com/:w:/r/sites/msteams_38dd8f/Shared%20Documents/Document%20Library/Documents/Git%20Integration/Internal%20Scripting%20Guide.docx?d=wc124f806fcd8401b8d8e051ce9daab87&csf=1&web=1&e=qt05xI).

Create a .env file using the sample.env file as a reference. Some fields are left blank and can be filled in using the Account Details section on the Snowflake website. The DATABASE and SCHEMA fields refer to the destination location where both file staging and the destination tables will appear.

Before running the code, any named schema and database in the .env file must be created in snowflake first.

Replace the demo example in the config.toml with details of your pipeline(s). Each pipeline should include the following:
* Be listed using the dataset name in the [base][datasets] array
* Have dataset specific information under [table.{Your Dataset Name}] (e.g. [table.demo] for the demo dataset)
  * The setting under [table.{Your Dataset Name}] includes:
    * file_ext (file extension): csv or otherwise
    * data_dir: Where in the data directory your data files for this dataset will be stored
    * table_name: Destination table for your ingestion (the database and schema is chosen in the .env file)
    * skip_rows: What row the data header is in the source file
    * (Optional) field_delimiter: What delimiter is used in the csv source files. Default: ','
    * (Optional) encoding: What encoding is used in the csv source file. Default: "UTF-8"
* Contain column information in [table.{Your Dataset}.columns] (e.g. [table.demo.columns] for the demo dataset)
  * Each item in this section should contain the column name and data type (e.g. "Organisation Code" = "VARCHAR" defines a column called Organisation with the VARCHAR data type)

When the src/main.py code is executed, all data files found in the corresponding data_dir locations as specified in the config.toml will be ingested into Snowflake. For example, with the default demo settings, all csv files in data/demo/ will be processed.

The ingested data will have a _TIMESTAMP column appended to the destination table for logging and debugging purposes.

### Custom Processing
There is dedicated space in the main.py script to add custom code for specific ingestion pipelines. For example adding user specified labels or hardcoded values not in the data file itself.

## Changelog

### [1.0.0] - 2025-07-28
- Initial release of the ingestion template
- Capable of ingestion pipelines using csv files

### [1.1.0] - 2025-07-29
- Can archive data files after processing with ARCHIVE_FILES setting in .env
- Can process multiple csv files for a dataset with MULTI_FILES setting in .env

## Licence
This repository is dual licensed under the [Open Government v3]([https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) & MIT. All code can outputs are subject to Crown Copyright.

## Contact
Jake Kealey - jake.kealey@nhs.net
