# python-pyspark-base
Template for creating pyspark python based project questions

## Environment:
- Spark Version: 3.0.1
- Python Version: 3.7

## Read-Only Files:
### < In case any project has more read only files apart from below add in the below list>
- `src/app.py`
- `src/tests/test_pipeline.py`
- `src/main/__init__.py`
- `src/main/base/__init__.py`
- `src/main/job/__init__.py`
- `make.sh`
- `data/*`

## Requirements:
### <Add the detailed test requirement>
Template for creating pyspark python based project questions

## Commands
- run: 
```bash
source venv/bin/activate; cd src; python3 app.py ../data/data_file1.csv ../data/data_file2.csv
```
- install: 
```bash
bash install.sh; source venv/bin/activate; pip3 install -r requirements.txt
```
- test: 
```bash
source venv/bin/activate; cd src; py.test -p no:warnings
```

## Instructions                                       
  - This template is only applicable for dataframe tests with csv as input
  - Follow the below steps to create a new project from this template


# Step-1 - Create sample data files                              

/data
- All the test related csv's will go inside data/*.csv
- If test requires 2 data files provide names "data_file1.csv", "data_file2.csv"
- If test requires 3 data files provide names "data_file1.csv", "data_file2.csv", "data_file3.csv"
  and so on
  
  
# Step-2 - Create abstract methods need to be implemented

/src/main/base/__init__.py

- In __init__.py is the abstract class where below method are already defined 
  - read_csv(input_path)
  - stop()
- No need to change or update or remove init_spark_session() method  
- rest all, define unimplemented methods by adding @abc.abstractmethod above it
  - In the template we have created 2 sample methods below, take these as reference and create another methods
    distinct_ids()
    valid_age_count()
    
    
# Step-3 - Create job pipeline for umimplemented methods           

/src/main/job/pipeline.py

- No need to change or update or remove init_spark_session() method  
- create placeholder of all unimplemented methods defined in abstract class
  - In the template we have created 2 sample methods below, take these as reference and create another methods
    distinct_ids()
    valid_age_count()
    

# Step-4 - Create test pipeline for umimplemented methods                  

/src/tests/test_pipeline.py
- Create the schema and sample data for the number of sample data files, we have created 2 sample below methods, take these as reference and create
    data_file1_schema
    data_file1_sample
    data_file2_schema
    data_file2_sample
    - Like that if you have 3 files create schema and sample for the 3rd one and so on.
    
- No need to change or update or remove create_sample() method, this is used to create the dataframe from the sample data and schema

- create tests of all unimplemented methods defined in abstract class
    - No need to change or update or remove test_init_spark_session() test.
    - use "test_" before the method you wanted to implement, see the 2 sample tests in the template we have created, take these as reference and create another methods
    test_distinct_ids()
    test_valid_age_count()
    - you can create multiple tests based on multiple sample data sets, see second test of test_valid_age_count2()
        - Create another sample data data_file2_sample2
        - Create another test test_valid_age_count2()
        

# Step-5 - Create driver file i.e. app.py                     

/src/app.py

- In this file you need to read the csv file for the dataframe of unimplemented methods.
    - use read_csv method to read all the data csv files and get the dataframes.
        - see samples under "<<Reading CSV>>"
    - call the functions by passing the dataframes and print the output
        - see samples under <<Distinct IDs>> and << Valid age records is greater than equal 18 >>
        - In case you have more functions use the same pattern and display the output
        

# Step-6 - Changes in hackerrank.yml, make.sh, requirements.txt                          
 
/hackerrank.yml
- Not much change in this file, only one change i.e. based on command line arguments passed
    - Under configuration/ide_config/project_menu add or remove the data files based on the question.
        run: "source venv/bin/activate; cd src; python3 app.py ../data/data_file1.csv ../data/data_file2.csv"   
    - If question has 2 data files keep as it is no change.
    - If question has 3 data files add one more parameter as below and so on.
        run: "source venv/bin/activate; cd src; python3 app.py ../data/data_file1.csv ../data/data_file2.csv ../data/data_file3.csv"
    
/make.sh
- No change keep as is

/requirements.txt
- No change keep as is
     