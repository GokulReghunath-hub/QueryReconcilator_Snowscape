 # Query Reconcilator 


## Installation

### Installing Python
For application to work in system you need to install python upfront.
	-   Install   [Python version 3.8  or higher for Windows.](https://www.python.org/downloads/windows/)

Check your Python installation in CLI with

    python --version

### Installing PIP On Windows

Follow the steps outlined below to install PIP on Windows.

#### Step 1: Download PIP get-pip.py

Before installing PIP, download the  [get-pip.py file](https://bootstrap.pypa.io/get-pip.py).

1. Launch a command prompt if it isn't already open. To do so, open the Windows search bar, type  **cmd**  and click on the icon.

2. Then, run the following command to download the  **get-pip.py**  file:
After you succesfully installed Python and pip then you can go for installing Snowflake Connector.
The Snowflake Connector for Python uses many Python packages. The connector supports a range of versions for each package.


#### Step 2: Installing PIP on Windows

    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

To install PIP type in the following:


    python get-pip.py
    
#### Step 3: Verify Installation

Once you’ve installed PIP, you can test whether the installation has been successful by typing the following:

    pip help
If PIP has been installed, the program runs, and you should see the location of the software package and a list of commands you can use with **`pip`**.

Check your Pip installation in CLI with

    pip --version

#### Upgrading PIP for Python on Windows
To upgrade PIP on Windows, enter the following in the command prompt:

    python -m pip install --upgrade pip

This command uninstalls the old version of PIP and then installs the most current version of PIP.

### Installing Snowflake Connector

The Snowflake Connector for Python uses many Python packages. For a more details about of Snowflake Python connector, visit the [Snowflake Doc.](https://docs.snowflake.com/en/user-guide/python-connector-install.html#label-python-connector-prerequisites-python-packages)

#### Step 1: Install the Connector file

The Snowflake Connector for Python is available in  [PyPI](https://pypi.org/project/snowflake-connector-python/). A change log is available on the site, so you can determine the changes that have been implemented in each release.

When installing a version of the Snowflake Connector for Python, Snowflake recommends installing the versions of the dependent libraries that have been tested with that version of the connector.

To install the Snowflake Connector for Python and the dependent libraries:

1.  Determine the version of the Snowflake Connector for Python that you plan to install.
    
2.  To install the dependent libraries, run the  `pip`  (or  `pip3`) command and point to  [the requirements file](https://docs.snowflake.com/en/user-guide/python-connector-install.html#label-python-connector-prerequisites-python-packages)  for that version of the connector.
    
    For example, suppose the latest Snowflake Connector for Python version is 2.4.6 and you are using Python 3.6. To install the dependent libraries for that version of the connector, run the following command:
#####

    pip install -r [https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v](https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v)2.4.6/tested_requirements/requirements_36.reqs

In the example above, the path to the requirements file specifies the version of the connector (“/v2.4.6/”). The requirements filename (“requirements_36.reqs”) specifies the version of Python (Python 3.6).

> Note
>
> If you need to install a version of the Snowflake Connector for Python that is between 2.2.0 and 2.3.5, replace  `.reqs`  in the requirements filename with `.txt`. For example, use  `requirements_36.txt`, rather
 than  `requirements_36.reqs`.

#### Step 2: Install the Connector

To install the connector, run the following command:

     pip install snowflake-connector-python==<version>
where  `_version_`  is the version of the connector that you want to install.

For example, to install version 2.4.6 of the Snowflake Connector for Python, run:

    pip install snowflake-connector-python==2.4.6

#### Step 2: Verify Your Installation

Run snowflake connection test python file `Snowflake_Connection_Validate.py` containing the following Python sample code, which connects to Snowflake and displays the Snowflake version.

Before running the example

 - Replace  `<user_name>`  with the Nike user name that you use to connect to Snowflake.Next, execute the sample code

Next, execute the sample code
#####

    Python Snowflake_Connection_Validate.py

The Snowflake version (e.g. `3.5.0`) should be displayed.

### Installing Packages

Install below packages to prior to running the code.

    pip install pyodbc
    pip install pandas
    pip install numpy



## Configure your profile

You need to configure the connection profile in your system to intiate transaction to the database enviroment. You can set the connections to your database through config.py file. 
For cofiguring connection profile you should edit /config.py file which is there in the specific user folder.
#### Snowflake Profile
Snowflake can be configured using basic connection authentication as shown below.

    ~/config.py


    #Account Information.
    #Give all values enclosed by single qoutes
    user = <Nike User Name> 
    role = <Snowflake Role>
    warehouse = <Snowflake Warehouse>
    
    #Database and Schema Information
    snowflake_db_count = <Number of connections in Snowflake>
    database1 = <Snowflake DB Name>
    schema1 = <Snowflake schema Name>
    database2 = <Snowflake DB Name 2>
    schema1 = <Snwowflake Schema Name 2>
	
	  #Give as many database names and schema names you want to run the reconciliation like database'n' and schema'n'




Great!🎉. Once the repo is cloned you can navigate to cloned dbt directory and start using the  Snowflake Reconcilator.

If all are good then run the reconcilator by

     python Nike_SF_Query_Reconciliator.py

 
