import re
import requests
from requests.exceptions import HTTPError
import json

for url in [#'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20160101&edate=20160229&state=06'
            #,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20160301&edate=20160430&state=06'
            #,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20160501&edate=20160630&state=06'
            #,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20160701&edate=20160831&state=06'
            #,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20160901&edate=20161031&state=06'
            #,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20161101&edate=20161231&state=06'
            'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20170101&edate=20170228&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20170301&edate=20170430&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20170501&edate=20170630&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20170701&edate=20170831&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20170901&edate=20171031&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20171101&edate=20171231&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20180101&edate=20180228&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20180301&edate=20180430&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20180501&edate=20180630&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20180701&edate=20180831&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20180901&edate=20181031&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20181101&edate=20181231&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20190101&edate=20190228&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20190301&edate=20190430&state=06'
            ,'https://aqs.epa.gov/data/api/sampleData/byState?email=test@aqs.api&key=test&param=88101&bdate=20190501&edate=20190630&state=06'
            ]:
    try:
        response = requests.get(url)
        json_response = response.json()
        Data = json_response['Data']

        #print(Data)
        bdate = re.search('bdate=(.*)&edate', url)
        edate = re.search('edate=(.*)&state', url)

        text_file = 'PM2.5_' + bdate.group(1) + '_' + edate.group(1) + '.json'

        #print('Writing file', text_file)

        with open(text_file, "w") as output_file:
            for objs in Data:
                output_file.write(json.dumps(objs) + '\n')

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Python 3.6
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.6
    else:
        print('Success!')
