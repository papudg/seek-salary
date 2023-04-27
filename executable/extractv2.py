import urllib3
from helper import *
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Set up file handler
handler = logging.FileHandler(f'extracts_tests/logs/log.txt')
handler.setLevel(logging.INFO)

# Set up formatters
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(handler)
### THIS SECTION IS FOR EXTRACTION FROM SEEK ###


################################################################################
def extract(): # For every URL and corresponding page
    for linkSet in Links:
        for url in linkSet:
            jobList = []
            name = getSubstring(str(url), 'jobs-in-','?')
            salary_range = getSubstring(str(url), 'salaryrange=', '&').replace('-','_')
            outputName = f'{name}${salary_range}'
            category = re.sub("-", " ",url[32:])
            numRecords = extract_num_records(url)
            numPages = int(numRecords/21) # Since 21 records per page
            logger.info(f'Starting {outputName}')
            if(numPages<1): # To read page 0
                pageTransform(1,1,jobList,category,url)
            # each page i till numPages
            for i in range(1, numPages): 
                pageTransform(i,numPages,jobList,category,url)
            df=pd.DataFrame(jobList)
            print(df)
            df.to_csv(f'extracts_tests/{outputName}.csv')

def pageTransform(i,numPages,jobList,category,url):
        loadingUIv2(i,numPages)
        # find the index of the first question mark in the URL
        qm_index = url.find('?')
        url_addon = url[:qm_index+1] + f'page={int(i)}&' + url[qm_index+1:]
        r=requests.get(url_addon,headers, timeout=50)
        soup=BeautifulSoup(r.content,'html.parser')
        transform(soup, jobList,category)   


################################################################################
# HELPER FUNCS - COMMON

def loadingUI(i, MAX):
    if i%int(MAX/100) == 0:
        str = '|'
        print(str, end ="")
    if(i==MAX):
        print('')

def loadingUIv2(i, MAX):
    perc = int((i/MAX)*100)
    clear_output(wait=True)
    str = '|'*+perc+f'{perc}%'
    print(str)
    print(f'Done {i} of {MAX}')

def getSubstring(str, startString, endString):
    x = str.index(startString) + len(startString)
    if endString  != '':
      y = str[x:].index(endString)+x
    else:
      y = len(str)
    return(str[x:y])
################################################################################
# Extracts number of records given the URL of a category 
def extract_num_records(url):
    r=requests.get(url,headers)
    try:
        substr = getSubstring(str(r.content), 'Find your ideal job at SEEK with ', ' jobs')
        numResults = int(re.sub(",", "", substr))
    except: 
        numResults = 1
    return numResults

# Transforms soup from seek to corresponding dataframe
def transform(soup, jobList,category):
    
    #divs=soup.find_all('div', class_='yvsb870 v8nw070 v8nw072') # UPDATE THIS
    divs=soup.find_all('div', class_='_1wkzzau0 a1msqi7e')
    # class="_1wkzzau0 szurmz0 szurmzb"

   # print(f'HERE IS THE divs: {divs}')
    for item in divs:
        try:
            #jobTitle=item.find('a', {'data-automation':'jobTitle'}).text.strip()
            jobTitle = f"'{item.find('a', {'data-automation':'jobTitle'})}'"
            # print(jobTitle)
            if("promoted" in jobTitle):
                print(jobTitle)
                continue
        except:
            jobTitle= ''
        try:
            CompanyNmae=item.find('a', {'data-automation':'jobCompany'}).text.strip()
        except:
            CompanyNmae= ''
        #print(CompanyNmae)
        
        try:
            jobSalary=item.find('span', {'data-automation':'jobSalary'}).text.strip()
        except:
            jobSalary= '-'
        try:
            jobLocationAll=item.find_all('a', {'data-automation':'jobLocation'})
            
            def remove_html(jobLocationAll):
                #print(jobLocationAll)
                return ','.join(xml.etree.ElementTree.fromstring(jobLocationAll).itertext())

            jobLocation=remove_html(jobLocationAll)
            print(jobLocation)
        except:
            jobLocation= ''
        try:
             Description=item.find_all('div',{'class': '_1wkzzau0 szurmz0 szurmz2'})
        except:
            Description= ''    

        job={
            'Category': category,
            'Title':jobTitle,
             'Company':CompanyNmae,
             'Salary':jobSalary,
             'Min Salary': 'TODO',
             'Max Salary': 'TODO',
             'jobLocation':jobLocationAll,
             'Description':Description
            }
        jobList.append(job)
    return len(divs)

extract()

