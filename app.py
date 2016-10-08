import json
import traceback
from datetime import datetime
import shutil #file operations
from collections import defaultdict
import re #regex
import email #.eml format parsing
import os
import zipfile #compression

import xmltodict #xml parsing

EMAIL_PATH = '/mnt/email/edrm-enron-v2'
UNZIP_PATH = './zeal_unzip'
CSV_PATH = './zeal.csv'
LOG_FILE = './log.txt'
ZIP_EXTENSION = '_xml.zip'
EMAIL_EXTENSION = '.eml'
EMAIL_XML_EXTENSION = '.xml'
STATE_FILE = './state.json'
TOP_EMAILS_COUNT = 100

# ------------------------ Processing functions ----------------------

# write to log file function
def log(text):
    print(datetime.now().isoformat(), text)
    with open(LOG_FILE, 'a+') as f:
        f.write(datetime.now().isoformat())
        f.write(': ')
        f.write(text)
        f.write('\n')

# extract emails from text
def parse_emails(text):
    return re.findall(r"([^;<>'\"\s]+@[^;<>'\"\s]+\.[^;<>'\"\s]{2,3})", text.lower())

#get list of files with specific extension in path
def find_files_with_extenstion(path, extension):
    for dp, dn, filenames in os.walk(path):
        for f in filenames:
            if f.endswith(extension):
                yield os.path.join(dp, f)


#from an email object - return the number of words
def get_email_info(email_object):
    return {
        'words_count': get_word_count(email_object.get_payload()),
    }


#Read .eml file and split up the email structure and call: get_email_info
def get_email_message(email_path):
    with open(email_path, 'r') as f:
        email_object = email.message_from_string(f.read())
        #if multi-part get components else take as is.
        if email_object.is_multipart():
            for payload in email_object.get_payload():
                yield get_email_info(email_object)
        else:
            yield get_email_info(email_object)

#Extract all zip files
def unzip_email_files(files):
    start_time = datetime.now()
    files_count = len(files)
    n = 1
    email_folders = set()
    log('Going to unzip {} files'.format(files_count))
    previous_folder = None
    
    #skip files already processed on restart
    if os.path.isfile(STATE_FILE):
        log('Load from {}'.format(STATE_FILE))
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        email_folders = set(state['email_folders'])
        n = len(state['email_folders']) + 1
    
                                       
    #go through folders and pick up the zip files
    for item in files:
        if os.path.basename(item)[:-4] in email_folders:
            log('Skip {} cause results processed in {}'.format(item, STATE_FILE))
            continue
        try:
            if previous_folder:
                shutil.rmtree(previous_folder)
            unzip_path = os.path.join(UNZIP_PATH, os.path.basename(item))[:-4]
        except Exception:
            log('Can\'t delete {}'.format(previous_folder))
        log('Unzip {} to {}'.format(item, unzip_path))
                                       
        #unzip the file
        try:
            zip_ref = zipfile.ZipFile(item)
            zip_ref.extractall(unzip_path)
            zip_ref.close()
            yield unzip_path
        except zipfile.BadZipFile as e:
            log('Exception (zipfile.BadZipFile): {}'.format(e))
        previous_folder = unzip_path
        elapsed_time = (datetime.now() - start_time).seconds
        estimated_finish_time = int(elapsed_time / n * (files_count - n))
        log('FILES: ({}/{}) Elapsed time {}s. Estimated finish time {}s'.format(n, files_count, elapsed_time, estimated_finish_time))
        n += 1
    if previous_folder:
        #remove unzipped folder and files once complete
        shutil.rmtree(previous_folder)


#count the number of words in the text/plain part of the email
def get_word_count(message):
    text = ''
    if isinstance(message, list):
        for i in message:
            if i.get_content_type() == 'text/plain':
                text += i.get_payload()
    else:
        text = message
    return len(text.split()) + 1


#read path to .eml file
def read_email_message(email_file):
    words_count = 0
    # call: get_email_message -> get email_info -> get_word_count
    for email_info in get_email_message(email_file):
        words_count += email_info['words_count']
    return words_count


def parse_xml_email(email_folder, file):
    email_info = {
        'cc': [],
        'to': [],
        'files': [],
        'words_count': 0,
        'files_count': 0,
    }

    with open(file, 'r') as f, open(CSV_PATH, 'a+') as csv:
        data = xmltodict.parse(f.read())
        for doc in data.get('Root', {}).get('Batch', {}).get('Documents', {}).get('Document', []):
            current_email_to = []
            current_email_cc = []
            if doc['@DocType'] != 'Message':
                continue
            for tag in doc['Tags']['Tag']:
                tag_name = tag['@TagName']
                if tag_name == '#To':
                    current_email_to = parse_emails(tag['@TagValue'])
                    email_info['to'].extend(current_email_to)
                elif tag_name == '#CC':
                    current_email_cc = parse_emails(tag['@TagValue'])
                    email_info['cc'].extend(current_email_cc)
            for email_file in doc['Files']['File']:
                if email_file['@FileType'] == 'Native':
                    email_file_path = '{}/{}'.format(email_file['ExternalFile']['@FilePath'], email_file['ExternalFile']['@FileName'])
                    
                    #function read_email_message: calls read_email_message for word count
                    words_count = read_email_message(os.path.join(email_folder, email_file_path))
                    email_info['words_count'] += words_count
                    email_info['files_count'] += 1
                    # write to csv for audit trail
                    csv.write('{},{},{},{}\n'.format(email_file_path, words_count, ';'.join(current_email_to), ';'.join(current_email_cc)))
        return email_info

                                       
#read through all the emails
def read_all_email_info(email_folders):
    top_emails = defaultdict(float)
    words_count = 0
    files_count = 0

    with open(CSV_PATH, 'w+') as csv:
        csv.write('{},{},{},{}\n'.format('File path', 'Words count', 'Email To', 'Email CC'))

    if os.path.isfile(STATE_FILE):
        log('Load from {}'.format(STATE_FILE))
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        top_emails.update(state['top_emails'])
        words_count = state['words_count']
        files_count = state['files_count']
    else:
        state = {}

    # main loop for unzipped email foders
    for email_folder in email_folders:
        top_sorted_emails = []
        #iterate through xml extensions -> call function to find all xml files in folder
        files = find_files_with_extenstion(email_folder, EMAIL_XML_EXTENSION)
        for file in files:
            log('Read {}'.format(file))
            try:
                #retrieves structure and .eml files to open for word count
                email_info = parse_xml_email(email_folder, file)
                if not email_info:
                    continue
            except Exception:
                log(traceback.format_exc())
                log('Skip {}/{}'.format(email_folder, file))
                continue
            
            #count to emails plus 1 and 50% to cc
            for email_to in email_info['to']:
                top_emails[email_to] += 1
            for email_to in email_info['cc']:
                top_emails[email_to] += 0.5
            words_count += email_info['words_count']
            files_count += email_info['files_count']

        # save state to continue if fails
        for email_address in sorted(top_emails, key=top_emails.__getitem__, reverse=True)[:TOP_EMAILS_COUNT]:
            top_sorted_emails.append((email_address, top_emails[email_address], ))
        state = {
            'words_count': words_count or 1,
            'average': words_count / (files_count or 1),
            'files_count': files_count,
            'top_emails': top_emails,
            'top_sorted_emails': top_sorted_emails,
            'email_folders': state.get('email_folders', []) + [os.path.basename(email_folder)]
        }
        log('Dump to {}'.format(STATE_FILE))
        with open(STATE_FILE, 'w+') as f:
            json.dump(state, f)
    return state


def get_zip_files(path):
    # here can be splited for multiprocessing
    log('Look for zipped emails in "{}"'.format(path))
    return [os.path.join(path, file) for file in os.listdir(path) if file.endswith(ZIP_EXTENSION)]

# ------------------------ run functions ----------------------
                                       
def app(zip_files):
    #remove previous audit csv on start
    if os.path.isfile(CSV_PATH):
        log('Remove csv file "{}" and creating a new one'.format(CSV_PATH))
        os.remove(CSV_PATH)
    
    #unzip emails
    log('App got {} files'.format(len(zip_files)))
    email_folders = unzip_email_files(zip_files)
    
    #read email contents of extracted files
    result = read_all_email_info(email_folders)
    log('Total words count {}'.format(result['words_count']))
    log('Average words count {}'.format(result['average']))
    log('Files count {}'.format(result['files_count']))
    log('\nTop {} emails:'.format(TOP_EMAILS_COUNT))
    log('{:40} {}'.format('Email', 'Score'))
    for top in result['top_sorted_emails']:
        log('{:40} {}'.format(top[0], top[1]))
    os.remove(STATE_FILE)

# ------------------------ execute ----------------------

def main():
    try:
        #search for zip files in directory
        zip_files = get_zip_files(EMAIL_PATH)
        app(zip_files)
    except:
        log(traceback.format_exc())


main()
