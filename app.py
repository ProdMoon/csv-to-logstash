import csv
import chardet
import os
import pandas as pd

# Modify this Config class before running.
class Config:
    # Path of your CSV file
    csv_file_path = '/path/to/your/companies.csv'

    # Path of the Logstash configuration file to be saved
    logstash_conf_path = '/path/to/your/logstash.conf'

    # Elasticsearch configuration
    hosts = 'http://localhost:9200'
    index = 'companies'
    user = ''
    password = ''
    ssl_certificate_authorities = '' # e.g. '/path/to/your/http_ca.crt

def main():
    conf = Config()

    if not os.path.exists(conf.csv_file_path):
        print('csv file not found')
        return

    # Encoding the CSV file to UTF-8
    conf.csv_file_path = create_encoded_csv(conf.csv_file_path)

    # Creating the Logstash configuration file
    create_logstash_conf(conf)

    print('... Done!')

def create_encoded_csv(file_path, new_encoding='utf-8'):
    print(f'Creating new CSV file encoded {new_encoding}...')

    encoding_list = ['utf-8', 'cp949', 'utf-16']
    for encoding in encoding_list:
        try:
            print(f'Trying to read CSV file as {encoding}...')
            df = pd.read_csv(file_path, encoding=encoding)
            print(f'Original encoding was {encoding}.')
            break
        except:
            pass
    
    if encoding == new_encoding:
        print(f'CSV file already encoded as {new_encoding}.')
        return file_path
    
    new_file_path = file_path.replace('.csv', '_utf8.csv')
    df.to_csv(new_file_path, encoding=new_encoding, index=False)

    print(f'CSV file saved with new encoding({new_encoding}) at: {new_file_path}')

    return new_file_path

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        read_bytes = min(10000, os.path.getsize(file_path))
        rawdata = file.read(read_bytes)
    return chardet.detect(rawdata)['encoding']

def create_logstash_conf(conf):
    print('Creating Logstash configuration file...')
    
    csv_file_encoding = detect_encoding(conf.csv_file_path)
    print(f'CSV file encoding: {csv_file_encoding}')

    # Reading the first row (column names) from the CSV file
    with open(conf.csv_file_path, mode='r', encoding=csv_file_encoding) as csvfile:
        reader = csv.reader(csvfile)
        column_names = next(reader)  # Gets the first row (column names)

    # Creating the Logstash configuration file
    with open(conf.logstash_conf_path, mode='w', encoding=csv_file_encoding) as conf_file:
        # Writing the input section
        conf_file.write('input {\n')
        conf_file.write('  file {\n')
        conf_file.write(f'    path => ["{conf.csv_file_path}"]\n')
        conf_file.write('    start_position => "beginning"\n')
        conf_file.write('    sincedb_path => "NUL"\n')
        conf_file.write('  }\n')
        conf_file.write('}\n\n')
        
        # Writing the filter section
        conf_file.write('filter {\n')
        conf_file.write('  csv {\n')
        conf_file.write(f'    columns => {column_names}\n')
        conf_file.write('    skip_header => "true"\n')
        conf_file.write('  }\n')
        conf_file.write('  mutate {\n')
        conf_file.write('    gsub => [\n')
        conf_file.write('      "사업장명", "주식회사", "",\n')
        conf_file.write('      "사업장명", "유한회사", "",\n')
        conf_file.write('      "사업장명", "유한책임회사", "",\n')
        conf_file.write('      "사업장명", "합자회사", "",\n')
        conf_file.write('      "사업장명", "합명회사", "",\n')
        conf_file.write('      "사업장명", "사단법인", "",\n')
        conf_file.write('      "사업장명", "재단법인", "",\n')
        conf_file.write('      "사업장명", "농업회사법인", "",\n')
        conf_file.write('      "사업장명", "어업회사법인", "",\n')
        conf_file.write('      "사업장명", "영농조합법인", "",\n')
        conf_file.write('      "사업장명", "영어조합법인", "",\n')
        conf_file.write('      "사업장명", "\([^)]*\)", "",\n')
        conf_file.write('      "사업장명", "（[^）]*）", "",\n')
        conf_file.write('      "사업장명", "\s+", " "\n')
        conf_file.write('    ]\n')
        conf_file.write('    strip => ["사업장명"]\n')
        conf_file.write('  }\n')
        conf_file.write('}\n\n')
        
        # Writing the output section
        conf_file.write('output {\n')
        conf_file.write('  elasticsearch {\n')
        conf_file.write(f'    hosts => "{conf.hosts}"\n')
        conf_file.write(f'    index => "{conf.index}"\n')
        if conf.user:
            conf_file.write(f'    user => "{conf.user}"\n')
        if conf.password:
            conf_file.write(f'    password => "{conf.password}"\n')
        if conf.ssl_certificate_authorities:
            conf_file.write(f'    ssl_certificate_authorities => "{conf.ssl_certificate_authorities}"\n')
        conf_file.write('  }\n')
        # conf_file.write('  stdout { codec => rubydebug }\n')  # For debugging; remove or comment out for production
        conf_file.write('}\n')

    print(f'Logstash configuration file created at: {conf.logstash_conf_path}\n')
    print('To run Logstash with this configuration, use the following command:\n')
    windows_path = conf.logstash_conf_path.replace('/', '\\')
    print(f'  WINDOWS: path\\to\\logstash -f "{windows_path}"\n')
    print(f'  LINUX: /path/to/logstash -f {conf.logstash_conf_path}\n')

if __name__ == '__main__':
    main()