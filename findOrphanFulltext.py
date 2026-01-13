
from pathlib import Path


class PublicationLog:

    def __init__(self, csv_string):
        # csv_string is:
        # file_name, as_xml, as_pdf, fac_acknowledged, fac_suspected\n'
        attributes = csv_string.rstrip('\n').split(',')
        self.file_name = Path(attributes[0])
        self.is_pmc_xml_fulltext = self._str_to_bool(attributes[1])
        self.is_kops_pdf_fulltext = self._str_to_bool(attributes[2])
        self.is_fac_acknowledged = self._str_to_bool(attributes[3])
        self.is_fac_suspected = self._str_to_bool(attributes[4])  # Currently not in use
        self.pub_fulltext_file_path = self._get_pub_file_name()
        self.ack_text = ''

    def _str_to_bool(self, boolean_string):
        if boolean_string == 'True':
            return True
        if boolean_string == 'False':
            return False

    # Retrieve the path of the fulltext file
    def _get_pub_file_name(self):
        if self.is_pmc_xml_fulltext:
            return pub_dir / (self.file_name.stem + '_full.xml')
        if self.is_kops_pdf_fulltext:
            return pub_dir / self.file_name.with_suffix('.pdf')


    def log_message(self):
        if self.pub_fulltext_file_path:
            return f'{self.pub_fulltext_file_path} has been scanned: Core facility {"has been " if self.is_fac_acknowledged else "is not "}acknowledged.'
        else:
            return f'{self.file_name} has no fulltext to parse.'

    def check_file(self):
        if self.pub_fulltext_file_path:
            if self.pub_fulltext_file_path.is_file():
                return f'OK: For {self.file_name} a fulltext is present: {self.pub_fulltext_file_path}'
            else:
                return f'Error: {self.file_name} is missing its fulltext file: {self.pub_fulltext_file_path}'
        else:
            return f'OK: For {self.file_name} no fulltext has been downloaded.'


pub_dir = Path('D:/PubTracker/Publications/2019')
log_subdirectory = 'logs'
publication_log_file_name = 'publications_log.csv'


with (pub_dir / log_subdirectory / publication_log_file_name).open(mode='rb') as log_file:
    log_file_content = log_file.read().decode('utf-8')

# split file content into lines, remove '\n' and header
publication_entry_list = log_file_content.rstrip('\n').split('\n')[1:]

publication_list = []

for publication_string in publication_entry_list:
    publication_list.append(PublicationLog(publication_string))

registered_file_list = []
for publication in publication_list:
    print(publication.check_file())
    if publication.pub_fulltext_file_path:
        registered_file_list.append(publication.pub_fulltext_file_path)
present_file_list = list(pub_dir.glob("*.pdf"))
present_file_list.extend(list(pub_dir.glob("*.xml")))
print('-----------------------------')
for present_file in present_file_list:
    if present_file not in registered_file_list:
        print(f'Error: {present_file.name} has no associated publication entry.')

