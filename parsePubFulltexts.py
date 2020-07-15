
import shutil
from pathlib import Path

from lxml import etree as ET

from tika import parser

class PublicationLog:

    def __init__(self, csv_string):
        # csv_string is:
        # file_name, as_xml, as_pdf, bic_acknowledged, bic_suspected\n'
        attributes = csv_string.rstrip('\n').split(',')
        self.file_name = Path(attributes[0])
        self.is_pmc_xml_fulltext = self._str_to_bool(attributes[1])
        self.is_kops_pdf_fulltext = self._str_to_bool(attributes[2])
        self.is_bic_acknowledged = self._str_to_bool(attributes[3])
        self.is_bic_suspected = self._str_to_bool(attributes[4])
        self.pub_fulltext_file_path = self._get_pub_file_name()
        self.ack_text = ''

    def _str_to_bool(self, boolean_string):
        if boolean_string == 'True':
            return True
        if boolean_string == 'False':
            return False

    def _get_pub_file_name(self):
        if self.is_pmc_xml_fulltext:
            return pub_dir / (self.file_name.stem + '_full.xml')
        if self.is_kops_pdf_fulltext:
            return pub_dir / self.file_name.with_suffix('.pdf')

    def _parse_pdf_for_ack(self, search_terms):
        pdf_file_content = parser.from_file(str(self.pub_fulltext_file_path))

        try:
            start_index_ack_ = pdf_file_content['content'].lower().index('acknowl')
            self.ack_text = pdf_file_content['content'][start_index_ack_:start_index_ack_ + 800]
        except ValueError:
            paragraphs = pdf_file_content['content'].split('\n\n')
            ack_paragraphs = []
            for p in paragraphs:
                if any([term in p.lower() for term in ack_term_list]):
                    ack_paragraphs.append(p)
            self.ack_text = ''.join(ack_paragraphs)
        self._store_ack_text(pub_dir)
        self.is_bic_acknowledged = False
        for search_term in search_terms:
            if search_term in self.ack_text:
                self.is_bic_acknowledged = True
                break

    def _parse_xml_for_ack(self, search_terms):
        with open(self.pub_fulltext_file_path, 'r') as xml_file:
            xml_fulltext = xml_file.read()
        xml_root = ET.fromstring(xml_fulltext)
        self.is_bic_acknowledged = False
        ack_list = list(xml_root.iter('ack'))

        if ack_list:
            for acknowledgment in ack_list:
                for paragraph in acknowledgment.iter('p'):
                    self.ack_text += paragraph.text
        else:
            title_list = list(xml_root.iter('title'))
            for title in title_list:
                if 'acknowl' in title.text.lower():
                    section_element = next(title.iterancestors())

                    for paragraph in section_element.iter('p'):
                        self.ack_text += paragraph.text

        self._store_ack_text(pub_dir)
        for search_term in search_terms:
            if search_term in self.ack_text:
                self.is_bic_acknowledged = True
                break

    def _store_ack_text(self, directory):
        with (directory / (self.file_name.stem + '_ack.txt')).open(mode='wb') as ack_file:
            ack_file.write(self.ack_text.encode('utf-8'))


    def log_message(self):
        if self.pub_fulltext_file_path:
            return f'{self.pub_fulltext_file_path} has been scanned: BIC {"has been " if self.is_bic_acknowledged else "is not "}acknowledged.'
        else:
            return f'{self.file_name} has no fulltext to parse.'


    def search_ack_for_terms(self, term_list):
        if self.is_pmc_xml_fulltext:
            self._parse_xml_for_ack(term_list)
        if self.is_kops_pdf_fulltext:
            self._parse_pdf_for_ack(term_list)

    # Compound the necessary information into a csv-string to write to the log.
    def log_csv_string(self):
        # file_name, as_xml, as_pdf, bic_acknowledged, bic_suspected\n'
        data = [str(self.file_name), str(self.is_pmc_xml_fulltext), str(self.is_kops_pdf_fulltext), str(self.is_bic_acknowledged), 'None\n']
        return ','.join(data)

    def copy_pub_with_ack(self):
        pub_ack_path = pub_dir / ack_subdirectory
        if not pub_ack_path.is_dir():
            pub_ack_path.mkdir()
        self._store_ack_text(pub_ack_path)
        shutil.copy(pub_dir / self.file_name, pub_ack_path)
        shutil.copy(self.pub_fulltext_file_path, pub_ack_path)
        shutil.copy((pub_dir / self.file_name).with_suffix('.ris'), pub_ack_path)



# Directory to process
pub_dir = Path('D:/PubTracker/Publications/test')
publication_log_file_name = 'publications_log.csv'
results_log_file_name = 'results_log.txt'
ack_subdirectory = 'bic_acknowledged'
log_subdirectory = 'logs'
search_term_list = ['BIC', 'Bioimaging']
ack_term_list = ['we thank', 'like to thank', 'we acknowledge', 'assisted by', 'thankful', 'grateful']


# Read, create and, reset the log_files
with (pub_dir / log_subdirectory / publication_log_file_name).open(mode='rb') as log_file:
    log_file_content = log_file.read().decode('utf-8')
if (pub_dir / log_subdirectory / publication_log_file_name).with_suffix('.old').is_file():
    (pub_dir / log_subdirectory / publication_log_file_name).with_suffix('.old').unlink()
(pub_dir / log_subdirectory / publication_log_file_name).rename((pub_dir / log_subdirectory / publication_log_file_name).with_suffix('.old'))
with (pub_dir / log_subdirectory / publication_log_file_name).open(mode='w') as log_file:
    log_file.write('file_name,as_xml,as_pdf,bic_acknowledged,bic_suspected\n')

with (pub_dir / log_subdirectory / results_log_file_name).open(mode='w') as log_file:
    log_file.write(f'# Results of parsing the publications for {", ".join(search_term_list)}\n')

publication_list = log_file_content.rstrip('\n').split('\n')[1:]

for publication_string in publication_list:
    current_publication = PublicationLog(publication_string)
    current_publication.search_ack_for_terms(search_term_list)

    print(current_publication.log_message())

    with (pub_dir / log_subdirectory / publication_log_file_name).open(mode='ab') as log_file:
        log_file.write(current_publication.log_csv_string().encode('utf8'))

    with (pub_dir / log_subdirectory / results_log_file_name).open(mode='ab') as log_file:
        log_file.write((current_publication.log_message() + '\n').encode('utf-8'))

    if current_publication.is_bic_acknowledged:
        current_publication.copy_pub_with_ack()

(pub_dir / log_subdirectory / publication_log_file_name).with_suffix('.old').unlink()