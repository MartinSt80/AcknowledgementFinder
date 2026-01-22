import sys
from typing import Tuple, List
import logging

import numpy as np
from pathlib import Path

import pandas as pd

from FilePathsUrls import FilePaths

from PublicationDownloadServices import QueryPmcAws, QueryPmcOaiPmh, QueryPmcFtp, QueryKopsPdf, QueryDoiHtml, \
    QueryShadowPdf, PublicationLog


def configure_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        handlers=[logging.FileHandler(FilePaths.download_log_fullpath),
                                  logging.StreamHandler(sys.stdout),
                                  ]
                        )
    return logger


def create_directories_and_files() -> None:

    # Create, reset the logfiles
    if not FilePaths.log_dir_fullpath.is_dir():
        FilePaths.log_dir_fullpath.mkdir()

    with FilePaths.publication_log_fullpath.open(mode='w') as log_file:
        log_file.write('citation_file_name,as_xml,as_pdf,fac_acknowledged,fac_suspected\n')

    with FilePaths.results_log_fullpath.open(mode='w') as log_file:
        log_file.write('# Download results for the publications\n')

    # Make sure the fulltext directories exist
    if not FilePaths.xml_dir_fullpath.is_dir():
        FilePaths.xml_dir_fullpath.mkdir()

    if not FilePaths.pdf_dir_fullpath.is_dir():
        FilePaths.pdf_dir_fullpath.mkdir()

    if not FilePaths.html_dir_fullpath.is_dir():
        FilePaths.html_dir_fullpath.mkdir()


def perform_downloads(query_pmc=True,
                      query_kops=True,
                      query_doi=False,
                      query_shadow=True
                      ) -> List[PublicationLog]:

    pub_list = []

    # Use only the pubmed entries in xml format
    for pub_entry in FilePaths.publication_dir.glob('*.txt'):

        # create the Publication object
        current_publication = PublicationLog()
        current_publication.initialize_from_citation(pub_entry)

        # # If the paper is available in PMC AWS cloud as xml, get the XML, not needed, is subset of pmh
        # if current_publication.pmcid:
        # 		current_publication.is_xml_fulltext = pmc_request.download_fulltext_from_pmc_aws(current_publication.pmcid)

        # else try to get it from the PMC AOI PMH API
        if current_publication.pmcid and query_pmc:
            current_publication.is_xml_fulltext = QueryPmcOaiPmh().download_fulltext_from_pmc_pmh(current_publication.pmcid)

        # # else try to get it from the PMC ftp server, not needed, can be used as backup
        # if current_publication.pmcid and query_pmc:
        # 	current_publication.is_xml_fulltext = ftp_request.download_pmc_fulltext_from_ftp(current_publication.pmcid)

        # If we didn't get the fulltext by PMC, try to download the pdf from KOPS
        if not current_publication.is_xml_fulltext and query_kops:
            current_publication.is_pdf_fulltext = kops_request.download_fulltext_pdf_from_kops(current_publication.pmid)

        # If nothing else worked try to get the html version of the file via the DOI, Warning scraper protection is hit really quick
        if (not current_publication.is_xml_fulltext and not current_publication.is_pdf_fulltext) and query_doi:
            current_publication.is_html_fulltext = doi_request.download_fulltext_html_via_doi(current_publication.doi)

        # Currently it is easier to try to retrieve full texts from shadow libraries
        if (not current_publication.is_xml_fulltext and not current_publication.is_pdf_fulltext) and query_shadow:
            current_publication.is_pdf_fulltext = shadow_request.download_fulltext_pdf_from_shadow(current_publication.pmid,
                                                                                                   current_publication.doi,
                                                                                                   )
        pub_list.append(current_publication)

    return pub_list


def perform_download_from_shadow(from_log=False,
                                 pubmed_list=None,
                                 doi_list=None,
                                 ) -> None:

    def _retrieve_pubmed_doi_from_log() -> Tuple[List[str], List[str]]:

        # Read in the logs to see which publications couldn't be retrieved via PMC or KOPS
        with open(FilePaths.publication_log_fullpath, 'r') as csv_file:
            pub_df = pd.read_csv(csv_file, header=0)

        # perform the NOR-operation on the PMC and KOPS entries, get the respective DOIs
        pmc_series = pub_df['pmc_xml']
        pdf_series = pub_df['kops_pdf']
        unretrieved_entries = ~pmc_series.combine(pdf_series, np.logical_or)
        unretrieved_dois = list(pub_df.loc[unretrieved_entries, ['DOI']])
        unretrieved_pubmed = list(pub_df.loc[unretrieved_entries, ['Pubmed Id']])

        return (unretrieved_pubmed, unretrieved_dois)

    if pubmed_list is None:
        pubmed_list = []

    if doi_list is None:
        doi_list = []

    if from_log:
        pubmed_list, doi_list = _retrieve_pubmed_doi_from_log()

    for pubmed_id, doi in zip(pubmed_list, doi_list):
        shadow_request.download_fulltext_pdf_from_shadow(pubmed_id[0], doi[0])
    return


def create_publication_log(publications:List[PublicationLog]) -> None:

    headers = ['citation_file_name', 'Pubmed Id', 'PMC Id', 'DOI', 'pmc_xml' , 'kops_pdf', 'doi_html', 'fac_acknowledged', 'fac_suspected']
    publication_data = [pub.list_publication_info() for pub in publications]
    publication_df = pd.DataFrame(publication_data, columns=headers)
    publication_df.to_csv(FilePaths.publication_log_fullpath, index=False)
    return


if __name__ == '__main__':


    # Setup logging
    root_logger = configure_logger()

    # Create directories and logs
    create_directories_and_files()

    # Setup download interfaces, if needed
    pmc_request = QueryPmcAws()
    ftp_request = QueryPmcFtp()
    kops_request = QueryKopsPdf()
    doi_request = QueryDoiHtml()
    shadow_request = QueryShadowPdf()


    publication_list = perform_downloads()

    # # not needed, if performed before
    # perform_download_from_shadow(from_log=True)

    # Write the info
    create_publication_log(publication_list)