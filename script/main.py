from time import sleep
from obj import CaseOfficer, CaseFactory


def main():
    url_file = r'N:\Python_Scripts\MassDownloader\script\urls\stub_urls.txt'
    with open(url_file) as file:
        urls = [url.rstrip() for url in file]
    co = CaseOfficer()
    cf = CaseFactory(default_directory=r'C:\Users\tristan.sebens\Downloads\MD_2.0_test')
    co.add_cases([cf.case(url) for url in urls])
    while co.cases_active():
        co.administrate()


if __name__ == '__main__':
    main()