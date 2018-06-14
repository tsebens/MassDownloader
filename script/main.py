from time import sleep
from obj import CaseOfficer, CaseFactory


def main():
    url_file = r'C:\Users\tristan.sebens\Projects\MassDownloader\script\urls\short_stories.txt'
    with open(url_file) as file:
        urls = [url.rstrip() for url in file]
    co = CaseOfficer(max_active_agents=10)
    cf = CaseFactory(default_directory=r'C:\Users\tristan.sebens\Downloads\MD_2.0_test')
    co.add_cases([cf.case(url) for url in urls])
    while co.cases_active():
        co.administrate()


if __name__ == '__main__':
    main()