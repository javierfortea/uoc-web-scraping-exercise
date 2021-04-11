from time import sleep
from typing import Optional, Dict, List, Any

import pandas as pd
import bs4
import requests

from bs4 import BeautifulSoup


class AirportsScraper:
    MAIN_URL: str = 'https://airportdatabase.net/'
    SLEEP_BETWEEN_REQUESTS_SECS: float = 0.5

    COLUMNS: List[str] = ['airport_name', 'ident', 'type', 'latitude', 'longitude', 'elevation', 'continent',
                          'iso country', 'ISO Region:', 'ISO Region link', 'Municipality', 'Scheduled Service',
                          'GPS Code', 'IATA Code', 'wikipedia link', 'APP', 'ATIS', 'GND', 'TWR', 'Website', 'keywords']

    def __init__(self) -> None:
        self.requests = requests.Session()

    def create_dataset(self) -> None:
        from_element = 0
        airports_links = {}

        while True:
            url = self.MAIN_URL + 'airports/index/{}'.format(from_element)
            airport_links_in_page = self.__get_airports_links(url)

            if not airport_links_in_page:
                break

            airports_links = {**airports_links, **airport_links_in_page}

            from_element += 10
            sleep(self.SLEEP_BETWEEN_REQUESTS_SECS)

        airports_data = []
        for airport_name, airport_link in airports_links.items():
            airports_data.append(self.__get_airport_info(airport_name, airport_link))
            sleep(self.SLEEP_BETWEEN_REQUESTS_SECS)

        self.__write_csv(airports_data)

    def __write_csv(self, airports_data: List[Dict[str, Any]]) -> None:
        df = pd.DataFrame(airports_data)

        final_df = df[self.COLUMNS]
        final_df = final_df.rename(columns={"ISO Region:": "ISO Region"})
        final_df.to_csv('../data/final_dataset.csv', index=False)

    def __get_html_content(self, url: str) -> Optional[str]:
        response = self.requests.get(url)

        if response.status_code == 404:
            return None

        return response.text

    def __get_airports_links(self, airport_url: str) -> Optional[Dict[str, str]]:
        html_content = self.__get_html_content(airport_url)

        if not html_content:
            return None

        beautiful_soup = BeautifulSoup(html_content, 'html.parser')
        table = beautiful_soup.find('table', class_="table")

        if not table:
            return None

        airport_links = {}
        for row in table.findAll('tr'):
            cell_with_link = row.findAll('td')[3]
            link = cell_with_link.find('a')

            airport_name = link.contents[0]
            airport_links[airport_name] = self.MAIN_URL + link["href"]

        return airport_links

    def __get_airport_info(self, airport_name: str, airport_url: str) -> Dict[str, str]:
        html_content = self.__get_html_content(airport_url)
        beautiful_soup = BeautifulSoup(html_content, 'html.parser')

        table = beautiful_soup.find('table', class_="table")
        airport_data = {'airport_name': airport_name}

        for row in table.findAll('tr'):
            cells = row.findAll('td')

            if len(cells) == 2:
                key = cells[0].contents[0].text
                value = cells[1].contents[0]

                if type(value) == bs4.element.Tag:
                    if key in ["wikipedia link", "Website"]:
                        airport_data[key] = value["href"]
                    elif key == "ISO Region:":
                        airport_data[key] = value.text.strip()
                        airport_data["ISO Region link"] = value["href"]
                    else:
                        airport_data[key] = value.text.strip()
                else:
                    airport_data[key] = value.title().strip()

        return airport_data


if __name__ == '__main__':
    airports_scraper = AirportsScraper()
    airports_scraper.create_dataset()
