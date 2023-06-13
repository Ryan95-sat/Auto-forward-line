import parsel
import requests
from urllib.parse import urljoin


def download_handle(link):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    response = requests.get(link, headers=headers)
    if response.status_code == 200:
        selector = parsel.Selector(response.text)
        link_list = selector.css('.module-side .module-textlist.scroll-content a::attr("href")').getall()
        print(link_list)
        for url in link_list:
            response = requests.get(link + url, headers=headers)
            if response.status_code == 200:
                selector = parsel.Selector(response.text)
                href = selector.css('.video-info-footer.display a::attr("href")').get()
                # title = selector.css('.video-info-footer.display a::attr("title")').get()
                print(link + href)


def new_download(link, region):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    msg = f"From IMDb.\nUpcoming releases {region}.\n"
    link = link % region
    try:
        response = requests.get(link, headers=headers)
        if response.status_code == 200:
            selector = parsel.Selector(response.text)
            link_list = selector.css('a.ipc-metadata-list-summary-item__t')
            if link_list:
                link_list = link_list[:10]
                for item in link_list:
                    href = item.css('::attr("href")').get()
                    title = item.css('::text').get()
                    msg += f"{title} {urljoin(link, href)}\n"
        return msg + "Of course, You can switch countries, such as @Fl Movie US,TW,CN etc..."
    except Exception as e:
        return msg + "There is something went wrong..."


def crawl_msg(region="US"):
    link = "https://www.imdb.com/calendar/?ref_=rlm&region=%s&type=MOVIE"
    message = new_download(link, region)
    return message


if __name__ == '__main__':
    reg = "TW"  # CN
    print(crawl_msg())
