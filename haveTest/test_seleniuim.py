
from selenium import webdriver
from selenium.webdriver.chrome.service import Service



def test_selenium():
    # 手动指定chromedriver路径
    driver_path = r'D:\software\chromedriver-win64\chromedriver.exe'
    service = Service(executable_path=driver_path)

    # 创建 Chrome WebDriver 实例
    driver = webdriver.Chrome(service=service)

    driver.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSLA&apikey=ICTN9P9ES00EADUF&outputsize=full&datatype=json')
    print(driver.page_source)

    data = driver.page_source
    return data
    driver.quit()

if __name__ == "__main__":
    res = test_selenium()
    print(res)



























