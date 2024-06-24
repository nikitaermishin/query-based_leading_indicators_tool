import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc 
from datetime import datetime
import os
import time

class YandexWordstatScraper():
  class Locators():
    LOGIN_BUTTON = (By.XPATH, '/html/body/div[2]/table/tbody/tr/td[6]/table/tbody/tr[1]/td[2]/a/span')
    LOGIN_BY_NAME_BUTTON = (By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div[2]/div[3]/div/div/div/div/form/div/div[2]/div[1]/div[1]/button')
    LOGIN_INPUT = (By.XPATH, '//*[@id="passp-field-login"]')
    LOGIN_SUBMIT = (By.XPATH, '//*[@id="passp:sign-in"]')
    PASSWORD_INPUT = (By.XPATH, '//*[@id="passp-field-passwd"]')

    SEARCH_FIELD = (By.XPATH, '/html/body/div[1]/div[2]/div/div[2]/div/div[2]/span/input')
    SEARCH_BUTTON = (By.XPATH, '/html/body/div[1]/div[2]/div/div[2]/div/div[2]/button')

    DATEPICKER_BUTTON = (By.XPATH, '/html/body/div[1]/div[2]/div/div[2]/div/div[3]/div[1]/div/div[1]/div[2]/div/button')
    DATEPICKER_YEAR_BUTTON = (By.XPATH, '/html/body/div[4]/div[1]/div/div[2]/div[1]/div/span/button')

    @staticmethod
    def GetDatepickerYearButton(year_num):
      return (By.XPATH, f'/html/body/div[4]/div[1]/div/div[2]/div[1]/div/span/div/div/div[{year_num - 2018 + 1}]/span[2]')

    @staticmethod
    def GetDatepickerMonthButton(month_num):
      return (By.XPATH, f'/html/body/div[4]/div[1]/div/div[2]/div[2]/div[{month_num // 3 + 1}]/div[{month_num % 3 + 1}]')

    DOWNLOAD_BUTTON = (By.XPATH, '/html/body/div[1]/div[2]/div/div[2]/div/div[3]/div[2]/div[1]/div/div/div[1]/div[2]/button')
    DOWNLOAD_CSV_BUTTON = (By.XPATH, '/html/body/div[5]/div[1]/div[1]/span/div/a/button')

  def __init__(self):
    options = uc.ChromeOptions()
    options.add_argument("--disable-popup-blocking")

    self.driver = uc.Chrome(options=options, version_main=126)
    params = {
        "behavior": "allow",
        "downloadPath": ".tmp"
    }
    self.driver.execute_cdp_cmd("Page.setDownloadBehavior", params)
   
  def __DoAuth(self, login, password):
    self.driver.get("https://passport.yandex.com/auth?retpath=https%3A%2F%2Fwordstat-2.yandex.com")

    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located(self.Locators.LOGIN_BY_NAME_BUTTON)).click()
    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located(self.Locators.LOGIN_INPUT)).send_keys(login)
    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located(self.Locators.LOGIN_SUBMIT)).click()
    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located(self.Locators.PASSWORD_INPUT)).send_keys(password)
    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located(self.Locators.LOGIN_SUBMIT)).click()

  def __DoQuery(self, keyword):
    for i in range(50):
      WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(self.Locators.SEARCH_FIELD)).send_keys('\b')

    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(self.Locators.SEARCH_FIELD)).send_keys(keyword)
    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(self.Locators.SEARCH_BUTTON)).click()

  def __SetTimeframe(self, timeframe):
    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.DATEPICKER_BUTTON)).click()

    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.DATEPICKER_YEAR_BUTTON)).click()
    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.GetDatepickerYearButton(timeframe[0].year))).click()
    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.GetDatepickerMonthButton(timeframe[0].month - 1))).click()

    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.DATEPICKER_YEAR_BUTTON)).click()
    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.GetDatepickerYearButton(timeframe[1].year))).click()
    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.GetDatepickerMonthButton(timeframe[1].month - 1))).click()

  def __DownloadResult(self):
    if os.path.exists("./.tmp/wordstat_dynamic.csv"):
      os.remove("./.tmp/wordstat_dynamic.csv")

    time.sleep(5)
    WebDriverWait(self.driver, 30).until(EC.visibility_of_element_located(self.Locators.DOWNLOAD_BUTTON)).click()
    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.Locators.DOWNLOAD_CSV_BUTTON)).click()
    time.sleep(5)
    assert(os.path.exists("./.tmp/wordstat_dynamic.csv"))

  def DoAuth(self, login, password):
    self.__DoAuth(login, password)

  def FetchInterestOverTime(self, keyword, timeframe):
    self.__DoQuery(keyword)
    self.__SetTimeframe(timeframe)
    self.__DownloadResult()

    return pd.read_csv("./.tmp/wordstat_dynamic.csv", delimiter=";").iloc[:, :-1]