from pytrends.request import TrendReq

class GoogleTrendsFetcher():
  pytrends_fetcher = None

  def __init__(self, host_language='en-US', time_zone=180):
    requests_args = {
      'headers': {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
      },
      'verify': False
    }

    self.pytrends_fetcher = TrendReq(hl=host_language, tz=time_zone, requests_args=requests_args)

  def BuildPayload(self, keywords, timeframe="today 5-y", geo="", cat=0):
    self.pytrends_fetcher.build_payload(kw_list=keywords, timeframe=timeframe, geo=geo, cat=cat)

  def FetchInterestOverTime(self):
    return self.pytrends_fetcher.interest_over_time().drop(columns='isPartial')

  def FetchSuggestions(self, keyword):
    return self.pytrends_fetcher.suggestions(keyword)

  def FetchSearchedWith(self):
    return self.pytrends_fetcher.related_topics()

  def FetchSearchedAlso(self):
    return self.pytrends_fetcher.related_queries()