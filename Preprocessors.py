import pandas as pd


class BasePreprocessor:
    @staticmethod
    def process(data: pd.DataFrame, keyword: str, serialized_daterange) -> pd.DataFrame:
        pass


class YandexPreprocessor(BasePreprocessor):
    @staticmethod
    def process(data: pd.DataFrame, keyword: str, serialized_daterange) -> pd.DataFrame:
        result = data.rename(columns={'Period': 'date', 'Number of queries': 'absolute_value', 'Percentage of total queries, %': 'relative_value'})
        result['date'] = pd.to_datetime(result['date'], format='%B %Y')
        result['absolute_value'] = result['absolute_value'].apply(lambda x: int("".join(str(x).split(" "))))
        result['relative_value'] = result['relative_value'].apply(lambda x: float(str(x).replace(',', '.')))

        result = result[(result['date'] > serialized_daterange.split(" ")[0]) & (result['date'] < serialized_daterange.split(" ")[1])]
        return result


class GooglePreprocessor(BasePreprocessor):
    @staticmethod
    def process(data: pd.DataFrame, keyword: str, serialized_daterange) -> pd.DataFrame:
        tmp = data.reset_index()
        tmp = tmp.rename(columns={keyword: 'relative_value'})

        tmp['month'] = tmp['date'].dt.month
        tmp['year'] = tmp['date'].dt.year

        result = tmp.groupby(['year', 'month']).mean().reset_index()
        result['date'] = pd.to_datetime(result[['year', 'month']].assign(day=1))
        result = result.drop(['year', 'month'], axis=1)

        result = result[(result['date'] > serialized_daterange.split(" ")[0]) & (result['date'] < serialized_daterange.split(" ")[1])]
        return result