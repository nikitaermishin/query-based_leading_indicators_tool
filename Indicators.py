import pandas as pd


class BaseIndicator:
    name: str
    description: str

    @staticmethod
    def aggregate(google_df: pd.DataFrame, yandex_df: pd.DataFrame) -> pd.DataFrame:
        pass


class IndicatorsManager:
    def __init__(self, indicators: tuple[BaseIndicator]):
        self._indicators = indicators
        self._names_dict = {}

        for i, indicator in enumerate(self._indicators):
            self._names_dict[indicator.name] = i

    def get_names(self) -> tuple[str]:
        return tuple(self._names_dict.keys())

    def get_indicator_by_name(self, name: str) -> BaseIndicator:
        if name not in self._names_dict:
            raise RuntimeError(f'No indicator with name {name} in _names_dict {self._names_dict}')
        return self._indicators[self._names_dict[name]]


class GoogleRelativeIndicator(BaseIndicator):
    name = 'Google Relative Indicator'
    description = 'Google Relative Indicator - индикатор, соответствующий относительной популярности запроса по статистике Google Trends'

    @staticmethod
    def aggregate(google_df: pd.DataFrame, yandex_df: pd.DataFrame) -> pd.DataFrame:
        return google_df.rename(columns={'relative_value': 'value'})


class YandexRelativeIndicator(BaseIndicator):
    name = 'Yandex Relative Indicator'
    description = 'Yandex Relative Indicator - индикатор, соответствующий относительной популярности запроса по статистике Yandex Wordstat'

    @staticmethod
    def aggregate(google_df: pd.DataFrame, yandex_df: pd.DataFrame) -> pd.DataFrame:
        return yandex_df.rename(columns={'relative_value': 'value'}).drop(columns=['absolute_value'])


class YandexAbsoluteIndicator(BaseIndicator):
    name = 'Yandex Absolute Indicator'
    description = 'Yandex Absolute Indicator - индикатор, соответствующий абсолютной популярности запроса по статистике Yandex Wordstat'

    @staticmethod
    def aggregate(google_df: pd.DataFrame, yandex_df: pd.DataFrame) -> pd.DataFrame:
        return yandex_df.rename(columns={'absolute_value': 'value'}).drop(columns=['relative_value'])


class RelativeNormalizedSumIndicator(BaseIndicator):
    name = 'Relative Sum Indicator'
    description = 'Relative Sum Indicator - индикатор, соответствующий сумме долей относительных популярностей, деленных на максимум за период времени'

    @staticmethod
    def aggregate(google_df: pd.DataFrame, yandex_df: pd.DataFrame) -> pd.DataFrame:
        print(yandex_df['date'], google_df['date'])

        google_df['relative_value'] /= google_df['relative_value'].max()
        yandex_df['relative_value'] /= yandex_df['relative_value'].max()
        result = pd.concat([google_df, yandex_df.drop(columns=['absolute_value'])])
        result = result.groupby('date').sum().reset_index()
        return result.rename(columns={'relative_value': 'value'})
