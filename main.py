import io

from shiny import reactive, render, req
from shiny.express import input, ui
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import calendar

from GoogleTrendsFetcher import GoogleTrendsFetcher
from YandexWordstat2Scraper import YandexWordstatScraper
from dataclasses import dataclass
import datetime
import Indicators
import Preprocessors
from typing import Tuple


@dataclass
class FetchingResult:
    keywords: str
    serialized_daterange: str
    data: any

    def is_actual(self, keywords, serialized_daterange):
        return self.keywords == keywords and self.serialized_daterange == serialized_daterange


ui.page_opts(title="Application for Constructing Leading Indicators Based on Search Queries")

google_client = GoogleTrendsFetcher()
yandex_client = YandexWordstatScraper()
auth_success = reactive.value(False)
google_fetch_result = reactive.value(FetchingResult("", "", None))
yandex_fetch_result = reactive.value(FetchingResult("", "", None))

indicator_manager = Indicators.IndicatorsManager((
    Indicators.GoogleRelativeIndicator,
    Indicators.YandexRelativeIndicator,
    Indicators.YandexAbsoluteIndicator,
    Indicators.RelativeNormalizedSumIndicator)
)


@reactive.calc
@reactive.event(input.indicator_select)
def get_selected_indicator() -> Indicators.BaseIndicator:
    print(input.indicator_select())
    return indicator_manager.get_indicator_by_name(input.indicator_select())


@reactive.calc
@reactive.event(google_fetch_result)
def calculate_preprocessed_google_data():
    return Preprocessors.GooglePreprocessor.process(google_fetch_result.get().data, google_fetch_result.get().keywords, google_fetch_result.get().serialized_daterange)


@reactive.calc
@reactive.event(yandex_fetch_result)
def calculate_preprocessed_yandex_data():
    return Preprocessors.YandexPreprocessor.process(yandex_fetch_result.get().data, yandex_fetch_result.get().keywords, yandex_fetch_result.get().serialized_daterange)


@reactive.calc
@reactive.event(get_selected_indicator, calculate_preprocessed_google_data, calculate_preprocessed_yandex_data)
def calculate_indicator_data():
    return get_selected_indicator().aggregate(calculate_preprocessed_google_data(), calculate_preprocessed_yandex_data())


def serialize_daterange(daterange: Tuple[datetime.datetime, datetime.datetime]) -> str:
    d = f'{daterange[0].strftime("%Y-%m")}-01 {daterange[1].strftime("%Y-%m")}-{calendar.monthrange(daterange[1].year, daterange[1].month)[1]}'
    print(d)
    return d


async def try_fetch_google_data(keywords: str, daterange_str: str):
    try:
        google_client.BuildPayload([keywords], timeframe=daterange_str)
        fetched = google_client.FetchInterestOverTime()

        google_fetch_result.set(FetchingResult(keywords, daterange_str, fetched))

        msg = f"Successfully fetched Google Trends data"
        ui.notification_show(
            msg,
            type="message",
            duration=2,
        )
        print(msg)
    except Exception as err:
        msg = f"Failed to fetch Google Trends data, try again later. Error {err}"
        ui.notification_show(
            msg,
            type="error",
            duration=5,
        )
        print(msg)


async def try_fetch_yandex_data(keywords: str, daterange_str: str, daterange):
    try:
        fetched = yandex_client.FetchInterestOverTime(keywords, daterange)

        yandex_fetch_result.set(FetchingResult(keywords, daterange_str, fetched))

        msg = f"Successfully fetched Yandex Wordstat data"
        ui.notification_show(
            msg,
            type="message",
            duration=2,
        )
        print(msg)

    except Exception as err:
        msg = f"Failed to fetch Yandex Wordstat data, try again later. Error {err}"
        ui.notification_show(
            msg,
            type="error",
            duration=5,
        )
        print(msg)


@reactive.effect
@reactive.event(input.action_button)
async def fetch_data():
    if not auth_success.get():
        msg = f"You have to authorize with Yandex Passport first"
        ui.notification_show(
            msg,
            type="error",
            duration=2,
        )
        print(msg)
        return

    keywords = req(input.text())
    daterange_str = serialize_daterange(input.daterange())

    trying_to_fetch_new = False

    if not google_fetch_result.get().is_actual(keywords, daterange_str):
        trying_to_fetch_new = True
        await try_fetch_google_data(keywords, daterange_str)

    if not yandex_fetch_result.get().is_actual(keywords, daterange_str):
        trying_to_fetch_new = True
        await try_fetch_yandex_data(keywords, daterange_str, input.daterange())

    if not trying_to_fetch_new:
        msg = f"Already fetched actual data"
        ui.notification_show(
            msg,
            type="message",
            duration=2,
        )
        print(msg)


@reactive.effect
@reactive.event(input.yandex_auth_btn)
def yandex_auth():
    global auth_success
    login = req(input.yandex_login_text())
    password = req(input.yandex_password_text())

    try:
        yandex_client.DoAuth(login, password)
        auth_success.set(True)
    except Exception as err:
        msg = f"Yandex Passport authorization error: {err}"
        ui.notification_show(
            msg,
            type="error",
            duration=5,
        )
        print(msg)


with ui.sidebar(width=350):
    with ui.card():
        ui.input_text("text", "Search keywords", placeholder="Enter search keyword...")
        ui.input_date_range("daterange", "Search aggregation range", start="2020-05-03", min="2018-01-01", end="2022-06-12", max="2024-03-31")
        ui.input_action_button("action_button", "Request search data")


    @render.express
    def _render_yandex_auth_card():
        with ui.card():
            header_class_list = 'bg-success-subtle' if auth_success.get() else ''
            ui.card_header("Yandex Passport Auth", class_=header_class_list)

            if auth_success.get():
                ui.p("Authorized successfully")
            else:
                ui.input_text("yandex_login_text", "Yandex Passport login", placeholder="Enter login...")
                ui.input_password("yandex_password_text", "Yandex Passport password", placeholder="Enter password...")
                ui.input_action_button("yandex_auth_btn", "Authorize")


with ui.layout_columns():
    @render.express
    def _render_google_data_card():
        with ui.card():
            ui.card_header("Google search query statistics")

            if google_fetch_result.get().data is None:
                ui.p("Nothing to render yet. Make request with search keywords.")
                return

            @render.plot(alt=f"Relative popularity via Google Trends")
            def _render_google_plot():
                ax = sns.lineplot(data=calculate_preprocessed_google_data(), x='date', y='relative_value')
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
                return ax

            @render.data_frame
            def _render_google_stats():
                stats = calculate_preprocessed_google_data().describe().transpose().reset_index()
                return render.DataGrid(stats.loc[stats['index'] != 'date'])

            with ui.layout_columns():
                @render.download(label='Download raw .csv', filename='raw_google_data.csv')
                def _download_raw_google_csv():
                    with io.BytesIO() as buf:
                        google_fetch_result.get().data.to_csv(buf)
                        yield buf.getvalue()

                @render.download(label='Download preprocessed .csv', filename='preprocessed_google_data.csv')
                def _download_preprocessed_google_csv():
                    with io.BytesIO() as buf:
                        calculate_preprocessed_google_data().to_csv(buf)
                        yield buf.getvalue()


    @render.express
    def _render_yandex_data_card():
        with ui.card():
            ui.card_header("Yandex search query statistics")

            if yandex_fetch_result.get().data is None:
                ui.p("Nothing to render yet. Make request with search keywords.")
                return

            @render.plot(alt=f"Relative popularity via Yandex Wordstat")
            @reactive.event(input.yandex_plot_radio, calculate_preprocessed_yandex_data)
            def _render_yandex_plot():
                ax = sns.lineplot(data=calculate_preprocessed_yandex_data(), x='date', y=input.yandex_plot_radio())
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
                return ax

            ui.input_radio_buttons(
                "yandex_plot_radio",
                "Graph to display",
                {'relative_value': 'Relative value', 'absolute_value': 'Absolute value'},
            )

            @render.data_frame
            def _render_yandex_stats():
                stats = calculate_preprocessed_yandex_data().describe().transpose().reset_index()
                return render.DataGrid(stats.loc[stats['index'] != 'date'])

            with ui.layout_columns():
                @render.download(label='Download raw .csv', filename='raw_yandex_data.csv')
                def _download_raw_yandex_csv():
                    with io.BytesIO() as buf:
                        yandex_fetch_result.get().data.to_csv(buf)
                        yield buf.getvalue()

                @render.download(label='Download preprocessed .csv', filename='preprocessed_yandex_data.csv')
                def _download_preprocessed_yandex_csv():
                    with io.BytesIO() as buf:
                        calculate_preprocessed_yandex_data().to_csv(buf)
                        yield buf.getvalue()


@render.express
def _render_indicator_card():
    with ui.card():
        ui.card_header("Leading Indicator")

        if yandex_fetch_result.get().data is None or google_fetch_result.get().data is None:
            ui.p("Nothing to render yet. Make request with search keywords.")
            return

        with ui.layout_columns(col_widths=(3, 3, 6)):
            with ui.card():
                ui.input_select(
                    "indicator_select",
                    "Select an indicator below:",
                    indicator_manager.get_names(),
                )

                @render.download(label='Download .csv', filename='indicator_data.csv')
                def _download_indicator_csv():
                    with io.BytesIO() as buf:
                        calculate_indicator_data().to_csv(buf)
                        yield buf.getvalue()

            with ui.card():
                @render.text
                def _render_indicator_description():
                    return get_selected_indicator().description

            with ui.card():
                @render.plot(alt=f"Indicator graph")
                def _render_indicator_plot():
                    ax = sns.lineplot(data=calculate_indicator_data(), x='date', y='value')
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
                    return ax

                @render.data_frame
                def _render_indicator_stats():
                    stats = calculate_indicator_data().describe().transpose().reset_index()
                    return render.DataGrid(stats.loc[stats['index'] != 'date'])
