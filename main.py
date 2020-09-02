import report_analytics
from precipy.main import render_file

render_file('report_config.json', [report_analytics], storages=[])

