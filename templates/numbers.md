{% set text_data = load_json(plot_figures.files["text_data.json"].cache_filepath) %}

{% for key, value in text_data.items() %}
{{key}}: {{value}}
{% endfor %}