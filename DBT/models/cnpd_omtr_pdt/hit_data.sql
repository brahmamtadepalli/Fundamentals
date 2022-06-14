{% set year = var('year', '' ) %}
{% set month = var('month', '' ) %}
{% set day = var('day', '' ) %}

{% if execute %}
  {% if year == '' %}
    {{ exceptions.warn("hit_data - `year` was not specified") }}
  {% endif %}
  {% if month == '' %}
    {{ exceptions.warn("hit_data - `month` was not specified") }}
  {% endif %}
  {% if day == '' %}
    {{ exceptions.warn("hit_data - `day` was not specified") }}
  {% endif %}
{% endif %}

{{
  config(
    materialized='external_parquet',
    stage_url='@s3_stages.unrestricted.S3_STAGE_MOVE_DATAENG_OMNITURE/homerealtor/processed-data-xact/hit_data/year=' ~year ~'/month=' ~month ~'/day=' ~day ~'/',
    database='legacy_raw',
    schema='cnpd_omtr_pdt',
    tags=["cnpd_omtr_pdt","trusted"],
    partitions=[{
      'name': 'year',
      'sql': "split_part(split_part(metadata$filename, 'year=', 2), '/', 1)"
    },
    {
      'name': 'month',
      'sql': "split_part(split_part(metadata$filename, 'month=', 2), '/', 1)"
    },
    {
      'name': 'day',
      'sql': "split_part(split_part(metadata$filename, 'day=', 2), '/', 1)"
    },
    {
      'name': 'hour',
      'sql': "split_part(split_part(metadata$filename, 'hour=', 2), '/', 1)"
    }],
    pre_hook='{{ delete_by_year_month_day_key(this.database, this.schema, this.identifier, "' ~year ~'", "' ~month~'", "' ~day ~'") }}'
  )
}}

