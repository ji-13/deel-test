{% macro country_name(column_name) %}

  CASE WHEN {{column_name}} = 'US' THEN 'USA'
  WHEN {{column_name}} = 'MX' THEN 'Mexico'
  WHEN {{column_name}} = 'UK' THEN 'United Kingdom'
  WHEN {{column_name}} = 'FR' THEN 'France'
  WHEN {{column_name}} = 'CA' THEN 'Canada'
  WHEN {{column_name}} = 'AE' THEN 'United Arab Emirates'
    ELSE {{column_name}} END
    
{% endmacro %}