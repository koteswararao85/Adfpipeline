
 {% macro handle_mv_refresh_conflict(mv_name, max_wait_minutes=30) %}
    {% set check_running_sql %}
        SHOW MATERIALIZED VIEWS LIKE '{{ mv_name }}'
    {% endset %}
    
    {% set mv_info = run_query(check_running_sql) %}
    
    {% if mv_info and mv_info.rows %}
        {% set mv_status = mv_info.rows[0][4] if mv_info.rows[0]|length > 4 else 'UNKNOWN' %}
        
        {% if mv_status == 'REFRESHING' %}
            {{ log("Materialized view " ~ mv_name ~ " is currently refreshing. Waiting up to " ~ max_wait_minutes ~ " minutes...", info=true) }}
            
            {% set wait_seconds = max_wait_minutes * 60 %}
            {% set check_interval = 30 %}
            {% set attempts = wait_seconds // check_interval %}
            
            {% for i in range(attempts) %}
                {% set status_check %}
                    SHOW MATERIALIZED VIEWS LIKE '{{ mv_name }}'
                {% endset %}
                
                {% set current_status = run_query(status_check) %}
                {% if current_status and current_status.rows and current_status.rows[0]|length > 4 %}
                    {% set current_mv_status = current_status.rows[0][4] %}
                    {% if current_mv_status != 'REFRESHING' %}
                        {{ log("Materialized view " ~ mv_name ~ " is no longer refreshing. Proceeding.", info=true) }}
                        {% break %}
                    {% endif %}
                {% endif %}
                
                {% if i < attempts - 1 %}
                    {{ log("Still refreshing... waiting " ~ check_interval ~ " seconds (attempt " ~ (i + 1) ~ "/" ~ attempts ~ ")", info=true) }}
                    {% do run_query("SELECT SLEEP(" ~ check_interval ~ ")") %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}



{% materialization dbx_custom_materialization, adapter='databricks' %}

  {%- set identifier = model['alias'] -%}
  {%- set schema = config.get('schema', 'default') -%}
  {%- set database = config.get('database', target.database) -%}
  {%- set location_root = config.get('location_root', '') -%}
  {%- set partition_by = config.get('partition_by', []) -%}
  {%- set unique_key = config.get('unique_key', []) -%}
  {%- set columns = config.get('columns', []) -%}
  {%- set masked_columns = config.get('masked_columns', []) -%}
  {%- set row_filter = config.get('row_filter', []) -%}
  {%- set schedule = config.get('schedule', {}) -%}
  {%- set file_format = config.get('file_format', 'delta') -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='materializedview'
  ) -%}

  {%- set full_table_name = database + '.' + schema + '.' + identifier -%}

  -- Check if materialized view already exists and is refreshing
  {% if schedule %}
    {{ handle_mv_refresh_conflict(full_table_name) }}
  {% endif %}

  -- Build the materialized view creation SQL
  {%- set create_mv_sql %}
    CREATE MATERIALIZED VIEW {{ target_relation }} (
      {%- for column in columns %}
        {{ column }}{% if column in masked_columns %} MASK {{ masked_columns[column] }}{% endif %}{% if not loop.last %},{% endif %}
      {%- endfor %}
    )
    {%- if row_filter %}
    WITH ROW FILTER {{ row_filter[0] }} ON ({{ row_filter[1] }})
    {%- endif %}
    {%- if location_root %}
    LOCATION '{{ location_root }}/{{ identifier }}'
    {%- endif %}
    {%- if partition_by %}
    PARTITIONED BY ({{ partition_by | join(', ') }})
    {%- endif %}
    {%- if schedule %}
    REFRESH {{ schedule.cron }} {{ schedule.time_zone }}
    {%- endif %}
    AS {{ sql }}
  {%- endset %}

  -- Execute the materialized view creation
  {% do run_query(create_mv_sql) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
 
 
 
 Failure in model member_program_view (models/gold/views/prospectiveDW/progPerf/member_program_view.sql)
00:50:27    Database Error in model member_program_view (models/gold/views/prospectiveDW/progPerf/member_program_view.sql)
  A refresh is already running in the background. You can monitor the previous refresh by visiting the following URL:
{{ config(
    materialized='dbx_custom_materialization',
    file_format='delta',
    schema='gold',
    schedule = {
        'cron': var('refresh_prospective_interval'),
        'time_zone': 'America/New_York'
    },
    location_root = var('magnus_storage_location')+'/gold/views/prospectiveDW/progperf/',
    partition_by='SOURCE_SYSTEM',
    unique_key='(LINK_MEMBER_PROVIDER_PROVIDER_GROUP_ASSOCIATION_HK,LOAD_DTS,SOURCE_SYSTEM)',
    columns = ['LINK_MEMBER_PROVIDER_PROVIDER_GROUP_ASSOCIATION_HK','deployableWOTollgate','deployed','IsSuppressed','ClientId','ClientName','clientSubscriberId','correlationId','JobId','ProgramYear','ProviderGroupId','ProviderGroupName','ProviderName','ProviderState','RunId','SubClientSk','GlobalMemberId','Lob','LobName','ProgramType','ProgramTypeId','latestChannel','primaryChannel','firstChannel','firstDeployedTs','latestDeployedTs','CreatedBy','CreatedDate','UpdatedBy','UpdatedDate','ProviderId','MMSMemberUpdatedDate','deployableStatus','LOAD_DTS','SOURCE_SYSTEM'],
    masked_columns = ['GlobalMemberId'],
    row_filter = ['row_filter_function ON (LobName)']
) }}

{
    "name": "ProcessQueryParameters",
    "properties": {
        "activities": [
            {
                "name": "ProcessParameters",
                "type": "ForEach",
                "dependsOn": [],
                "userProperties": [],
                "typeProperties": {
                    "items": {
                        "value": "@pipeline().parameters.parameters",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "CreateReplacedQuery",
                            "type": "SetVariable",
                            "dependsOn": [],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "replacedQuery",
                                "value": {
                                    "value": "@if(contains(variables('currentQuery'), concat('{', item().key, '}')), replace(variables('currentQuery'), concat('{', item().key, '}'), item().value), variables('currentQuery'))",
                                    "type": "Expression"
                                }
                            }
                        },
                        {
                            "name": "UpdateCurrentQuery",
                            "type": "SetVariable",
                            "dependsOn": [
                                {
                                    "activity": "CreateReplacedQuery",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "currentQuery",
                                "value": {
                                    "value": "@variables('replacedQuery')",
                                    "type": "Expression"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "name": "SetOutput",
                "type": "SetVariable",
                "dependsOn": [
                    {
                        "activity": "ProcessParameters",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "variableName": "processedQuery",
                    "value": {
                        "value": "@variables('currentQuery')",
                        "type": "Expression"
                    }
                }
            }
        ],
        "parameters": {
            "query": {
                "type": "string"
            },
            "parameters": {
                "type": "object"
            }
        },
        "variables": {
            "currentQuery": {
                "type": "String"
            },
            "replacedQuery": {
                "type": "String"
            },
            "processedQuery": {
                "type": "String"
            }
        },
        "annotations": []
    }
} 
