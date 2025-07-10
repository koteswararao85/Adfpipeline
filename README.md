
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
