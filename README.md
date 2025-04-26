{
    "name": "DynamicQueryPipeline",
    "properties": {
        "activities": [
            {
                "name": "ProcessQueries",
                "type": "ForEach",
                "dependsOn": [],
                "userProperties": [],
                "typeProperties": {
                    "items": {
                        "value": "@pipeline().parameters.queries",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "SetInitialQuery",
                            "type": "SetVariable",
                            "dependsOn": [],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "currentQuery",
                                "value": {
                                    "value": "@item()",
                                    "type": "Expression"
                                }
                            }
                        },
                        {
                            "name": "ProcessParameters",
                            "type": "ForEach",
                            "dependsOn": [
                                {
                                    "activity": "SetInitialQuery",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
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
                            "name": "SetFinalQuery",
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
                        },
                        {
                            "name": "LogQuery",
                            "type": "WebActivity",
                            "dependsOn": [
                                {
                                    "activity": "SetFinalQuery",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "userProperties": [],
                            "typeProperties": {
                                "url": "https://api.logging.com/log",
                                "method": "POST",
                                "body": {
                                    "query": "@variables('processedQuery')"
                                }
                            }
                        }
                    ]
                }
            }
        ],
        "parameters": {
            "parameters": {
                "type": "object",
                "defaultValue": {
                    "start_date": "2024-05-01",
                    "end_date": "2024-05-15",
                    "lob": "IOC"
                }
            },
            "queries": {
                "type": "array",
                "defaultValue": [
                    "SELECT * FROM test where mbr_sub_pln_adj_end_dt >= DATE {start_date} - INTERVAL 13",
                    "SELECT * FROM test where lob_code = {lob}",
                    "SELECT * FROM test where date_field = {end_date}",
                    "SELECT * FROM test where no_parameters = 'static_value'"
                ]
            }
        },
        "variables": {
            "processedQuery": {
                "type": "String"
            },
            "currentQuery": {
                "type": "String"
            },
            "replacedQuery": {
                "type": "String"
            }
        },
        "annotations": []
    }
} 
