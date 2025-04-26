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
                                "variableName": "tempQuery",
                                "value": {
                                    "value": "@item()",
                                    "type": "Expression"
                                }
                            }
                        },
                        {
                            "name": "CheckAndReplaceMore",
                            "type": "Until",
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
                                "expression": {
                                    "value": "@not(contains(variables('tempQuery'), '{'))",
                                    "type": "Expression"
                                },
                                "activities": [
                                    {
                                        "name": "GetParamName",
                                        "type": "SetVariable",
                                        "dependsOn": [],
                                        "userProperties": [],
                                        "typeProperties": {
                                            "variableName": "paramName",
                                            "value": {
                                                "value": "@first(split(split(variables('tempQuery'), '{')[1], '}')[0])",
                                                "type": "Expression"
                                            }
                                        }
                                    },
                                    {
                                        "name": "GetParamValue",
                                        "type": "SetVariable",
                                        "dependsOn": [
                                            {
                                                "activity": "GetParamName",
                                                "dependencyConditions": [
                                                    "Succeeded"
                                                ]
                                            }
                                        ],
                                        "userProperties": [],
                                        "typeProperties": {
                                            "variableName": "paramValue",
                                            "value": {
                                                "value": "@pipeline().parameters.parameters[variables('paramName')]",
                                                "type": "Expression"
                                            }
                                        }
                                    },
                                    {
                                        "name": "ReplaceNextParameter",
                                        "type": "SetVariable",
                                        "dependsOn": [
                                            {
                                                "activity": "GetParamValue",
                                                "dependencyConditions": [
                                                    "Succeeded"
                                                ]
                                            }
                                        ],
                                        "userProperties": [],
                                        "typeProperties": {
                                            "variableName": "tempQuery",
                                            "value": {
                                                "value": "@replace(variables('tempQuery'), concat('{', variables('paramName'), '}'), variables('paramValue'))",
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
                                    "activity": "CheckAndReplaceMore",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "processedQuery",
                                "value": {
                                    "value": "@variables('tempQuery')",
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
            "tempQuery": {
                "type": "String"
            },
            "paramName": {
                "type": "String"
            },
            "paramValue": {
                "type": "String"
            }
        },
        "annotations": []
    }
} 
