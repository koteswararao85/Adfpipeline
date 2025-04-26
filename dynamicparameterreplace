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
                            "name": "ReplaceParameters",
                            "type": "SetVariable",
                            "dependsOn": [],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "processedQuery",
                                "value": {
                                    "value": "@if(contains(item(), '{'), replace(item(), '{' + first(split(split(item(), '{')[1], '}')[0]) + '}', pipeline().parameters.parameters[first(split(split(item(), '{')[1], '}')[0])]), item())",
                                    "type": "Expression"
                                }
                            }
                        },
                        {
                            "name": "CheckAndReplaceMore",
                            "type": "Until",
                            "dependsOn": [
                                {
                                    "activity": "ReplaceParameters",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "userProperties": [],
                            "typeProperties": {
                                "expression": {
                                    "value": "@not(contains(variables('processedQuery'), '{'))",
                                    "type": "Expression"
                                },
                                "activities": [
                                    {
                                        "name": "ReplaceNextParameter",
                                        "type": "SetVariable",
                                        "dependsOn": [],
                                        "userProperties": [],
                                        "typeProperties": {
                                            "variableName": "processedQuery",
                                            "value": {
                                                "value": "@replace(variables('processedQuery'), '{' + first(split(split(variables('processedQuery'), '{')[1], '}')[0]) + '}', pipeline().parameters.parameters[first(split(split(variables('processedQuery'), '{')[1], '}')[0])])",
                                                "type": "Expression"
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "name": "LogQuery",
                            "type": "WebActivity",
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
            }
        },
        "annotations": []
    }
} 
