{
    "name": "ProcessQueryParameters",
    "properties": {
        "activities": [
            {
                "name": "InitializeCurrentQuery",
                "type": "SetVariable",
                "dependsOn": [],
                "userProperties": [],
                "typeProperties": {
                    "variableName": "currentQuery",
                    "value": {
                        "value": "@pipeline().parameters.query",
                        "type": "Expression"
                    }
                }
            },
            {
                "name": "ProcessParameters",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "InitializeCurrentQuery",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "items": {
                        "value": "@keys(pipeline().parameters.parameters)",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "CreateTempQuery",
                            "type": "SetVariable",
                            "dependsOn": [],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "tempQuery",
                                "value": {
                                    "value": "@if(contains(variables('currentQuery'), concat('{', item(), '}')), replace(variables('currentQuery'), concat('{', item(), '}'), pipeline().parameters.parameters[item()]), variables('currentQuery'))",
                                    "type": "Expression"
                                }
                            }
                        },
                        {
                            "name": "UpdateCurrentQuery",
                            "type": "SetVariable",
                            "dependsOn": [
                                {
                                    "activity": "CreateTempQuery",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "currentQuery",
                                "value": {
                                    "value": "@variables('tempQuery')",
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
            "tempQuery": {
                "type": "String"
            },
            "processedQuery": {
                "type": "String"
            }
        },
        "annotations": []
    }
} 
