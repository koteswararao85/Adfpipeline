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
