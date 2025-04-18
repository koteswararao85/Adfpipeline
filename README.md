# Adfpipeline

"value": {
    "value": "@json(concat('[', join(array(foreach(variables('processedQueries'), item => replace(item, concat('{', item().key, '}'), item().value))), ','), ']'))",
    "type": "Expression"
}


{
    "name": "QueryParameterReplacementPipeline",
    "properties": {
        "activities": [
            {
                "name": "InitializeProcessedQueries",
                "type": "SetVariable",
                "dependsOn": [],
                "userProperties": [],
                "typeProperties": {
                    "variableName": "processedQueries",
                    "value": {
                        "value": "@pipeline().parameters.queries",
                        "type": "Expression"
                    }
                }
            },
            {
                "name": "ProcessParameters",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "InitializeProcessedQueries",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "items": {
                        "value": "@pipeline().parameters.inputParams",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "ReplaceParameter",
                            "type": "SetVariable",
                            "dependsOn": [],
                            "userProperties": [],
                            "typeProperties": {
                                "variableName": "processedQueries",
                                "value": {
                                    "value": "@json(concat('[', join(array(foreach(variables('processedQueries'), item => replace(item, concat('{', item().key, '}'), item().value))), ','), ']'))",
                                    "type": "Expression"
                                }
                            }
                        }
                    ]
                }
            }
        ],
        "parameters": {
            "inputParams": {
                "type": "object",
                "defaultValue": {
                    "start_date": "2024-05-01",
                    "end_date": "start_date",
                    "lob": "IOC"
                }
            },
            "queries": {
                "type": "array",
                "defaultValue": [
                    "SELECT * FROM test where mbr_sub_pln_adj_end_dt >= DATE {start_date} - INTERVAL 13",
                    "SELECT * FROM test where lob_code = {lob}"
                ]
            }
        },
        "variables": {
            "processedQueries": {
                "type": "Array"
            }
        },
        "annotations": []
    }
} 
