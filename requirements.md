# Text to SQL tool

## Overview
This tool is designed to take natural language and convert it to an SQL query which will efficiently and effectively extract the data requested. 

## Features
* Accepts natural language input, outputs an SQL query
* Handles role authorisation to the Snowflake database
* Generates SQL compatible for use in Snowflake database

## Guardrails
* Only generate SELECT statements
* Use delimeters in the prompt to distinguish between the user input and the rest of the prompt to protect against prompt injection