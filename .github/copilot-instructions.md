# Instructions for GitHub Copilot agent
## Project description
This project is a data engineering pipeline for flight performance analytics. It involves extracting, transforming, and loading (ETL) flight data from various sources, performing data quality checks, and generating reports and visualizations for stakeholders. The pipeline is built using Python and utilizes libraries such as polars, great_expectations, postgres, clickhouse database, dagster for orchestration, and dbt for data transformation.

## Coding guidelines
- Follow PEP 8 style guide for Python code.
- Use descriptive variable and function names.
- Write simple, modular and reusable code.
- Include docstrings for all functions and classes.
- Use type hints for function parameters and return types.
- Write unit tests for all functions and classes.
- Use logging instead of print statements for debugging and monitoring.
- Handle exceptions gracefully and provide meaningful error messages.
- Use version control (git) for all code changes and commit messages should be clear and concise.
- Avoid hardcoding values; use configuration files or environment variables instead.
- Ensure that the code is compatible with Python 3.12 or higher.
- Follow best practices for data engineering, such as using efficient data structures and algorithms, optimizing database queries, and minimizing memory usage.
- Simplify queries, DDL and DML statements, and avoid unnecessary complexity in the codebase.
- Follow design patterns and principles, such as SOLID, DRY, and KISS, to ensure maintainability and scalability of the codebase.
- Ensure that the code is well-documented and easy to understand for other developers who may work on the project in the future.
- Always wait for user input before generating code, and ask clarifying questions if the requirements are not clear.
- Always assess the impact of code changes on the overall project and ensure that they align with the project goals and objectives.
- Assess trade-offs between different approaches and choose the one that best fits the project requirements and constraints.
- Plan and design the code structure before writing code, and ensure that it is modular and scalable.

# Code Review guidelines
- Look for adherence to coding guidelines and best practices.
- Check cohesion and SRP (Single Responsibility Principle) of functions and classes.
- Check for encapsulation and abstraction.
- Check for code readability, maintainability, and testability.
- Check for portability and compatibility with different environments.
- Check for security vulnerabilities and potential risks.
- Check for performance and efficiency of the code.
- Check for defensibility and error handling.
- Check for reusability and modularity of the code.
- Check for documentation and comments in the code.
- Provide constructive feedback and suggestions for improvement.
- Ensure that the code changes align with the project goals and objectives.
- Ensure code is robust and can handle edge cases and unexpected inputs gracefully.
- Ensure that the code is well-tested and that all tests pass successfully.